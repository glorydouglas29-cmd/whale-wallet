// Whale Copytrade Tracker — Cloudflare Worker
//
// Serves the dashboard, manages a tracked-wallet list in KV, and runs on a
// schedule (every 5 minutes, set in wrangler.jsonc) to poll each tracked
// wallet's recent activity via Helius, detect swaps, log them for the
// dashboard feed, and push a Discord alert for each new one.
//
// Also includes a set of on-demand investigation tools that don't touch KV
// at all — whale scanning, cross-token buyer checks, deployer lookup, and
// multi-hop funding-lineage tracing — for finding wallets worth tracking in
// the first place.
//
// REQUIRED SETUP after first deploy (same pattern as ZEKE LEDGER):
// 1. Settings -> Bindings -> add a KV namespace, binding name: TRACKER_KV
// 2. Settings -> Variables and Secrets -> add HELIUS_API_KEY (secret)
// 3. Settings -> Variables and Secrets -> add DISCORD_WEBHOOK_URL (secret)
//    — optional. Alerts are just silently skipped if this isn't set; the
//    dashboard feed still works without it.
//
// This is a genuinely separate deployed project from ZEKE LEDGER, so it
// needs its own copies of these — nothing is shared between the two Workers
// at runtime.
//
// NOTE: uses Helius's current v0/addresses/{address}/transactions endpoint
// (not the old v1/wallet/history path, which is on the deprecated Enhanced
// Transactions API). This one returns a `type` field per tx (SWAP, TRANSFER,
// etc.) straight from Helius, so swap detection trusts that first and only
// falls back to a "moved 2+ assets" heuristic for unclassified txs.
//
// IMPORTANT — wrangler.jsonc must declare kv_namespaces for TRACKER_KV.
// Bindings added only through the dashboard get wiped on every Git-triggered
// deploy, since Cloudflare treats wrangler.jsonc as the source of truth.

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const MAX_TRADES_STORED = 200;
const MAX_TRACKED_WALLETS = 25; // keeps Helius usage + poll time bounded
const RATE_LIMIT_WINDOW_SEC = 60;
const RATE_LIMIT_MAX_REQUESTS = 10; // per IP, per window, for wallet add/remove
const SYSTEM_PROGRAM_ID = '1'.repeat(32); // owner of every real (non-PDA) wallet account
const BURN_ADDRESSES = new Set([
  '1nc1nerator11111111111111111111111111111111', // Solana's known incinerator/burn address
]);

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
};

function json(body, status = 200, extra = {}){
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', ...extra },
  });
}

function isValidSolanaAddress(addr){
  return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(addr);
}

// Lightweight per-IP rate limit for endpoints that mutate state or could be
// spammed/expensive. Uses a single KV counter per IP per time window rather
// than a sliding log, since this only needs to stop abuse, not be precise.
// Fails open (allows the request) if KV isn't configured yet.
async function checkRateLimit(request, env){
  if(!env.TRACKER_KV) return true;
  const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
  const windowId = Math.floor(Date.now() / 1000 / RATE_LIMIT_WINDOW_SEC);
  const key = `ratelimit:${ip}:${windowId}`;
  const current = parseInt(await env.TRACKER_KV.get(key) || '0', 10);
  if(current >= RATE_LIMIT_MAX_REQUESTS) return false;
  await env.TRACKER_KV.put(key, String(current + 1), { expirationTtl: RATE_LIMIT_WINDOW_SEC * 2 });
  return true;
}

// Runs a list of async tasks with a concurrency cap — a middle ground
// between fully sequential (slow enough to hit Cloudflare's execution
// time limit on anything scanning 15+ wallets) and fully parallel (risks
// tripping Helius's own rate limits when 20+ requests fire at once, which
// would silently return empty results for some wallets rather than a
// clean error). Returns Promise.allSettled-shaped results so callers can
// tell which tasks actually succeeded.
async function runWithConcurrency(items, limit, task){
  const results = new Array(items.length);
  let index = 0;
  async function worker(){
    while(index < items.length){
      const i = index++;
      try{
        results[i] = { status: 'fulfilled', value: await task(items[i], i) };
      }catch(e){
        results[i] = { status: 'rejected', reason: e };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

// Short-lived KV cache for external API lookups (Jupiter price/metadata).
// These get called constantly — every alert, every scan, every score —
// often for the exact same token within seconds of each other (e.g. one
// poll cycle alerting on the same pumping token across several tracked
// wallets). Caching cuts that down to one real request instead of N,
// which reduces how often this app can trip Helius's or Jupiter's own
// rate limits in the first place. Fails open (just calls fetcher directly)
// if KV isn't configured yet, so this never blocks anything from working.
async function getCached(env, key, ttlSeconds, fetcher){
  if(!env.TRACKER_KV) return fetcher();
  try{
    const cached = await env.TRACKER_KV.get(key);
    if(cached !== null) return JSON.parse(cached);
  }catch(e){ /* corrupt/missing cache entry — fall through to a fresh fetch */ }
  const value = await fetcher();
  try{
    await env.TRACKER_KV.put(key, JSON.stringify(value), { expirationTtl: ttlSeconds });
  }catch(e){ /* caching is a nice-to-have, never let it block the response */ }
  return value;
}

async function getWallets(env){
  const raw = await env.TRACKER_KV.get('wallets');
  return raw ? JSON.parse(raw) : [];
}
async function saveWallets(env, wallets){
  await env.TRACKER_KV.put('wallets', JSON.stringify(wallets));
}
async function getTrades(env){
  const raw = await env.TRACKER_KV.get('trades');
  return raw ? JSON.parse(raw) : [];
}
async function saveTrades(env, trades){
  await env.TRACKER_KV.put('trades', JSON.stringify(trades.slice(0, MAX_TRADES_STORED)));
}

// Remembers the market cap at the moment a tracked wallet buys a token, so
// that when it later sells the same token, the alert can show the full
// trajectory ("bought at $86K MC, sold at $210K MC") instead of just the
// exit price in isolation — a much faster "did this actually work out"
// signal than reading raw SOL amounts. Overwritten on every new buy of the
// same mint, so it always reflects the most recent entry point rather than
// trying to track multiple partial positions. 30-day TTL so long-abandoned
// entries don't accumulate forever in KV.
async function getEntryMarketCap(env, wallet, mint){
  if(!env.TRACKER_KV) return null;
  try{
    const raw = await env.TRACKER_KV.get(`entrymc:${wallet}:${mint}`);
    return raw ? JSON.parse(raw) : null;
  }catch(e){ return null; }
}
async function setEntryMarketCap(env, wallet, mint, marketCap, timestamp){
  if(!env.TRACKER_KV || marketCap == null) return;
  try{
    await env.TRACKER_KV.put(
      `entrymc:${wallet}:${mint}`,
      JSON.stringify({ mc: marketCap, timestamp }),
      { expirationTtl: 60 * 60 * 24 * 30 }
    );
  }catch(e){ /* best-effort — a missed write just means no trajectory shown on the next sell */ }
}

// Helius's old v1/wallet/history endpoint (Enhanced Transactions API) is
// deprecated. The current recommended path is the parsed-transactions
// endpoint below, which returns a `type` field (SWAP, TRANSFER, etc.)
// straight from Helius instead of us having to guess from raw balance
// deltas. Falls back to an empty array on any failure so a single bad
// wallet/API hiccup doesn't kill the whole poll cycle / scan for others.
//
// Works for any address, not just wallets — a token mint's own address has
// a transaction history too (every transfer/swap involving it), which is
// how the early-buyer scan below works.
async function fetchAddressTransactions(address, env, { limit = 25, sortOrder, before } = {}){
  let url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?api-key=${env.HELIUS_API_KEY}&limit=${limit}`;
  if(sortOrder) url += `&sort-order=${sortOrder}`;
  if(before) url += `&before=${before}`;
  try{
    const res = await fetch(url);
    if(!res.ok){
      console.warn(`Helius history fetch failed for ${address}: ${res.status}`);
      return [];
    }
    const data = await res.json();
    return Array.isArray(data) ? data : [];
  }catch(e){
    console.warn(`Helius history fetch threw for ${address}:`, e);
    return [];
  }
}

// Helius caps a single call at 100 transactions. For a deeper look, this
// pages backward: fetch the first 100, then ask for the next batch "before"
// the oldest signature seen so far, repeating until `limit` is reached or
// the wallet runs out of history. Only used when a depth beyond 100 is
// actually requested — the common 100-tx case stays a single call.
async function fetchWalletHistoryPaged(address, env, limit = 100){
  if(limit <= 100) return fetchWalletHistory(address, env, limit);

  let all = await fetchAddressTransactions(address, env, { limit: 100 });
  while(all.length < limit){
    const lastSig = all[all.length - 1]?.signature;
    if(!lastSig) break;
    const remaining = limit - all.length;
    const next = await fetchAddressTransactions(address, env, { limit: Math.min(remaining, 100), before: lastSig });
    if(!next.length) break; // wallet has no more history to page through
    all = all.concat(next);
  }
  return all;
}

async function fetchWalletHistory(address, env, limit = 25){
  return fetchAddressTransactions(address, env, { limit });
}

// Reduces a parsed transaction's tokenTransfers + nativeTransfers down to
// this wallet's net change per mint (e.g. -1.2 SOL, +48000 MAD). Merges
// multiple transfers of the same mint within one tx into a single line.
function walletChanges(tx, address){
  const totals = {};

  // SOL: prefer accountData's nativeBalanceChange — the actual net lamport
  // delta for this account (computed from real before/after balances), not
  // a manual sum of individual transfer instructions. Some AMM programs
  // (Pump.fun's post-migration pool is one) settle SOL through the pool
  // contract in a way that doesn't fully show up as enumerable
  // nativeTransfers entries, while a small separate fee/tip transfer does —
  // summing nativeTransfers alone can end up picking up just the fee and
  // completely missing the actual trade proceeds. accountData can't have
  // that gap, since it isn't counting individual legs at all.
  const acctEntry = (tx.accountData || []).find(a => a.account === address);
  if(acctEntry && typeof acctEntry.nativeBalanceChange === 'number' && acctEntry.nativeBalanceChange !== 0){
    totals[SOL_MINT] = acctEntry.nativeBalanceChange / 1e9;
  } else {
    for(const nt of tx.nativeTransfers || []){
      if(nt.fromUserAccount === address) totals[SOL_MINT] = (totals[SOL_MINT]||0) - nt.amount / 1e9;
      if(nt.toUserAccount === address) totals[SOL_MINT] = (totals[SOL_MINT]||0) + nt.amount / 1e9;
    }
  }

  for(const tt of tx.tokenTransfers || []){
    const mint = tt.mint;
    if(!mint) continue;
    if(tt.fromUserAccount === address) totals[mint] = (totals[mint]||0) - tt.tokenAmount;
    if(tt.toUserAccount === address) totals[mint] = (totals[mint]||0) + tt.tokenAmount;
  }
  return Object.entries(totals)
    .filter(([, amount]) => Math.abs(amount) > 0.000001)
    .map(([mint, amount]) => ({ mint, amount }));
}

// Trust Helius's own `type` classification first (SWAP is set for Jupiter,
// Raydium, Pump.fun, etc.). Fall back to the "moved 2+ assets" heuristic
// only for transactions Helius didn't classify, so a custom/unlisted AMM
// program doesn't get silently dropped from the feed.
function detectSwap(tx, address){
  const changes = walletChanges(tx, address);
  if(tx.type === 'SWAP') return changes.length ? changes : null;
  // Helius's type tag isn't reliable for every routed swap — a multi-hop
  // trade (e.g. SOL -> USDC -> TOKEN through an aggregator) can come back
  // tagged as something other than SWAP even though the wallet's net
  // change is unmistakably "spent one asset, received another" (the
  // intermediate leg nets to zero and drops out of walletChanges already).
  // Trust that pattern regardless of what Helius labeled the tx as, not
  // only when the type is missing or UNKNOWN.
  return changes.length >= 2 ? changes : null;
}

function shortAddr(addr){
  return addr ? `${addr.slice(0,4)}…${addr.slice(-4)}` : '';
}

function walletLabel(wallet){
  const short = wallet.address.slice(0,4) + '…' + wallet.address.slice(-4);
  return wallet.label ? `${wallet.label} (${short})` : short;
}

// Resolves a mint to its symbol/name via Jupiter's Token API v2 (same lite
// endpoint family already used for pricing elsewhere in the degen suite —
// no API key needed on this tier). SOL is hardcoded since it's not worth a
// lookup. Falls back to nulls on any failure so a metadata hiccup never
// blocks an alert from sending — the alert just shows the raw mint instead.
async function getTokenMeta(mint, env){
  if(mint === SOL_MINT) return { symbol: 'SOL', name: 'Solana' };
  return getCached(env, `meta:${mint}`, 3600, async () => {
    try{
      const res = await fetch(`https://lite-api.jup.ag/tokens/v2/search?query=${mint}`);
      if(!res.ok) return { symbol: null, name: null };
      const data = await res.json();
      const match = Array.isArray(data) ? (data.find(t => t.address === mint) || data[0]) : null;
      return match ? { symbol: match.symbol || null, name: match.name || null } : { symbol: null, name: null };
    }catch(e){ return { symbol: null, name: null }; }
  });
}

function formatUsd(value){
  if(value == null) return null;
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: value < 1 ? 6 : 2 });
}

// Compact form for large numbers in alert messages — "$1.24M" instead of
// "$1,240,000.00", since market caps get unwieldy fast and alerts need to
// stay scannable at a glance.
function formatCompactUsd(value){
  if(value == null) return null;
  const abs = Math.abs(value);
  if(abs >= 1e9) return '$' + (value / 1e9).toFixed(2) + 'B';
  if(abs >= 1e6) return '$' + (value / 1e6).toFixed(2) + 'M';
  if(abs >= 1e3) return '$' + (value / 1e3).toFixed(1) + 'K';
  return '$' + value.toFixed(2);
}

// Rough market cap: current price × total supply. This is fully-diluted
// valuation, not strictly circulating market cap — for most memecoins
// (no vesting/locked supply) the two are close enough to be useful, but
// for a token with significant locked/vested supply this will read high.
// Not applied to SOL itself — "market cap of SOL" isn't the useful number
// in a whale-alert context, so it's skipped for that mint.
async function getTokenMarketCap(mint, env){
  if(mint === SOL_MINT) return null;
  try{
    const [supplyResult, price] = await Promise.all([
      rpcCall(env, 'getTokenSupply', [mint]),
      getTokenPriceUsd(mint, env),
    ]);
    const supply = Number(supplyResult?.value?.uiAmount || 0);
    if(!supply || price == null) return null;
    return supply * price;
  }catch(e){ return null; }
}

// Same Jupiter Price API v3 used elsewhere in the degen suite (Rekt or
// Rich). Free lite endpoint, no key needed. Returns null on any failure so
// a price hiccup degrades to "show token amounts only" instead of erroring
// out the whole scan.
async function getTokenPriceUsd(mint, env){
  return getCached(env, `price:${mint}`, 25, async () => {
    try{
      const res = await fetch(`https://lite-api.jup.ag/price/v3?ids=${mint}`);
      if(!res.ok) return null;
      const data = await res.json();
      return data?.[mint]?.usdPrice ?? null;
    }catch(e){ return null; }
  });
}

// Rough bot/sniper signal: walks a wallet's recent swap history in
// chronological order and measures how quickly it typically flips a
// position (time between buying a token and selling that same token
// again). Bots that snipe new launches tend to hold for seconds to a few
// minutes across a high volume of trades; a discretionary trader's hold
// times are usually longer and far less uniform. This is a heuristic, not
// a certainty — a genuinely fast human scalper would also trip it, so
// treat it as a flag worth checking, not a verdict.
async function analyzeSniperBehavior(address, env, historyLimit = 100){
  const history = await fetchWalletHistory(address, env, historyLimit);
  const chron = [...history].reverse(); // oldest first — needed to measure hold time correctly
  const openBuys = {}; // mint -> FIFO queue of buy timestamps
  const flipSeconds = [];
  let swapCount = 0;

  for(const tx of chron){
    if(tx.transactionError) continue;
    const changes = detectSwap(tx, address);
    if(!changes) continue;
    swapCount += 1;
    for(const c of changes){
      if(c.mint === SOL_MINT) continue;
      if(c.amount > 0){
        if(!openBuys[c.mint]) openBuys[c.mint] = [];
        openBuys[c.mint].push(tx.timestamp);
      } else if(c.amount < 0 && openBuys[c.mint] && openBuys[c.mint].length){
        const buyTs = openBuys[c.mint].shift();
        flipSeconds.push(tx.timestamp - buyTs);
      }
    }
  }

  const avgFlipSeconds = flipSeconds.length
    ? Math.round(flipSeconds.reduce((a, b) => a + b, 0) / flipSeconds.length)
    : null;
  // Active (12+ swaps in the fetched window) AND fast-flipping (under 10 min
  // average hold) reads as automated rather than discretionary.
  const likelyBot = swapCount >= 12 && avgFlipSeconds != null && avgFlipSeconds < 600;

  return { swapCount, avgFlipSeconds, flipsMeasured: flipSeconds.length, likelyBot };
}

// Checks one wallet's recent swap history for genuine buys of the target
// mint. Shared by findMintBuyers (flat list) and traceLineageForBuyers
// (multi-hop), so "bought" means exactly the same thing everywhere. Does
// NOT count plain transfers/airdrops as a "buy" — only actual swaps.
async function checkWalletBought(address, targetMint, env, historyLimit = 50){
  const history = await fetchWalletHistory(address, env, historyLimit);
  let totalBought = 0, buyCount = 0, lastBuyTimestamp = null;
  for(const tx of history){
    if(tx.transactionError) continue;
    const changes = detectSwap(tx, address);
    if(!changes) continue;
    const bought = changes.find(c => c.mint === targetMint && c.amount > 0);
    if(bought){
      totalBought += bought.amount;
      buyCount += 1;
      if(!lastBuyTimestamp || tx.timestamp > lastBuyTimestamp) lastBuyTimestamp = tx.timestamp;
    }
  }
  return totalBought > 0 ? { totalBought, buyCount, lastBuyTimestamp } : null;
}

// Flat version: checks a given list of wallets against one target mint.
async function findMintBuyers(wallets, targetMint, env){
  const priceUsd = await getTokenPriceUsd(targetMint, env);

  const settled = await runWithConcurrency(wallets, 5, wallet => checkWalletBought(wallet.address, targetMint, env));

  const results = [];
  wallets.forEach((wallet, i) => {
    if(settled[i].status !== 'fulfilled' || !settled[i].value) return;
    const bought = settled[i].value;
    results.push({
      address: wallet.address,
      label: wallet.label || null,
      totalBought: bought.totalBought,
      valueUsd: priceUsd != null ? bought.totalBought * priceUsd : null,
      buyCount: bought.buyCount,
      lastBuyTimestamp: bought.lastBuyTimestamp,
    });
  });

  results.sort((a, b) => (b.valueUsd ?? b.totalBought) - (a.valueUsd ?? a.totalBought));
  return { results, priceUsd, scannedWallets: wallets.length };
}

// Generic Solana JSON-RPC call, routed through Helius (same API key you
// already use for transaction history — no separate RPC provider needed).
async function rpcCall(env, method, params){
  const url = `https://mainnet.helius-rpc.com/?api-key=${env.HELIUS_API_KEY}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if(!res.ok) throw new Error(`RPC ${method} failed: ${res.status}`);
  const data = await res.json();
  if(data.error) throw new Error(`RPC ${method} error: ${data.error.message || 'unknown'}`);
  return data.result;
}

// Same wallet-vs-program-account check used in scanTokenHolders, pulled out
// standalone so early-buyer detection can reuse it without re-scanning
// holders — pool/vault addresses show up as "buyers" in raw transfer data
// too, and need the same filtering.
async function classifyOwners(addresses, env){
  if(!addresses.length) return {};
  const infos = await rpcCall(env, 'getMultipleAccounts', [addresses, { encoding: 'base64' }]);
  const map = {};
  addresses.forEach((addr, i) => {
    const info = infos?.value?.[i];
    map[addr] = info && info.owner === SYSTEM_PROGRAM_ID ? 'wallet' : 'program';
  });
  return map;
}

// Given a token mint, finds its top ~20 holder accounts and classifies each
// as a real wallet vs. a program-controlled account (LP pool, staking
// vault, PDA, etc.) so the UI can show actual whales instead of a list
// that's mostly Raydium/Pump.fun pool addresses.
//
// The classification trick: every ordinary wallet's on-chain account is
// owned by the System Program. Pool vaults, staking accounts, and other
// PDAs are owned by whatever program created them. Checking that one field
// is enough to tell "trader" from "infrastructure" without guessing.
async function scanTokenHolders(mint, env){
  const largest = await rpcCall(env, 'getTokenLargestAccounts', [mint]);
  const tokenAccounts = (largest?.value || []).filter(a => Number(a.amount) > 0);
  if(!tokenAccounts.length) return { holders: [], totalSupply: 0 };

  const accountInfos = await rpcCall(env, 'getMultipleAccounts', [
    tokenAccounts.map(a => a.address),
    { encoding: 'jsonParsed' },
  ]);
  const owned = tokenAccounts.map((acc, i) => {
    const parsed = accountInfos?.value?.[i]?.data?.parsed?.info;
    return { owner: parsed?.owner || null, amount: Number(acc.uiAmount) };
  }).filter(o => o.owner);

  const supplyResult = await rpcCall(env, 'getTokenSupply', [mint]);
  const totalSupply = Number(supplyResult?.value?.uiAmount || 0);

  const uniqueOwners = [...new Set(owned.map(o => o.owner))];
  const ownerInfos = await rpcCall(env, 'getMultipleAccounts', [uniqueOwners, { encoding: 'base64' }]);
  const ownerType = {};
  uniqueOwners.forEach((addr, i) => {
    const info = ownerInfos?.value?.[i];
    ownerType[addr] = info && info.owner === SYSTEM_PROGRAM_ID ? 'wallet' : 'program';
  });

  const holders = owned
    .map(o => ({
      address: o.owner,
      amount: o.amount,
      pctSupply: totalSupply > 0 ? (o.amount / totalSupply) * 100 : 0,
      type: BURN_ADDRESSES.has(o.owner) ? 'burn' : (ownerType[o.owner] || 'unknown'),
    }))
    .sort((a, b) => b.amount - a.amount);

  return { holders, totalSupply };
}

// Reconstructs realized PnL for a wallet the same way Rekt or Rich does:
// FIFO cost-basis matching, denominated in SOL rather than USD. Only swaps
// with a SOL leg are counted — that's how "cost" and "proceeds" get a
// common unit without needing historical price data per trade, since the
// SOL amount in the swap itself IS the price paid/received at that moment.
// Token-to-token swaps (no SOL leg) are skipped for this reason; there's no
// shared denominator to compute PnL against without a price oracle.
//
// Scope limitation: only the wallet's most recent `historyLimit`
// transactions are considered, same as everywhere else in this app — this
// is "recent form," not a lifetime audit.
async function scoreWalletPerformance(address, env, historyLimit = 100){
  const history = await fetchWalletHistoryPaged(address, env, historyLimit);
  const chron = [...history].reverse(); // oldest first, needed for correct FIFO matching

  const openLots = {}; // mint -> FIFO queue of { amount, costSol }
  const closedTrades = []; // { mint, pnlSol, pnlPercent, timestamp }

  for(const tx of chron){
    if(tx.transactionError) continue;
    const changes = detectSwap(tx, address);
    if(!changes) continue;

    const solChange = changes.find(c => c.mint === SOL_MINT);
    if(!solChange) continue; // token-to-token swap, no common denominator — skip

    const tokenChange = changes.find(c => c.mint !== SOL_MINT);
    if(!tokenChange) continue;

    if(solChange.amount < 0){
      // Bought tokenChange.mint with SOL — open a new cost-basis lot.
      const costSol = Math.abs(solChange.amount);
      const amount = Math.abs(tokenChange.amount);
      if(!openLots[tokenChange.mint]) openLots[tokenChange.mint] = [];
      openLots[tokenChange.mint].push({ amount, costSol });
    } else {
      // Sold tokenChange.mint for SOL — match against open lots, FIFO.
      const proceedsSol = solChange.amount;
      let remaining = Math.abs(tokenChange.amount);
      let costBasisTotal = 0;
      const lots = openLots[tokenChange.mint];
      if(lots && lots.length){
        while(remaining > 0.000001 && lots.length){
          const lot = lots[0];
          if(lot.amount <= remaining + 0.000001){
            costBasisTotal += lot.costSol;
            remaining -= lot.amount;
            lots.shift();
          } else {
            const portion = remaining / lot.amount;
            const portionCost = lot.costSol * portion;
            costBasisTotal += portionCost;
            lot.amount -= remaining;
            lot.costSol -= portionCost;
            remaining = 0;
          }
        }
        const pnlSol = proceedsSol - costBasisTotal;
        const pnlPercent = costBasisTotal > 0 ? (pnlSol / costBasisTotal) * 100 : null;
        closedTrades.push({
          mint: tokenChange.mint, pnlSol, pnlPercent, timestamp: tx.timestamp,
          amount: Math.abs(tokenChange.amount), costBasisTotal, proceedsSol,
        });
      }
      // If there's no matching open lot (e.g. the buy happened before our
      // history window), the sale is simply not scored — no invented cost
      // basis, since that would fabricate a number we can't back up.
    }
  }

  const totalTrades = closedTrades.length;
  const wins = closedTrades.filter(t => t.pnlSol > 0).length;
  const winRate = totalTrades ? (wins / totalTrades) * 100 : null;
  const totalPnlSol = closedTrades.reduce((sum, t) => sum + t.pnlSol, 0);
  const avgPnlSol = totalTrades ? totalPnlSol / totalTrades : null;
  const best = totalTrades ? closedTrades.reduce((a, b) => (b.pnlSol > a.pnlSol ? b : a)) : null;
  const worst = totalTrades ? closedTrades.reduce((a, b) => (b.pnlSol < a.pnlSol ? b : a)) : null;
  const openPositions = Object.values(openLots).filter(lots => lots.length > 0).length;

  const solPriceUsd = await getTokenPriceUsd(SOL_MINT, env);
  const totalPnlUsdApprox = solPriceUsd != null ? totalPnlSol * solPriceUsd : null;

  // Back into an approximate entry/exit market cap for the highlighted
  // trades — same idea as the live alert trajectory, just reconstructed
  // after the fact from the trade's own SOL amounts instead of tracked in
  // real time. Uses today's SOL price and today's token supply for both
  // sides of the same trade, which means the resulting % change is
  // mathematically the same number as pnlPercent above — this isn't new
  // information, just a more concrete, tangible way to see it ($86K -> 
  // $210K reads faster than "+144%"). Only computed for best/worst, not
  // every trade, to keep this cheap regardless of history depth.
  const tradesToEnrich = best === worst ? [best] : [best, worst].filter(Boolean);
  for(const trade of tradesToEnrich){
    trade.entryMarketCap = null;
    trade.exitMarketCap = null;
    if(solPriceUsd == null || !trade.amount) continue;
    try{
      const supplyResult = await rpcCall(env, 'getTokenSupply', [trade.mint]);
      const supply = Number(supplyResult?.value?.uiAmount || 0);
      if(supply > 0){
        trade.entryMarketCap = (trade.costBasisTotal / trade.amount) * solPriceUsd * supply;
        trade.exitMarketCap = (trade.proceedsSol / trade.amount) * solPriceUsd * supply;
      }
    }catch(e){ /* leave nulls — trade still shows, just without the MC line */ }
  }

  return {
    totalTrades, winRate, totalPnlSol, avgPnlSol, best, worst,
    openPositions, solPriceUsd, totalPnlUsdApprox,
    historyDepth: historyLimit, transactionsScanned: history.length,
  };
}


// Scores every tracked wallet at once and ranks by realized PnL — the
// "who's actually good, out of everyone I'm watching" view, instead of
// checking wallets one at a time. Reuses scoreWalletPerformance exactly,
// so a wallet's numbers here always match what the single-wallet Score
// tool would show for the same wallet.
async function scoreAllTrackedWallets(env, historyLimit = 100){
  const wallets = await getWallets(env);

  const settled = await runWithConcurrency(wallets, 5, wallet => scoreWalletPerformance(wallet.address, env, historyLimit));

  const results = [];
  wallets.forEach((wallet, i) => {
    if(settled[i].status !== 'fulfilled') return;
    results.push({ address: wallet.address, label: wallet.label || null, ...settled[i].value });
  });

  // Rank by USD PnL when available (falls back to SOL PnL). Wallets with
  // zero closed trades sort to the bottom rather than cluttering the top.
  results.sort((a, b) => {
    const aVal = a.totalTrades ? (a.totalPnlUsdApprox ?? a.totalPnlSol) : -Infinity;
    const bVal = b.totalTrades ? (b.totalPnlUsdApprox ?? b.totalPnlSol) : -Infinity;
    return bVal - aVal;
  });
  return { results, walletsScored: wallets.length };
}

// Finds the earliest wallets to buy a token after its creation, then
// classifies each as likely-bot or likely-discretionary using
// analyzeSniperBehavior. Pool/vault addresses that receive the token during
// initial liquidity setup are filtered out the same way whale-scan filters
// them — those aren't buyers, they're infrastructure.
//
// Cost note: this does one call to fetch the token's early history, one
// batched call to classify owners, then one more call per surviving
// candidate to analyze their trading pattern — so it scales with how many
// early buyers are found, capped at maxCandidates to keep it bounded.
async function findEarlyBuyers(mint, env, { earlyLimit = 100, maxCandidates = 15 } = {}){
  const history = await fetchAddressTransactions(mint, env, { limit: earlyLimit, sortOrder: 'asc' });
  if(!history.length) return { creationTimestamp: null, results: [] };

  const creationTimestamp = history[0].timestamp || null;

  // Earliest transfer of the token to each address, in chronological order.
  const seen = new Map();
  for(const tx of history){
    if(tx.transactionError) continue;
    for(const tt of tx.tokenTransfers || []){
      if(tt.mint !== mint || !tt.toUserAccount) continue;
      if(!seen.has(tt.toUserAccount)){
        seen.set(tt.toUserAccount, { timestamp: tx.timestamp, amount: tt.tokenAmount });
      }
    }
  }

  const candidates = [...seen.entries()].sort((a, b) => a[1].timestamp - b[1].timestamp);
  const addresses = candidates.map(([addr]) => addr);
  const ownerType = await classifyOwners(addresses, env);

  const wallets = candidates
    .filter(([addr]) => ownerType[addr] === 'wallet')
    .slice(0, maxCandidates);

  // Run all candidates' sniper analysis in parallel instead of one at a
  // time — sequential awaiting here meant up to 15 full history fetches
  // stacked end to end, easily enough to hit Cloudflare's execution time
  // limit on a token with a lot of early activity. Promise.allSettled also
  // means one candidate's failure can't take the whole scan down with it;
  // it's just dropped from the results instead. Bounded to 5 at once so it
  // doesn't trip Helius's own rate limits on a token with many candidates.
  const settled = await runWithConcurrency(wallets, 5, ([address]) => analyzeSniperBehavior(address, env));

  const results = [];
  wallets.forEach(([address, info], i) => {
    if(settled[i].status !== 'fulfilled') return;
    results.push({
      address,
      secondsAfterLaunch: creationTimestamp != null ? info.timestamp - creationTimestamp : null,
      amountBought: info.amount,
      ...settled[i].value,
    });
  });

  return { creationTimestamp, results };
}

// Checks several known-winner tokens at once and finds wallets that show
// up as a real (non-pool/program) holder across multiple of them — a
// wallet holding 3 of 5 given winners is a much stronger signal than one
// that holds just one. Uses current top-holder snapshots (same filtering
// as whale-scan), not "earliest post-launch buyer" — that population is
// dominated by snipers/bots and misses genuine holders who bought a bit
// later but still built a real position, which is what actually matters
// for a reputation check.
async function scoreTokenOverlap(mints, env){
  const tokenResults = await Promise.all(mints.map(async mint => {
    const [{ holders }, meta] = await Promise.all([
      scanTokenHolders(mint, env),
      getTokenMeta(mint, env),
    ]);
    const wallets = holders.filter(h => h.type === 'wallet');
    return { mint, symbol: meta.symbol, buyersFound: wallets.length, wallets };
  }));

  const overlap = {};
  for(const tr of tokenResults){
    for(const w of tr.wallets){
      if(!overlap[w.address]){
        overlap[w.address] = { address: w.address, count: 0, tokens: [] };
      }
      overlap[w.address].count += 1;
      overlap[w.address].tokens.push({ mint: tr.mint, symbol: tr.symbol, pctSupply: w.pctSupply });
    }
  }

  const results = Object.values(overlap)
    .filter(w => w.count >= 2)
    .sort((a, b) => b.count - a.count);

  return {
    tokensScanned: tokenResults.map(t => ({ mint: t.mint, symbol: t.symbol, buyersFound: t.buyersFound })),
    results,
  };
}

// One-shot version of the whale-scan + buyer-check combo: finds token A's
// top holders (already filtered to real wallets, not pools/programs), then
// checks each one for genuine swap-buys of token B. Nothing gets saved —
// no tracking, no KV — this is purely "who overlaps" on demand.
async function scanCrossBuyers(sourceMint, targetMint, env){
  const { holders } = await scanTokenHolders(sourceMint, env);
  const whales = holders.filter(h => h.type === 'wallet');
  if(!whales.length) return { results: [], scannedWallets: 0, priceUsd: null };

  const wallets = whales.map(h => ({ address: h.address, label: null }));
  const { results, priceUsd } = await findMintBuyers(wallets, targetMint, env);

  const pctBySource = Object.fromEntries(whales.map(h => [h.address, h.pctSupply]));
  const enriched = results.map(r => ({ ...r, sourcePctSupply: pctBySource[r.address] ?? null }));

  return { results: enriched, scannedWallets: wallets.length, priceUsd };
}

// A token's creation transaction is public — pulling the mint address's
// very first transaction and reading who paid for it identifies the
// deployer in most cases, especially pump.fun-style launches where the
// creator wallet signs the creation tx directly. Less reliable for tokens
// launched through a program/factory, where the fee payer might be a
// deployer *contract* rather than a person's wallet.
async function findTokenDeployer(mint, env){
  const history = await fetchAddressTransactions(mint, env, { limit: 1, sortOrder: 'asc' });
  const tx = history[0];
  if(!tx) return { deployer: null, signature: null, timestamp: null };
  return { deployer: tx.feePayer || null, signature: tx.signature || null, timestamp: tx.timestamp || null };
}

// Multi-hop funding-lineage trace: starting from a known wallet, follows
// the chain of "who did this wallet fund with SOL" outward several hops,
// checking every wallet discovered along the way for a genuine buy of the
// target token. Each level is fetched in parallel (Promise.all) to keep
// wall-clock time down, and each wallet's history is fetched once and
// reused for both the buy-check and the next hop's expansion.
//
// Bounded on three axes so this can't run away: depth, branching factor per
// wallet (top N funded addresses by SOL, not all of them), and a hard cap
// on total wallets scanned across the whole trace. Only catches on-chain
// funding — if a burner was funded straight from a CEX withdrawal, there's
// no link to follow and it won't show up here.
async function traceLineageForBuyers(rootWallet, targetMint, env, opts = {}){
  const {
    maxDepth = 2,
    maxFundedPerWallet = 8,
    maxTotalWallets = 40,
    minSol = 0.05,
    historyLimit = 100,
  } = opts;

  const visited = new Set([rootWallet]);
  let currentLevel = [{ address: rootWallet, fundedBy: null }];
  const foundBuyers = {};
  let scanned = 0;

  for(let depth = 0; depth <= maxDepth && currentLevel.length && scanned < maxTotalWallets; depth++){
    const budget = maxTotalWallets - scanned;
    const batch = currentLevel.slice(0, budget);
    scanned += batch.length;

    const histories = await Promise.all(
      batch.map(node => fetchWalletHistory(node.address, env, historyLimit).catch(() => []))
    );

    const nextLevel = [];
    batch.forEach((node, i) => {
      const history = histories[i];

      // Check this wallet for genuine swap-buys of the target token
      // (skip the root itself — that's the already-known wallet).
      if(depth > 0){
        for(const tx of history){
          if(tx.transactionError) continue;
          const changes = detectSwap(tx, node.address);
          if(!changes) continue;
          const bought = changes.find(c => c.mint === targetMint && c.amount > 0);
          if(!bought) continue;
          if(!foundBuyers[node.address]){
            foundBuyers[node.address] = {
              address: node.address, depth, fundedBy: node.fundedBy,
              totalBought: 0, buyCount: 0, lastBuyTimestamp: null,
            };
          }
          const f = foundBuyers[node.address];
          f.totalBought += bought.amount;
          f.buyCount += 1;
          if(!f.lastBuyTimestamp || tx.timestamp > f.lastBuyTimestamp) f.lastBuyTimestamp = tx.timestamp;
        }
      }

      // Expand outward: who did this wallet fund directly with SOL?
      if(depth < maxDepth){
        const funded = {};
        for(const tx of history){
          if(tx.transactionError) continue;
          for(const nt of tx.nativeTransfers || []){
            if(nt.fromUserAccount !== node.address || !nt.toUserAccount || nt.toUserAccount === node.address) continue;
            const sol = nt.amount / 1e9;
            if(sol < minSol) continue;
            funded[nt.toUserAccount] = (funded[nt.toUserAccount] || 0) + sol;
          }
        }
        Object.entries(funded)
          .sort((a, b) => b[1] - a[1])
          .slice(0, maxFundedPerWallet)
          .forEach(([addr]) => {
            if(visited.has(addr)) return;
            visited.add(addr);
            nextLevel.push({ address: addr, fundedBy: node.address });
          });
      }
    });

    currentLevel = nextLevel;
  }

  const priceUsd = await getTokenPriceUsd(targetMint, env);
  const results = Object.values(foundBuyers)
    .map(r => ({ ...r, valueUsd: priceUsd != null ? r.totalBought * priceUsd : null }))
    .sort((a, b) => (b.valueUsd ?? b.totalBought) - (a.valueUsd ?? a.totalBought));

  return { results, scannedWallets: scanned, priceUsd, truncated: scanned >= maxTotalWallets };
}

// Labels a detected event for display/alerts, using four distinct colors:
// green for buys, red for sells, orange for token-to-token swaps, yellow
// for plain transfers (sent or received — direction still shown in the
// label text so the two aren't confused with each other).
function classifyEvent(changes, isSwap){
  if(isSwap){
    const solChange = changes.find(c => c.mint === SOL_MINT);
    if(!solChange) return { label: 'SWAP', emoji: '🔁' };
    return solChange.amount < 0 ? { label: 'BUY', emoji: '🟢' } : { label: 'SELL', emoji: '🔴' };
  }
  const primary = changes[0];
  if(!primary) return { label: 'TRANSFER', emoji: '🟡' };
  return primary.amount < 0 ? { label: 'SENT', emoji: '🟡' } : { label: 'RECEIVED', emoji: '🟡' };
}

// Picks "the coin" an alert should foreground: the asset being acquired for
// a BUY or a token-to-token SWAP, the asset being offloaded for a SELL, or
// whatever single asset moved for a plain transfer.
function primaryAsset(changes, label){
  if(label === 'SELL') return changes.find(c => c.mint !== SOL_MINT && c.amount < 0) || changes[0];
  if(label === 'BUY' || label === 'SWAP') return changes.find(c => c.mint !== SOL_MINT && c.amount > 0) || changes[0];
  return changes[0]; // SENT/RECEIVED transfers
}

function formatChangeLines(changes){
  return changes.map(c=>{
    const sym = (c.mint||'').trim() === SOL_MINT ? 'SOL' : (c.mint||'').slice(0,4)+'…';
    const amt = Math.abs(c.amount).toLocaleString(undefined,{maximumFractionDigits:4});
    return `${c.amount>0?'+':'-'}${amt} ${sym}`;
  }).join('  ·  ');
}

// Builds the shared "here's the coin" block used by both alert channels:
// symbol, name, contract address (skipped for SOL — not worth showing),
// amount, and USD value if a price was resolved. context = { asset, meta,
// usdValue }, all optional — gracefully degrades to just the mint if
// Jupiter didn't recognize the token.
// Telegram's HTML parse mode only needs these three characters escaped —
// much simpler and more reliable than MarkdownV2, which requires escaping
// a long list of punctuation everywhere outside code blocks. Applied to
// any dynamic text (wallet labels, token names) that isn't already known
// to be safe, so a label with a stray "&" or "<" in it can't break the
// whole message's formatting.
function escapeTelegramHtml(str){
  if(str == null) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Builds the shared "here's the coin" block for both alert channels.
// format: 'discord' uses Discord's markdown (backticks still copyable
// there via long-press/select, which was never the complaint). format:
// 'telegram' uses real HTML <code> tags for the CA specifically — that's
// what gives Telegram's one-tap-to-copy behavior, versus plain text which
// only supports press-and-hold-to-select.
function buildTokenBlock(context, { format }){
  const { asset, meta, usdValue, marketCap, entryMarketCap } = context || {};
  if(!asset) return '';
  const isTelegram = format === 'telegram';
  const isDiscord = format === 'discord';

  const symbolRaw = meta?.symbol ? `$${meta.symbol}` : shortAddr(asset.mint);
  const symbolText = isTelegram ? escapeTelegramHtml(symbolRaw) : symbolRaw;
  const symbol = isDiscord ? `**${symbolText}**` : (isTelegram ? `<b>${symbolText}</b>` : symbolText);

  const nameRaw = meta?.name ? ` (${meta.name})` : '';
  const nameLine = isTelegram ? escapeTelegramHtml(nameRaw) : nameRaw;

  let caLine = '';
  if(asset.mint !== SOL_MINT){
    if(isDiscord) caLine = `\nCA: \`${asset.mint}\``;
    else if(isTelegram) caLine = `\nCA: <code>${asset.mint}</code>`; // one-tap copy in Telegram
    else caLine = `\nCA: ${asset.mint}`;
  }

  const symbolSuffix = meta?.symbol ? ' ' + (isTelegram ? escapeTelegramHtml(meta.symbol) : meta.symbol) : '';
  const amt = `${asset.amount > 0 ? '+' : '-'}${Math.abs(asset.amount).toLocaleString(undefined,{maximumFractionDigits:4})}${symbolSuffix}`;
  const usd = usdValue != null ? ` (~$${formatUsd(usdValue)})` : '';

  let mcLine = '';
  if(entryMarketCap != null && marketCap != null){
    // Selling a token we saw this wallet buy — show the full trajectory,
    // not just the exit price. This is the "did it actually work out"
    // signal at a glance.
    const pctChange = entryMarketCap > 0 ? ((marketCap - entryMarketCap) / entryMarketCap) * 100 : null;
    const trend = marketCap >= entryMarketCap ? '📈' : '📉';
    const pctText = pctChange != null ? ` (${pctChange >= 0 ? '+' : ''}${pctChange.toFixed(1)}%)` : '';
    mcLine = `\nMC: ${formatCompactUsd(entryMarketCap)} → ${formatCompactUsd(marketCap)} ${trend}${pctText}`;
  } else if(marketCap != null){
    mcLine = `\nMC: ~${formatCompactUsd(marketCap)}`;
  }

  return `${symbol}${nameLine}${caLine}\n${amt}${usd}${mcLine}\n\n`;
}

// Maps an event label to the natural verb used in the alert header —
// "Papoi bought" reads better than "Whale BUY detected / Papoi".
function actionVerb(label){
  switch(label){
    case 'BUY': return 'bought';
    case 'SELL': return 'sold';
    case 'SWAP': return 'swapped';
    case 'SENT': return 'sent';
    case 'RECEIVED': return 'received';
    default: return 'moved';
  }
}

async function sendDiscordAlert(env, wallet, tx, changes, isSwap = true, context = {}){
  if(!env.DISCORD_WEBHOOK_URL) return;
  const { label, emoji } = classifyEvent(changes, isSwap);
  const verb = actionVerb(label);
  const tokenBlock = buildTokenBlock(context, { format: 'discord' });
  const lines = formatChangeLines(changes);
  const content = `🐋 ${emoji} **${walletLabel(wallet)} ${verb}**\n\n${tokenBlock}${lines}\nhttps://solscan.io/tx/${tx.signature}`;
  try{
    await fetch(env.DISCORD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
  }catch(e){ console.warn('Discord alert failed', e); }
}

// Second, independent alert channel — Telegram, alongside Discord (not a
// replacement). Uses the plain Bot API sendMessage endpoint: create a bot
// via @BotFather, get its token, then get your chat ID by messaging the
// bot once and checking https://api.telegram.org/bot<TOKEN>/getUpdates (or
// using a helper bot like @userinfobot for your personal chat ID).
//
// Sent with parse_mode: HTML so the CA renders as a <code> span — in the
// Telegram app that's a single tap to copy, not press-and-hold-to-select
// like plain text. Everything outside the CA/symbol is HTML-escaped since
// wallet labels are user-set text that could otherwise break the parser.
async function sendTelegramAlert(env, wallet, tx, changes, isSwap = true, context = {}){
  if(!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;
  const { label, emoji } = classifyEvent(changes, isSwap);
  const verb = actionVerb(label);
  const tokenBlock = buildTokenBlock(context, { format: 'telegram' });
  const lines = escapeTelegramHtml(formatChangeLines(changes));
  const walletLine = escapeTelegramHtml(walletLabel(wallet));
  const text = `🐋 ${emoji} <b>${walletLine} ${verb}</b>\n\n${tokenBlock}${lines}\nhttps://solscan.io/tx/${tx.signature}`;
  try{
    await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML' }),
    });
  }catch(e){ console.warn('Telegram alert failed', e); }
}

// Core poll loop: for each tracked wallet, fetch recent history, find
// transactions newer than the last one we've already processed, and alert
// on every one that actually moves a balance — genuine swaps (buy/sell)
// and plain transfers (sent/received) alike, each clearly labeled so a
// transfer is never mistaken for a trade.
async function pollWallets(env){
  if(!env.TRACKER_KV || !env.HELIUS_API_KEY) return;
  const wallets = await getWallets(env);
  if(!wallets.length) return;
  const trades = await getTrades(env);

  for(const wallet of wallets){
    const lastSeenSig = await env.TRACKER_KV.get(`lastseen:${wallet.address}`);
    const history = await fetchWalletHistory(wallet.address, env);
    if(!history.length) continue;

    const newTxs = [];
    for(const tx of history){
      if(tx.signature === lastSeenSig) break;
      newTxs.push(tx);
    }
    if(lastSeenSig && !wallet.paused){
      newTxs.reverse();
      for(const tx of newTxs){
        if(tx.transactionError) continue;

        const swapChanges = detectSwap(tx, wallet.address);
        if(swapChanges){
          const { label } = classifyEvent(swapChanges, true);
          const asset = primaryAsset(swapChanges, label);
          const [meta, price, marketCap] = await Promise.all([
            getTokenMeta(asset.mint, env),
            getTokenPriceUsd(asset.mint, env),
            getTokenMarketCap(asset.mint, env),
          ]);
          const usdValue = price != null ? Math.abs(asset.amount) * price : null;

          // Track entry MC across buy/sell pairs — only meaningful for
          // clean SOL<->token swaps (BUY/SELL), not token-to-token SWAPs
          // where "entry" isn't a single well-defined price.
          let entryMarketCap = null;
          if(label === 'BUY'){
            await setEntryMarketCap(env, wallet.address, asset.mint, marketCap, tx.timestamp);
          } else if(label === 'SELL'){
            const entry = await getEntryMarketCap(env, wallet.address, asset.mint);
            entryMarketCap = entry ? entry.mc : null;
          }

          const context = { asset, meta, usdValue, marketCap, entryMarketCap };

          trades.unshift({
            wallet: wallet.address, label: wallet.label || null,
            kind: 'swap', eventLabel: label,
            tokenMint: asset.mint, tokenSymbol: meta.symbol, tokenName: meta.name, usdValue, marketCap, entryMarketCap,
            signature: tx.signature, timestamp: tx.timestamp,
            changes: swapChanges.map(c=>({ mint: c.mint, amount: c.amount })),
          });
          await sendDiscordAlert(env, wallet, tx, swapChanges, true, context);
          await sendTelegramAlert(env, wallet, tx, swapChanges, true, context);
          continue;
        }

        // Not a swap — check if this tx is a plain transfer moving a
        // balance in or out. Only fires if something actually moved;
        // unrelated instructions (NFT mints, program calls, etc.) are
        // silently skipped, same as before.
        const transferChanges = walletChanges(tx, wallet.address);
        if(transferChanges.length){
          const { label } = classifyEvent(transferChanges, false);
          const asset = primaryAsset(transferChanges, label);
          const [meta, price, marketCap] = await Promise.all([
            getTokenMeta(asset.mint, env),
            getTokenPriceUsd(asset.mint, env),
            getTokenMarketCap(asset.mint, env),
          ]);
          const usdValue = price != null ? Math.abs(asset.amount) * price : null;
          const context = { asset, meta, usdValue, marketCap };

          trades.unshift({
            wallet: wallet.address, label: wallet.label || null,
            kind: 'transfer', eventLabel: label,
            tokenMint: asset.mint, tokenSymbol: meta.symbol, tokenName: meta.name, usdValue, marketCap,
            signature: tx.signature, timestamp: tx.timestamp,
            changes: transferChanges.map(c=>({ mint: c.mint, amount: c.amount })),
          });
          await sendDiscordAlert(env, wallet, tx, transferChanges, false, context);
          await sendTelegramAlert(env, wallet, tx, transferChanges, false, context);
        }
      }
    }

    await env.TRACKER_KV.put(`lastseen:${wallet.address}`, history[0].signature);
  }

  await saveTrades(env, trades);
}

async function handleApi(request, env){
  const url = new URL(request.url);

  if(url.pathname === '/api/status' && request.method === 'GET'){
    return json({
      kvConfigured: !!env.TRACKER_KV,
      heliusConfigured: !!env.HELIUS_API_KEY,
      discordConfigured: !!env.DISCORD_WEBHOOK_URL,
      telegramConfigured: !!(env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID),
    });
  }

  if(url.pathname === '/api/wallet-score' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const wallet = (url.searchParams.get('wallet') || '').trim();
    if(!isValidSolanaAddress(wallet)) return json({ error: 'Invalid wallet address.' }, 400);
    const depthParam = parseInt(url.searchParams.get('depth') || '100', 10);
    const depth = depthParam === 200 ? 200 : 100; // only these two are offered in the UI
    try{
      const data = await scoreWalletPerformance(wallet, env, depth);
      if(!data.totalTrades){
        return json({ ...data, note: 'No completed round-trip trades found in recent history — either this wallet mostly holds, or its buys/sells fall outside the scanned window.' });
      }
      return json(data);
    }catch(e){
      console.warn('wallet-score failed', e);
      return json({ error: 'Scoring failed. Double-check the wallet address and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/overlap-score' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const raw = (url.searchParams.get('mints') || '').trim();
    const mints = raw.split(',').map(s => s.trim()).filter(Boolean);
    if(mints.length < 2) return json({ error: 'Enter at least 2 token addresses to check for overlap.' }, 400);
    if(mints.length > 6) return json({ error: 'Max 6 tokens per scan — this keeps it from timing out.' }, 400);
    for(const m of mints){
      if(!isValidSolanaAddress(m)) return json({ error: `Invalid token address: ${m}` }, 400);
    }
    try{
      const data = await scoreTokenOverlap(mints, env);
      return json(data);
    }catch(e){
      console.warn('overlap-score failed', e);
      return json({ error: 'Scan failed. Double-check the addresses and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/early-buyers' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const mint = (url.searchParams.get('mint') || '').trim();
    if(!isValidSolanaAddress(mint)) return json({ error: 'Invalid token mint address.' }, 400);
    try{
      const data = await findEarlyBuyers(mint, env);
      if(!data.creationTimestamp) return json({ error: "Couldn't find this token's creation transaction." }, 404);
      return json(data);
    }catch(e){
      console.warn('early-buyers failed', e);
      return json({ error: 'Scan failed. Double-check the mint address and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/find-deployer' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const mint = (url.searchParams.get('mint') || '').trim();
    if(!isValidSolanaAddress(mint)) return json({ error: 'Invalid token mint address.' }, 400);
    try{
      const data = await findTokenDeployer(mint, env);
      if(!data.deployer) return json({ error: "Couldn't find a creation transaction for this mint." }, 404);
      return json(data);
    }catch(e){
      console.warn('find-deployer failed', e);
      return json({ error: 'Lookup failed. Double-check the mint address and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/trace-lineage' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const wallet = (url.searchParams.get('wallet') || '').trim();
    const target = (url.searchParams.get('target') || '').trim();
    const hopsParam = parseInt(url.searchParams.get('hops') || '2', 10);
    const maxDepth = Math.min(4, Math.max(1, isNaN(hopsParam) ? 2 : hopsParam));
    if(!isValidSolanaAddress(wallet)) return json({ error: 'Invalid wallet address.' }, 400);
    if(!isValidSolanaAddress(target)) return json({ error: 'Invalid target token address.' }, 400);
    try{
      const data = await traceLineageForBuyers(wallet, target, env, { maxDepth });
      return json(data);
    }catch(e){
      console.warn('trace-lineage failed', e);
      return json({ error: 'Trace failed. Double-check both addresses and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/cross-scan' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const sourceMint = (url.searchParams.get('source') || '').trim();
    const targetMint = (url.searchParams.get('target') || '').trim();
    if(!isValidSolanaAddress(sourceMint)) return json({ error: 'Invalid token A (source) address.' }, 400);
    if(!isValidSolanaAddress(targetMint)) return json({ error: 'Invalid token B (target) address.' }, 400);
    try{
      const data = await scanCrossBuyers(sourceMint, targetMint, env);
      return json(data);
    }catch(e){
      console.warn('cross-scan failed', e);
      return json({ error: 'Scan failed. Double-check both addresses and try again.' }, 500);
    }
  }

  if(url.pathname === '/api/scan-token' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet. Add it in Settings -> Variables and Secrets." }, 500);
    }
    const mint = (url.searchParams.get('mint') || '').trim();
    if(!isValidSolanaAddress(mint)) return json({ error: 'Invalid token mint address.' }, 400);
    try{
      const { holders, totalSupply } = await scanTokenHolders(mint, env);
      return json({ holders, totalSupply });
    }catch(e){
      console.warn('Token scan failed', e);
      return json({ error: 'Failed to scan this token. Double-check the mint address and try again.' }, 500);
    }
  }

  if(!env.TRACKER_KV){
    return json({ error: "TRACKER_KV binding isn't set up yet. Add a KV namespace binding named TRACKER_KV in this Worker's Settings -> Bindings." }, 500);
  }

  if(url.pathname === '/api/wallets' && request.method === 'GET'){
    return json({ wallets: await getWallets(env) });
  }

  if(url.pathname === '/api/wallets' && request.method === 'POST'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    const body = await request.json().catch(()=>({}));
    const address = (body.address||'').trim();
    const label = (body.label||'').trim().slice(0, 40);
    if(!isValidSolanaAddress(address)) return json({ error: 'Invalid Solana address.' }, 400);
    const wallets = await getWallets(env);
    if(wallets.some(w=>w.address===address)) return json({ error: 'Already tracked.' }, 400);
    if(wallets.length >= MAX_TRACKED_WALLETS){
      return json({ error: `Limit of ${MAX_TRACKED_WALLETS} tracked wallets reached. Remove one first.` }, 400);
    }
    wallets.unshift({ address, label: label || null, addedAt: Date.now() });
    await saveWallets(env, wallets);
    return json({ wallets });
  }

  if(url.pathname === '/api/wallets' && request.method === 'DELETE'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    const body = await request.json().catch(()=>({}));
    const address = (body.address||'').trim();
    let wallets = await getWallets(env);
    wallets = wallets.filter(w=>w.address!==address);
    await saveWallets(env, wallets);
    await env.TRACKER_KV.delete(`lastseen:${address}`);
    return json({ wallets });
  }

  if(url.pathname === '/api/wallets/pause' && request.method === 'POST'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    const body = await request.json().catch(()=>({}));
    const address = (body.address||'').trim();
    const paused = !!body.paused;
    const wallets = await getWallets(env);
    const wallet = wallets.find(w=>w.address===address);
    if(!wallet) return json({ error: 'Wallet not found in tracked list.' }, 404);
    wallet.paused = paused;
    await saveWallets(env, wallets);
    return json({ wallets });
  }

  if(url.pathname === '/api/leaderboard' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet." }, 500);
    }
    const depthParam = parseInt(url.searchParams.get('depth') || '100', 10);
    const depth = depthParam === 200 ? 200 : 100;
    try{
      const data = await scoreAllTrackedWallets(env, depth);
      return json(data);
    }catch(e){
      console.warn('leaderboard failed', e);
      return json({ error: 'Scoring failed. Try again.' }, 500);
    }
  }

  if(url.pathname === '/api/find-buyers' && request.method === 'GET'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    if(!env.HELIUS_API_KEY){
      return json({ error: "HELIUS_API_KEY isn't set yet." }, 500);
    }
    const mint = (url.searchParams.get('mint') || '').trim();
    if(!isValidSolanaAddress(mint)) return json({ error: 'Invalid token mint address.' }, 400);
    const wallets = await getWallets(env);
    if(!wallets.length){
      return json({ error: 'No tracked wallets yet — scan a token for whales and track a few first.' }, 400);
    }
    try{
      const data = await findMintBuyers(wallets, mint, env);
      return json(data);
    }catch(e){
      console.warn('find-buyers failed', e);
      return json({ error: 'Failed to scan tracked wallets. Try again.' }, 500);
    }
  }

  if(url.pathname === '/api/trades' && request.method === 'GET'){
    return json({ trades: await getTrades(env) });
  }

  if(url.pathname === '/api/poll-now' && request.method === 'POST'){
    if(!(await checkRateLimit(request, env))){
      return json({ error: 'Too many requests. Try again in a minute.' }, 429);
    }
    await pollWallets(env);
    return json({ ok: true });
  }

  return json({ error: 'Not found' }, 404);
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    if(request.method === 'OPTIONS'){
      return new Response('', { status: 200, headers: CORS_HEADERS });
    }
    if(url.pathname.startsWith('/api/')){
      return handleApi(request, env);
    }
    return env.ASSETS.fetch(request);
  },

  async scheduled(event, env, ctx){
    ctx.waitUntil(pollWallets(env));
  },
};
