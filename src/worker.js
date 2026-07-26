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
async function fetchAddressTransactions(address, env, { limit = 25, sortOrder } = {}){
  let url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?api-key=${env.HELIUS_API_KEY}&limit=${limit}`;
  if(sortOrder) url += `&sort-order=${sortOrder}`;
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

async function fetchWalletHistory(address, env, limit = 25){
  return fetchAddressTransactions(address, env, { limit });
}

// Reduces a parsed transaction's tokenTransfers + nativeTransfers down to
// this wallet's net change per mint (e.g. -1.2 SOL, +48000 MAD). Merges
// multiple transfers of the same mint within one tx into a single line.
function walletChanges(tx, address){
  const totals = {};
  for(const nt of tx.nativeTransfers || []){
    if(nt.fromUserAccount === address) totals[SOL_MINT] = (totals[SOL_MINT]||0) - nt.amount / 1e9;
    if(nt.toUserAccount === address) totals[SOL_MINT] = (totals[SOL_MINT]||0) + nt.amount / 1e9;
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
  if(!tx.type || tx.type === 'UNKNOWN') return changes.length >= 2 ? changes : null;
  return null;
}

function walletLabel(wallet){
  const short = wallet.address.slice(0,4) + '…' + wallet.address.slice(-4);
  return wallet.label ? `${wallet.label} (${short})` : short;
}

// Same Jupiter Price API v3 used elsewhere in the degen suite (Rekt or
// Rich). Free lite endpoint, no key needed. Returns null on any failure so
// a price hiccup degrades to "show token amounts only" instead of erroring
// out the whole scan.
async function getTokenPriceUsd(mint){
  try{
    const res = await fetch(`https://lite-api.jup.ag/price/v3?ids=${mint}`);
    if(!res.ok) return null;
    const data = await res.json();
    return data?.[mint]?.usdPrice ?? null;
  }catch(e){ return null; }
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
  const priceUsd = await getTokenPriceUsd(targetMint);
  const results = [];

  for(const wallet of wallets){
    const bought = await checkWalletBought(wallet.address, targetMint, env);
    if(bought){
      results.push({
        address: wallet.address,
        label: wallet.label || null,
        totalBought: bought.totalBought,
        valueUsd: priceUsd != null ? bought.totalBought * priceUsd : null,
        buyCount: bought.buyCount,
        lastBuyTimestamp: bought.lastBuyTimestamp,
      });
    }
  }

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

// Given a token mint, finds its top ~20 holder accounts and classifies each
// as a real wallet vs. a program-controlled account (LP pool, staking
// vault, PDA, etc.) so the UI can show actual whales instead of a list
// that's mostly Raydium/Pump.fun pool addresses.
//
// The classification trick: every ordinary wallet's on-chain account is
// owned by the System Program. Pool vaults, staking accounts, and other
// PDAs are owned by whatever program created them. Checking that one field
// is enough to tell "trader" from "infrastructure" without guessing.
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

  const results = [];
  for(const [address, info] of wallets){
    const behavior = await analyzeSniperBehavior(address, env);
    results.push({
      address,
      secondsAfterLaunch: creationTimestamp != null ? info.timestamp - creationTimestamp : null,
      amountBought: info.amount,
      ...behavior,
    });
  }

  return { creationTimestamp, results };
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

  const priceUsd = await getTokenPriceUsd(targetMint);
  const results = Object.values(foundBuyers)
    .map(r => ({ ...r, valueUsd: priceUsd != null ? r.totalBought * priceUsd : null }))
    .sort((a, b) => (b.valueUsd ?? b.totalBought) - (a.valueUsd ?? a.totalBought));

  return { results, scannedWallets: scanned, priceUsd, truncated: scanned >= maxTotalWallets };
}

// Labels a detected event for display/alerts. Swaps get BUY/SELL based on
// which way SOL moved (falling back to a generic SWAP for token-to-token
// trades). Plain transfers — not swaps, just a balance moving in or out —
// get their own SENT/RECEIVED label so they're never confused with an
// actual buy or sell.
function classifyEvent(changes, isSwap){
  if(isSwap){
    const solChange = changes.find(c => c.mint === SOL_MINT);
    if(!solChange) return { label: 'SWAP', emoji: '🔁' };
    return solChange.amount < 0 ? { label: 'BUY', emoji: '🟢' } : { label: 'SELL', emoji: '🔴' };
  }
  const primary = changes[0];
  if(!primary) return { label: 'TRANSFER', emoji: '🔁' };
  return primary.amount < 0 ? { label: 'SENT', emoji: '📤' } : { label: 'RECEIVED', emoji: '📥' };
}

function formatChangeLines(changes){
  return changes.map(c=>{
    const sym = (c.mint||'').trim() === SOL_MINT ? 'SOL' : (c.mint||'').slice(0,4)+'…';
    const amt = Math.abs(c.amount).toLocaleString(undefined,{maximumFractionDigits:4});
    return `${c.amount>0?'+':'-'}${amt} ${sym}`;
  }).join('  ·  ');
}

async function sendDiscordAlert(env, wallet, tx, changes, isSwap = true){
  if(!env.DISCORD_WEBHOOK_URL) return;
  const { label, emoji } = classifyEvent(changes, isSwap);
  const verb = isSwap ? `${label} detected` : (label === 'SENT' ? 'sent a transfer' : 'received a transfer');
  const lines = formatChangeLines(changes);
  const content = `🐋 **Whale ${verb}** ${emoji}\n**${walletLabel(wallet)}**\n${lines}\nhttps://solscan.io/tx/${tx.signature}`;
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
async function sendTelegramAlert(env, wallet, tx, changes, isSwap = true){
  if(!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID) return;
  const { label, emoji } = classifyEvent(changes, isSwap);
  const verb = isSwap ? `${label} detected` : (label === 'SENT' ? 'sent a transfer' : 'received a transfer');
  const lines = formatChangeLines(changes);
  const text = `${emoji} Whale ${verb}\n${walletLabel(wallet)}\n${lines}\nhttps://solscan.io/tx/${tx.signature}`;
  try{
    await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text }),
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
    if(lastSeenSig){
      newTxs.reverse();
      for(const tx of newTxs){
        if(tx.transactionError) continue;

        const swapChanges = detectSwap(tx, wallet.address);
        if(swapChanges){
          const { label } = classifyEvent(swapChanges, true);
          trades.unshift({
            wallet: wallet.address, label: wallet.label || null,
            kind: 'swap', eventLabel: label,
            signature: tx.signature, timestamp: tx.timestamp,
            changes: swapChanges.map(c=>({ mint: c.mint, amount: c.amount })),
          });
          await sendDiscordAlert(env, wallet, tx, swapChanges, true);
          await sendTelegramAlert(env, wallet, tx, swapChanges, true);
          continue;
        }

        // Not a swap — check if this tx is a plain transfer moving a
        // balance in or out. Only fires if something actually moved;
        // unrelated instructions (NFT mints, program calls, etc.) are
        // silently skipped, same as before.
        const transferChanges = walletChanges(tx, wallet.address);
        if(transferChanges.length){
          const { label } = classifyEvent(transferChanges, false);
          trades.unshift({
            wallet: wallet.address, label: wallet.label || null,
            kind: 'transfer', eventLabel: label,
            signature: tx.signature, timestamp: tx.timestamp,
            changes: transferChanges.map(c=>({ mint: c.mint, amount: c.amount })),
          });
          await sendDiscordAlert(env, wallet, tx, transferChanges, false);
          await sendTelegramAlert(env, wallet, tx, transferChanges, false);
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
