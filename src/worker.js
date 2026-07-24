// Whale Copytrade Tracker — Cloudflare Worker
//
// Serves the dashboard, manages a tracked-wallet list in KV, and runs on a
// schedule (every 5 minutes, set in wrangler.jsonc) to poll each tracked
// wallet's recent activity via Helius, detect swaps, log them for the
// dashboard feed, and push a Discord alert for each new one.
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

// Lightweight per-IP rate limit for the wallet-add/remove endpoints, since
// those are the only ones that mutate state or could be spammed. Uses a
// single KV counter per IP per time window rather than a sliding log, since
// this only needs to stop abuse, not be precise.
async function checkRateLimit(request, env){
  if(!env.TRACKER_KV) return true; // fail open if KV isn't set up yet
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
// wallet/API hiccup doesn't kill the whole poll cycle for other wallets.
async function fetchWalletHistory(address, env, limit = 25){
  const url = `https://api-mainnet.helius-rpc.com/v0/addresses/${address}/transactions?api-key=${env.HELIUS_API_KEY}&limit=${limit}`;
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

// For each tracked wallet, pulls its recent history and checks whether any
// genuine swap (reusing the exact same detectSwap logic the poller uses,
// so "bought" means the same thing everywhere in this app) received the
// target mint. Deliberately does NOT count plain transfers/airdrops of the
// token as a "buy" — only actual swaps count.
//
// Scope limitation worth knowing: this only looks at each wallet's most
// recent transactions (default 50), not full history, so an old buy well
// outside that window won't surface. Good for "did they buy this recently /
// are they currently accumulating," not a lifetime audit.
async function findMintBuyers(wallets, targetMint, env){
  const priceUsd = await getTokenPriceUsd(targetMint);
  const results = [];

  for(const wallet of wallets){
    const history = await fetchWalletHistory(wallet.address, env, 50);
    let totalBought = 0;
    let buyCount = 0;
    let lastBuyTimestamp = null;

    for(const tx of history){
      if(tx.transactionError) continue;
      const changes = detectSwap(tx, wallet.address);
      if(!changes) continue;
      const bought = changes.find(c => c.mint === targetMint && c.amount > 0);
      if(bought){
        totalBought += bought.amount;
        buyCount += 1;
        if(!lastBuyTimestamp || tx.timestamp > lastBuyTimestamp) lastBuyTimestamp = tx.timestamp;
      }
    }

    if(totalBought > 0){
      results.push({
        address: wallet.address,
        label: wallet.label || null,
        totalBought,
        valueUsd: priceUsd != null ? totalBought * priceUsd : null,
        buyCount,
        lastBuyTimestamp,
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
async function scanTokenHolders(mint, env){
  const largest = await rpcCall(env, 'getTokenLargestAccounts', [mint]);
  const tokenAccounts = (largest?.value || []).filter(a => Number(a.amount) > 0);
  if(!tokenAccounts.length) return { holders: [], totalSupply: 0 };

  // Resolve each top token account to the wallet/PDA that actually owns it.
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

  // One batched call to check what kind of account each unique owner is.
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

async function sendDiscordAlert(env, wallet, tx, changes){
  if(!env.DISCORD_WEBHOOK_URL) return;
  const lines = changes.map(c=>{
    const sym = (c.mint||'').trim() === SOL_MINT ? 'SOL' : (c.mint||'').slice(0,4)+'…';
    const amt = Math.abs(c.amount).toLocaleString(undefined,{maximumFractionDigits:4});
    return `${c.amount>0?'+':'-'}${amt} ${sym}`;
  }).join('  ·  ');
  const content = `🐋 **Whale trade detected**\n**${walletLabel(wallet)}**\n${lines}\nhttps://solscan.io/tx/${tx.signature}`;
  try{
    await fetch(env.DISCORD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
  }catch(e){ console.warn('Discord alert failed', e); }
}

// Core poll loop: for each tracked wallet, fetch recent history, find
// transactions newer than the last one we've already processed, detect
// swaps among them, log to the trades feed, and alert Discord for each.
async function pollWallets(env){
  if(!env.TRACKER_KV || !env.HELIUS_API_KEY) return;
  const wallets = await getWallets(env);
  if(!wallets.length) return;
  const trades = await getTrades(env);

  for(const wallet of wallets){
    const lastSeenSig = await env.TRACKER_KV.get(`lastseen:${wallet.address}`);
    const history = await fetchWalletHistory(wallet.address, env);
    if(!history.length) continue;

    // History is newest-first. Walk it until we hit the signature we
    // processed last time, collecting only genuinely new transactions.
    const newTxs = [];
    for(const tx of history){
      if(tx.signature === lastSeenSig) break;
      newTxs.push(tx);
    }
    // First-ever poll of a wallet: don't dump its entire recent history as
    // "new" trades/alerts — just establish a starting point silently.
    if(lastSeenSig){
      newTxs.reverse(); // oldest-first, so log/alerts land in chronological order
      for(const tx of newTxs){
        if(tx.transactionError) continue;
        const changes = detectSwap(tx, wallet.address);
        if(!changes) continue;
        trades.unshift({
          wallet: wallet.address,
          label: wallet.label || null,
          signature: tx.signature,
          timestamp: tx.timestamp,
          changes: changes.map(c=>({ mint: c.mint, amount: c.amount })),
        });
        await sendDiscordAlert(env, wallet, tx, changes);
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
    });
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
    // Manual trigger, mainly for testing without waiting up to 5 minutes
    // for the next scheduled run.
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
