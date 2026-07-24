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

const SOL_MINT = 'So11111111111111111111111111111111111111112';
const MAX_TRADES_STORED = 200;

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

async function fetchWalletHistory(address, env){
  const url = `https://api.helius.xyz/v1/wallet/${address}/history?api-key=${env.HELIUS_API_KEY}&limit=25`;
  const res = await fetch(url);
  if(!res.ok) return [];
  const data = await res.json();
  return data.data || [];
}

// Same heuristic used in ZEKE LEDGER: a swap moves at least two different
// assets in one transaction (e.g. -SOL, +TOKEN). Simple, reused on purpose
// rather than trusting an upstream "type" label.
function detectSwap(tx){
  const changes = (tx.balanceChanges || []).filter(c => Math.abs(c.amount) > 0);
  return changes.length >= 2 ? changes : null;
}

function walletLabel(wallet){
  const short = wallet.address.slice(0,4) + '…' + wallet.address.slice(-4);
  return wallet.label ? `${wallet.label} (${short})` : short;
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
        if(tx.error) continue;
        const changes = detectSwap(tx);
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

  if(!env.TRACKER_KV){
    return json({ error: "TRACKER_KV binding isn't set up yet. Add a KV namespace binding named TRACKER_KV in this Worker's Settings -> Bindings." }, 500);
  }

  if(url.pathname === '/api/wallets' && request.method === 'GET'){
    return json({ wallets: await getWallets(env) });
  }

  if(url.pathname === '/api/wallets' && request.method === 'POST'){
    const body = await request.json().catch(()=>({}));
    const address = (body.address||'').trim();
    const label = (body.label||'').trim().slice(0, 40);
    if(!isValidSolanaAddress(address)) return json({ error: 'Invalid Solana address.' }, 400);
    const wallets = await getWallets(env);
    if(wallets.some(w=>w.address===address)) return json({ error: 'Already tracked.' }, 400);
    wallets.unshift({ address, label: label || null, addedAt: Date.now() });
    await saveWallets(env, wallets);
    return json({ wallets });
  }

  if(url.pathname === '/api/wallets' && request.method === 'DELETE'){
    const body = await request.json().catch(()=>({}));
    const address = (body.address||'').trim();
    let wallets = await getWallets(env);
    wallets = wallets.filter(w=>w.address!==address);
    await saveWallets(env, wallets);
    await env.TRACKER_KV.delete(`lastseen:${address}`);
    return json({ wallets });
  }

  if(url.pathname === '/api/trades' && request.method === 'GET'){
    return json({ trades: await getTrades(env) });
  }

  if(url.pathname === '/api/poll-now' && request.method === 'POST'){
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
