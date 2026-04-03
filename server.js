// TexTrack — Backend with Stripe + WebSocket
// Uses Turso for GPS history, Stripe for billing, WebSocket for live tracking
// FIXES APPLIED: heartbeat/dead socket reaping, token cache invalidation,
//   transfer duplicate fix, server-side VIN validation, ping rate config,
//   history pagination, recovery mode, admin email on recovery

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const { createClient } = require('@libsql/client');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

const PORT = process.env.PORT || 8080;
const HISTORY_DAYS = 60;

// ── REQUIRED ENV CHECK ────────────────────────────────────────
if (!process.env.ADMIN_KEY) {
  console.error('FATAL: ADMIN_KEY environment variable is required');
  process.exit(1);
}
const ADMIN_KEY = process.env.ADMIN_KEY;

// ── TURSO DATABASE ────────────────────────────────────────────
let db = null;

async function initDB() {
  if (!process.env.TURSO_URL || !process.env.TURSO_TOKEN) {
    console.warn('[db] TURSO_URL or TURSO_TOKEN not set — history disabled');
    return;
  }
  try {
    db = createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_TOKEN });
    await db.batch([
      {
        sql: `CREATE TABLE IF NOT EXISTS pings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT NOT NULL, ts INTEGER NOT NULL,
          lat REAL NOT NULL, lon REAL NOT NULL,
          mph REAL, acc REAL, moving INTEGER, voltage REAL
        )`, args: []
      },
      { sql: `CREATE INDEX IF NOT EXISTS idx_token_ts ON pings(token, ts)`, args: [] },
      {
        sql: `CREATE TABLE IF NOT EXISTS signatures (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          full_name TEXT NOT NULL,
          email TEXT NOT NULL,
          phone TEXT,
          state TEXT,
          zip TEXT,
          vin TEXT NOT NULL,
          year TEXT, make TEXT, model TEXT, color TEXT, plate TEXT,
          sig_name TEXT NOT NULL,
          sig_timestamp TEXT NOT NULL,
          sig_timezone TEXT,
          tos_version TEXT NOT NULL,
          chk_arbitration INTEGER, chk_class_waiver INTEGER,
          chk_liability_caps INTEGER, chk_audio INTEGER,
          chk_electrical INTEGER, chk_ownership INTEGER,
          chk_installer_independence INTEGER, chk_full_agreement INTEGER,
          installer_id TEXT,
          installer_name TEXT,
          ip_address TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS vehicles (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          vin TEXT UNIQUE NOT NULL,
          make TEXT, model TEXT, year TEXT, color TEXT,
          plate TEXT, plate_state TEXT,
          owner_name TEXT, owner_email TEXT, owner_phone TEXT,
          installer_id TEXT, installer_name TEXT,
          stripe_customer_id TEXT, stripe_subscription_id TEXT,
          token TEXT UNIQUE NOT NULL,
          plan TEXT DEFAULT 'single',
          status TEXT DEFAULT 'active',
          install_date INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS installers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE NOT NULL,
          phone TEXT,
          stripe_account_id TEXT UNIQUE,
          stripe_payment_link TEXT,
          status TEXT DEFAULT 'active',
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS transfers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT UNIQUE NOT NULL,
          token TEXT NOT NULL,
          old_customer_id TEXT,
          old_owner_email TEXT,
          new_owner_name TEXT,
          new_owner_email TEXT,
          vin TEXT, make TEXT, model TEXT, year TEXT, color TEXT, plate TEXT,
          used INTEGER DEFAULT 0,
          new_customer_id TEXT,
          expires_at INTEGER,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS magic_tokens (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT UNIQUE NOT NULL,
          email TEXT NOT NULL,
          expires_at INTEGER NOT NULL,
          used INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS reactivations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT UNIQUE NOT NULL,
          token TEXT NOT NULL,
          stripe_customer_id TEXT,
          owner_email TEXT,
          vin TEXT, make TEXT, model TEXT, year TEXT, color TEXT, plate TEXT,
          expires_at INTEGER,
          used INTEGER DEFAULT 0,
          new_customer_id TEXT,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS recovery_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT NOT NULL,
          share_code TEXT UNIQUE NOT NULL,
          active INTEGER DEFAULT 1,
          expires_at INTEGER NOT NULL,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      }
    ], 'write');
    console.log('[db] Turso connected and ready');
    pruneHistory();
  } catch(e) {
    console.error('[db] Init error:', e.message);
    db = null;
  }
}

// ── HISTORY MANAGEMENT ────────────────────────────────────────
async function pruneHistory() {
  if (!db) return;
  const cutoff = Date.now() - (HISTORY_DAYS * 24 * 60 * 60 * 1000);
  try {
    const r = await db.execute({ sql: 'DELETE FROM pings WHERE ts < ?', args: [cutoff] });
    if (r.rowsAffected > 0) console.log(`[db] Pruned ${r.rowsAffected} old pings`);
  } catch(e) { console.error('[db] Prune error:', e.message); }
}
setInterval(pruneHistory, 60 * 60 * 1000);

async function getHistory(token, days, page, pageSize) {
  if (!db) return { pings: [], total: 0 };
  const since = Date.now() - (days * 24 * 60 * 60 * 1000);
  const offset = (page - 1) * pageSize;
  try {
    const countResult = await db.execute({
      sql: 'SELECT COUNT(*) as cnt FROM pings WHERE token = ? AND ts >= ?',
      args: [token, since]
    });
    const total = countResult.rows[0]?.cnt || 0;

    const r = await db.execute({
      sql: 'SELECT ts, lat, lon, mph, acc, moving, voltage FROM pings WHERE token = ? AND ts >= ? ORDER BY ts ASC LIMIT ? OFFSET ?',
      args: [token, since, pageSize, offset]
    });
    return { pings: r.rows, total };
  } catch(e) { return { pings: [], total: 0 }; }
}

// ── PING BATCHING ─────────────────────────────────────────────
// Accumulate pings in memory, flush every 30 seconds to reduce DB writes
const pingBuffer = [];
const FLUSH_INTERVAL = 30000;

function bufferPing(token, msg) {
  pingBuffer.push({
    token, ts: msg.ts || Date.now(), lat: msg.lat, lon: msg.lon,
    mph: msg.mph || 0, acc: msg.acc || 0, moving: msg.moving ? 1 : 0,
    voltage: msg.voltage || null
  });
}

async function flushPings() {
  if (!db || pingBuffer.length === 0) return;
  const batch = pingBuffer.splice(0, pingBuffer.length);
  try {
    const stmts = batch.map(p => ({
      sql: 'INSERT INTO pings (token, ts, lat, lon, mph, acc, moving, voltage) VALUES (?,?,?,?,?,?,?,?)',
      args: [p.token, p.ts, p.lat, p.lon, p.mph, p.acc, p.moving, p.voltage]
    }));
    await db.batch(stmts, 'write');
  } catch(e) {
    console.error('[db] Batch insert error:', e.message);
    // Put failed pings back for retry
    pingBuffer.unshift(...batch);
  }
}
setInterval(flushPings, FLUSH_INTERVAL);

// ── VEHICLE HELPERS ───────────────────────────────────────────
async function getVehicleByToken(token) {
  if (!db) return null;
  try {
    const r = await db.execute({ sql: 'SELECT * FROM vehicles WHERE token = ?', args: [token] });
    return r.rows[0] || null;
  } catch(e) { return null; }
}

async function getAllVehicles() {
  if (!db) return [];
  try {
    const r = await db.execute({ sql: 'SELECT * FROM vehicles ORDER BY created_at DESC', args: [] });
    return r.rows;
  } catch(e) { return []; }
}

async function getAllInstallers() {
  if (!db) return [];
  try {
    const r = await db.execute({ sql: 'SELECT * FROM installers ORDER BY name ASC', args: [] });
    return r.rows;
  } catch(e) { return []; }
}

async function createVehicle(data) {
  if (!db) return null;
  const token = 'tt3k-' + crypto.randomBytes(16).toString('hex');
  try {
    await db.execute({
      sql: `INSERT INTO vehicles (vin, make, model, year, color, plate, plate_state,
            owner_name, owner_email, owner_phone, installer_id, installer_name,
            stripe_customer_id, stripe_subscription_id, token, plan, install_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      args: [
        data.vin, data.make, data.model, data.year, data.color,
        data.plate, data.plate_state, data.owner_name, data.owner_email,
        data.owner_phone, data.installer_id, data.installer_name,
        data.stripe_customer_id, data.stripe_subscription_id,
        token, data.plan || 'single', Date.now()
      ]
    });
    // Add to valid token cache immediately
    validTokenCache.set(token, Date.now());
    return token;
  } catch(e) { console.error('[db] Create vehicle error:', e.message); return null; }
}

// ── ADMIN AUTH ────────────────────────────────────────────────
function isAdmin(req) {
  const key = req.headers['x-admin-key'] || new URL(req.url, 'http://localhost').searchParams.get('key');
  return key === ADMIN_KEY;
}

// ── TOKEN CACHE WITH TTL ──────────────────────────────────────
// Cache tokens with timestamps, rebuild from DB every 5 minutes
const validTokenCache = new Map();
const TOKEN_CACHE_TTL = 5 * 60 * 1000;

// Add personal test token if configured
if (process.env.PERSONAL_TEST_TOKEN) {
  validTokenCache.set(process.env.PERSONAL_TEST_TOKEN, Date.now());
}

async function isValidToken(token) {
  if (!token) return false;

  // Check cache — if present and fresh, trust it
  const cached = validTokenCache.get(token);
  if (cached && (Date.now() - cached) < TOKEN_CACHE_TTL) return true;

  // Check DB
  const vehicle = await getVehicleByToken(token);
  if (vehicle && vehicle.status === 'active') {
    validTokenCache.set(token, Date.now());
    return true;
  }

  // Token is invalid or cancelled — remove from cache
  validTokenCache.delete(token);
  return false;
}

// Rebuild cache from DB every 5 minutes (handles cancellations)
async function rebuildTokenCache() {
  if (!db) return;
  try {
    const r = await db.execute({
      sql: "SELECT token FROM vehicles WHERE status = 'active'",
      args: []
    });
    const activeTokens = new Set(r.rows.map(row => row.token));
    // Remove cancelled tokens
    for (const [token] of validTokenCache) {
      if (token === process.env.PERSONAL_TEST_TOKEN) continue;
      if (!activeTokens.has(token)) {
        validTokenCache.delete(token);
        // Also disconnect the device if connected
        const room = rooms.get(token);
        if (room && room.device) {
          safeSend(room.device, { type: 'token_revoked' });
          room.device.terminate();
        }
      }
    }
    // Add any missing active tokens
    for (const row of r.rows) {
      if (!validTokenCache.has(row.token)) {
        validTokenCache.set(row.token, Date.now());
      }
    }
  } catch(e) { console.error('[cache] Rebuild error:', e.message); }
}
setInterval(rebuildTokenCache, TOKEN_CACHE_TTL);

// ── VIN VALIDATION ────────────────────────────────────────────
function isValidVIN(vin) {
  if (!vin || typeof vin !== 'string') return false;
  const cleaned = vin.trim().toUpperCase();
  return /^[A-HJ-NPR-Z0-9]{17}$/.test(cleaned);
}

// ── ROOMS ─────────────────────────────────────────────────────
const rooms = new Map();
function getRoom(token) {
  if (!rooms.has(token)) rooms.set(token, { device: null, controls: [], lastLocation: null, recoveryMode: false });
  return rooms.get(token);
}

// ── SIMPLE EMAIL HELPER ───────────────────────────────────────
async function sendEmail(to, subject, html) {
  if (!process.env.RESEND_API_KEY) return false;
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${process.env.RESEND_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: process.env.FROM_EMAIL || 'TexTrack <noreply@textrack.com>',
        to, subject, html
      })
    });
    return res.ok;
  } catch(e) { console.error('[email] Send error:', e.message); return false; }
}

// ── HTTP SERVER ───────────────────────────────────────────────
const httpServer = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const cors = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, x-admin-key',
    'Content-Type': 'application/json'
  };

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); res.end(); return; }

  // ── STRIPE WEBHOOK ─────────────────────────────────────────
  if (url.pathname === '/webhook' && req.method === 'POST') {
    let body = '';
    let bodyLength = 0;
    const MAX_BODY = 1024 * 1024; // 1MB max

    req.on('data', chunk => {
      bodyLength += chunk.length;
      if (bodyLength > MAX_BODY) { req.destroy(); return; }
      body += chunk;
    });
    req.on('end', async () => {
      if (bodyLength > MAX_BODY) {
        res.writeHead(413, { 'Content-Type': 'text/plain' });
        res.end('Payload too large');
        return;
      }
      const sig = req.headers['stripe-signature'];
      if (!sig) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Missing stripe-signature header');
        return;
      }
      let event;
      try {
        event = stripe.webhooks.constructEvent(body, sig, process.env.STRIPE_WEBHOOK_SECRET);
      } catch(e) {
        console.error('[stripe] Webhook verification failed:', e.message);
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Webhook error: ' + e.message);
        return;
      }

      console.log('[stripe] webhook:', event.type);

      if (event.type === 'checkout.session.completed') {
        const session = event.data.object;
        const fields = {};
        (session.custom_fields || []).forEach(f => {
          fields[f.key] = f.text?.value || f.dropdown?.value || '';
        });

        // Use stripe_account_id consistently
        const installerStripeId = session.metadata?.installer_id;
        const installerName = session.metadata?.installer_name;

        // Check if this is a transfer
        const transferCode = session.metadata?.transfer_code;
        if (transferCode && db) {
          try {
            const tr = await db.execute({
              sql: 'SELECT * FROM transfers WHERE code = ? AND used = 0',
              args: [transferCode]
            });
            const transfer = tr.rows[0];
            if (transfer) {
              // Update existing vehicle record for new owner (not INSERT duplicate)
              await db.execute({
                sql: `UPDATE vehicles SET
                  owner_name = ?, owner_email = ?, owner_phone = ?,
                  stripe_customer_id = ?, stripe_subscription_id = ?,
                  status = 'active', install_date = ?
                  WHERE token = ?`,
                args: [
                  session.customer_details?.name || transfer.new_owner_name || '',
                  session.customer_details?.email || transfer.new_owner_email || '',
                  session.customer_details?.phone || '',
                  session.customer,
                  session.subscription,
                  Date.now(),
                  transfer.token
                ]
              });
              // Mark transfer as used
              await db.execute({
                sql: 'UPDATE transfers SET used = 1, new_customer_id = ? WHERE code = ?',
                args: [session.customer, transferCode]
              });
              // Re-add token to cache
              validTokenCache.set(transfer.token, Date.now());
              console.log('[stripe] Transfer completed for token:', transfer.token);
              res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok');
              return;
            }
          } catch(e) { console.error('[stripe] Transfer processing error:', e.message); }
        }

        // Check if this is a reactivation
        const reactivateCode = session.metadata?.reactivate_code;
        if (reactivateCode && db) {
          try {
            const rr = await db.execute({
              sql: 'SELECT * FROM reactivations WHERE code = ? AND used = 0',
              args: [reactivateCode]
            });
            const reactivation = rr.rows[0];
            if (reactivation) {
              await db.execute({
                sql: `UPDATE vehicles SET status = 'active', stripe_customer_id = ?,
                      stripe_subscription_id = ? WHERE token = ?`,
                args: [session.customer, session.subscription, reactivation.token]
              });
              await db.execute({
                sql: 'UPDATE reactivations SET used = 1, new_customer_id = ? WHERE code = ?',
                args: [session.customer, reactivateCode]
              });
              validTokenCache.set(reactivation.token, Date.now());
              console.log('[stripe] Reactivation completed for token:', reactivation.token);
              res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok');
              return;
            }
          } catch(e) { console.error('[stripe] Reactivation error:', e.message); }
        }

        // Normal new vehicle registration
        const existingSubs = await stripe.subscriptions.list({ customer: session.customer, limit: 10 });
        const plan = existingSubs.data.length > 1 ? 'multi' : 'single';

        const token = await createVehicle({
          vin: fields.vin || '',
          make: fields.make || '',
          model: fields.model || '',
          year: fields.year || '',
          color: fields.color || '',
          plate: fields.plate || '',
          plate_state: fields.plate_state || '',
          owner_name: session.customer_details?.name || '',
          owner_email: session.customer_details?.email || '',
          owner_phone: session.customer_details?.phone || '',
          installer_id: installerStripeId,
          installer_name: installerName,
          stripe_customer_id: session.customer,
          stripe_subscription_id: session.subscription,
          plan
        });

        console.log('[stripe] New vehicle registered, token:', token);

        // Route $18 to installer using Stripe account ID
        if (installerStripeId && token) {
          try {
            await stripe.transfers.create({
              amount: 1800,
              currency: 'usd',
              destination: installerStripeId,
              description: `Install fee for vehicle ${fields.vin}`
            });
            console.log('[stripe] Install fee transferred to installer');
          } catch(e) { console.error('[stripe] Transfer error:', e.message); }
        }
      }

      if (event.type === 'invoice.payment_succeeded') {
        const invoice = event.data.object;
        if (invoice.subscription && db) {
          try {
            // JOIN to get installer's Stripe account ID properly
            const r = await db.execute({
              sql: `SELECT v.*, i.stripe_account_id as inst_stripe_id
                    FROM vehicles v
                    LEFT JOIN installers i ON v.installer_id = i.stripe_account_id
                    WHERE v.stripe_subscription_id = ?`,
              args: [invoice.subscription]
            });
            const vehicle = r.rows[0];
            if (vehicle && vehicle.inst_stripe_id) {
              await stripe.transfers.create({
                amount: 500,
                currency: 'usd',
                destination: vehicle.inst_stripe_id,
                description: `Monthly commission for ${vehicle.vin}`
              });
              console.log('[stripe] Monthly commission transferred for', vehicle.vin);
            }
          } catch(e) { console.error('[stripe] Monthly transfer error:', e.message); }
        }
      }

      if (event.type === 'customer.subscription.deleted') {
        const sub = event.data.object;
        if (db) {
          try {
            const r = await db.execute({
              sql: 'SELECT token FROM vehicles WHERE stripe_subscription_id = ?',
              args: [sub.id]
            });
            const vehicle = r.rows[0];
            await db.execute({
              sql: "UPDATE vehicles SET status = 'cancelled' WHERE stripe_subscription_id = ?",
              args: [sub.id]
            });
            // Invalidate token cache immediately
            if (vehicle) {
              validTokenCache.delete(vehicle.token);
            }
            console.log('[stripe] Subscription cancelled for', sub.id);
          } catch(e) { console.error('[stripe] Cancel error:', e.message); }
        }
      }

      res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok');
    });
    return;
  }

  // ── HISTORY API (with pagination) ───────────────────────────
  if (url.pathname === '/history' && req.method === 'GET') {
    const token = url.searchParams.get('token');
    const days = Math.min(Math.max(parseInt(url.searchParams.get('days')) || 7, 1), HISTORY_DAYS);
    const page = Math.max(parseInt(url.searchParams.get('page')) || 1, 1);
    const pageSize = Math.min(Math.max(parseInt(url.searchParams.get('limit')) || 500, 10), 2000);

    if (!token || !(await isValidToken(token))) {
      res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Invalid token' })); return;
    }
    const { pings, total } = await getHistory(token, days, page, pageSize);
    res.writeHead(200, cors);
    res.end(JSON.stringify({ pings, days, count: pings.length, total, page, pageSize }));
    return;
  }

  // ── RECOVERY MODE: START ────────────────────────────────────
  if (url.pathname === '/recovery/start' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        if (!data.token || !(await isValidToken(data.token))) {
          res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Invalid token' })); return;
        }
        const shareCode = crypto.randomBytes(8).toString('hex');
        const expiresAt = Date.now() + (24 * 60 * 60 * 1000); // 24 hours

        if (db) {
          // Deactivate any existing recovery session for this token
          await db.execute({
            sql: 'UPDATE recovery_sessions SET active = 0 WHERE token = ? AND active = 1',
            args: [data.token]
          });
          await db.execute({
            sql: 'INSERT INTO recovery_sessions (token, share_code, expires_at) VALUES (?,?,?)',
            args: [data.token, shareCode, expiresAt]
          });
        }

        // Set room to recovery mode
        const room = getRoom(data.token);
        room.recoveryMode = true;

        // Tell device to switch to recovery ping rate
        if (room.device && room.device.readyState === 1) {
          safeSend(room.device, { type: 'recovery_mode', active: true });
        }

        const siteUrl = process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro';
        const shareUrl = `${siteUrl}/recovery-live.html?code=${shareCode}`;

        // Email admin
        const vehicle = await getVehicleByToken(data.token);
        const adminEmail = process.env.ADMIN_EMAIL;
        if (adminEmail) {
          sendEmail(adminEmail, 'TexTrack Recovery Mode Activated', `
            <div style="font-family:sans-serif;background:#111;color:#ccc;padding:24px;">
              <h2 style="color:#e09020;">Recovery Mode Activated</h2>
              <p>Vehicle: ${vehicle?.year || ''} ${vehicle?.make || ''} ${vehicle?.model || ''}</p>
              <p>VIN: ${vehicle?.vin || 'unknown'}</p>
              <p>Owner: ${vehicle?.owner_name || ''} (${vehicle?.owner_email || ''})</p>
              <p>Share link: <a href="${shareUrl}" style="color:#e09020;">${shareUrl}</a></p>
              <p>Expires: ${new Date(expiresAt).toLocaleString()}</p>
            </div>
          `);
        }

        console.log('[recovery] Started for token:', data.token, 'share:', shareCode);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, share_code: shareCode, share_url: shareUrl, expires_at: expiresAt }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── RECOVERY MODE: STOP ─────────────────────────────────────
  if (url.pathname === '/recovery/stop' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        if (!data.token) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Token required' })); return; }

        if (db) {
          await db.execute({
            sql: 'UPDATE recovery_sessions SET active = 0 WHERE token = ? AND active = 1',
            args: [data.token]
          });
        }
        const room = getRoom(data.token);
        room.recoveryMode = false;
        if (room.device && room.device.readyState === 1) {
          safeSend(room.device, { type: 'recovery_mode', active: false });
        }
        console.log('[recovery] Stopped for token:', data.token);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── RECOVERY MODE: LIVE LOCATION (public, by share code) ───
  if (url.pathname === '/recovery/live' && req.method === 'GET') {
    const code = url.searchParams.get('code');
    if (!code) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Code required' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const r = await db.execute({
        sql: 'SELECT * FROM recovery_sessions WHERE share_code = ? AND active = 1 AND expires_at > ?',
        args: [code, Date.now()]
      });
      const session = r.rows[0];
      if (!session) {
        res.writeHead(404, cors);
        res.end(JSON.stringify({ error: 'Recovery session not found or expired' }));
        return;
      }
      const room = rooms.get(session.token);
      const loc = room?.lastLocation || null;
      res.writeHead(200, cors);
      res.end(JSON.stringify({
        active: true,
        location: loc ? { lat: loc.lat, lon: loc.lon, mph: loc.mph, acc: loc.acc, ts: loc.ts } : null
      }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── ADMIN — GET ALL VEHICLES ─────────────────────────────────
  if (url.pathname === '/admin/vehicles' && req.method === 'GET') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    const vehicles = await getAllVehicles();
    // Annotate with recovery mode status
    const annotated = vehicles.map(v => {
      const room = rooms.get(v.token);
      return { ...v, recovery_mode: room?.recoveryMode || false, online: !!(room?.device) };
    });
    res.writeHead(200, cors); res.end(JSON.stringify(annotated)); return;
  }

  // ── ADMIN — GET ALL INSTALLERS ───────────────────────────────
  if (url.pathname === '/admin/installers' && req.method === 'GET') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    const installers = await getAllInstallers();
    res.writeHead(200, cors); res.end(JSON.stringify(installers)); return;
  }

  // ── ADMIN — ADD INSTALLER ────────────────────────────────────
  if (url.pathname === '/admin/installers' && req.method === 'POST') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const account = await stripe.accounts.create({
          type: 'express',
          email: data.email,
          capabilities: { transfers: { requested: true } },
          business_type: 'individual',
          metadata: { name: data.name, phone: data.phone || '' }
        });
        const siteUrl = process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro';
        const link = await stripe.accountLinks.create({
          account: account.id,
          refresh_url: siteUrl + '/admin.html',
          return_url: siteUrl + '/admin.html',
          type: 'account_onboarding'
        });
        if (db) {
          await db.execute({
            sql: 'INSERT INTO installers (name, email, phone, stripe_account_id) VALUES (?,?,?,?)',
            args: [data.name, data.email, data.phone || '', account.id]
          });
        }
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, stripe_account_id: account.id, onboarding_url: link.url }));
      } catch(e) {
        res.writeHead(400, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── ADMIN — CREATE PAYMENT LINK FOR INSTALLER ────────────────
  if (url.pathname === '/admin/payment-link' && req.method === 'POST') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const siteUrl = process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro';
        const link = await stripe.paymentLinks.create({
          line_items: [
            { price: process.env.STRIPE_INSTALL_PRICE_ID, quantity: 1 },
            { price: process.env.STRIPE_SINGLE_PRICE_ID, quantity: 1 }
          ],
          custom_fields: [
            { key: 'vin', label: { type: 'custom', custom: 'Vehicle VIN (17 characters)' }, type: 'text', optional: false },
            { key: 'make', label: { type: 'custom', custom: 'Make (e.g. Toyota)' }, type: 'text', optional: false },
            { key: 'model', label: { type: 'custom', custom: 'Model (e.g. Camry)' }, type: 'text', optional: false },
            { key: 'year', label: { type: 'custom', custom: 'Year (e.g. 2021)' }, type: 'text', optional: false },
            { key: 'color', label: { type: 'custom', custom: 'Vehicle Color' }, type: 'text', optional: false },
            { key: 'plate', label: { type: 'custom', custom: 'License Plate + State' }, type: 'text', optional: false }
          ],
          phone_number_collection: { enabled: true },
          metadata: { installer_id: data.stripe_account_id, installer_name: data.name },
          after_completion: { type: 'redirect', redirect: { url: siteUrl + '/welcome.html' } }
        });

        // SAVE payment link back to installers table
        if (db) {
          await db.execute({
            sql: 'UPDATE installers SET stripe_payment_link = ? WHERE stripe_account_id = ?',
            args: [link.url, data.stripe_account_id]
          });
        }

        res.writeHead(200, cors);
        res.end(JSON.stringify({ url: link.url }));
      } catch(e) {
        res.writeHead(400, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── ADMIN — P&L SUMMARY ──────────────────────────────────────
  if (url.pathname === '/admin/pnl' && req.method === 'GET') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    try {
      const vehicles = await getAllVehicles();
      const active = vehicles.filter(v => v.status === 'active');
      const single = active.filter(v => v.plan === 'single');
      const multi = active.filter(v => v.plan === 'multi');
      const monthlyRevenue = (single.length * 22) + (multi.length * 19);
      const installerCommissions = active.length * 5;
      const simCosts = active.length * 10;
      const stripeFees = (single.length * 0.94) + (multi.length * 0.85) + (active.length * 0.26);
      const renderCost = 7;
      const netMonthly = monthlyRevenue - installerCommissions - simCosts - stripeFees - renderCost;
      res.writeHead(200, cors);
      res.end(JSON.stringify({
        active_vehicles: active.length,
        single_plan: single.length,
        multi_plan: multi.length,
        monthly_revenue: monthlyRevenue.toFixed(2),
        installer_commissions: installerCommissions.toFixed(2),
        sim_costs: simCosts.toFixed(2),
        stripe_fees: stripeFees.toFixed(2),
        render_cost: renderCost.toFixed(2),
        net_monthly: netMonthly.toFixed(2)
      }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── SIGN — Record TOS signature ──────────────────────────────
  if (url.pathname === '/sign' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      let data;
      try { data = JSON.parse(body); } catch(e) {
        res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Invalid JSON' })); return;
      }

      const required = ['full_name','email','vin','sig_name','sig_timestamp','tos_version'];
      const missing = required.filter(f => !data[f]);
      if (missing.length) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: 'Missing fields: ' + missing.join(', ') }));
        return;
      }

      // Server-side VIN validation
      if (!isValidVIN(data.vin)) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: 'Invalid VIN — must be exactly 17 alphanumeric characters (no I, O, or Q)' }));
        return;
      }

      if (data.sig_name.trim().toLowerCase() !== data.full_name.trim().toLowerCase()) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: 'Signature name does not match full name' }));
        return;
      }

      const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
               || req.socket.remoteAddress || 'unknown';

      try {
        if (db) {
          await db.execute({
            sql: `INSERT INTO signatures (
              full_name, email, phone, state, zip,
              vin, year, make, model, color, plate,
              sig_name, sig_timestamp, sig_timezone, tos_version,
              chk_arbitration, chk_class_waiver, chk_liability_caps,
              chk_audio, chk_electrical, chk_ownership,
              chk_installer_independence, chk_full_agreement,
              installer_id, installer_name, ip_address
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
            args: [
              data.full_name, data.email, data.phone || null,
              data.state || null, data.zip || null,
              data.vin.trim().toUpperCase(), data.year || null, data.make || null,
              data.model || null, data.color || null, data.plate || null,
              data.sig_name, data.sig_timestamp, data.sig_timezone || null,
              data.tos_version,
              data.chk_arbitration ? 1 : 0,
              data.chk_class_waiver ? 1 : 0,
              data.chk_liability_caps ? 1 : 0,
              data.chk_audio ? 1 : 0,
              data.chk_electrical ? 1 : 0,
              data.chk_ownership ? 1 : 0,
              data.chk_installer_independence ? 1 : 0,
              data.chk_full_agreement ? 1 : 0,
              data.installer_id || null,
              data.installer_name || null,
              ip
            ]
          });
        }
        console.log(`[sign] Signature recorded: ${data.full_name} | VIN: ${data.vin} | Installer: ${data.installer_id || 'none'}`);

        // Get Stripe payment link — look up by stripe_account_id (not DB integer id)
        let stripe_url = null;
        if (data.installer_id && db) {
          const ins = await db.execute({
            sql: 'SELECT stripe_payment_link FROM installers WHERE stripe_account_id = ? AND status = ?',
            args: [data.installer_id, 'active']
          });
          if (ins.rows.length > 0 && ins.rows[0].stripe_payment_link) {
            stripe_url = ins.rows[0].stripe_payment_link + '?prefilled_email=' + encodeURIComponent(data.email);
          }
        }

        if (!stripe_url && process.env.STRIPE_PAYMENT_LINK_SINGLE) {
          stripe_url = process.env.STRIPE_PAYMENT_LINK_SINGLE
            + '?prefilled_email=' + encodeURIComponent(data.email);
        }

        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, stripe_url, message: 'Signature recorded successfully' }));
      } catch(e) {
        console.error('[sign] Error:', e.message);
        res.writeHead(500, cors);
        res.end(JSON.stringify({ error: 'Failed to save signature: ' + e.message }));
      }
    });
    return;
  }

  // ── ADMIN — List signatures ───────────────────────────────────
  if (url.pathname === '/admin/signatures' && req.method === 'GET') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const rows = await db.execute({ sql: 'SELECT * FROM signatures ORDER BY created_at DESC LIMIT 200', args: [] });
      res.writeHead(200, cors); res.end(JSON.stringify(rows.rows));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── ACCOUNT — Magic login ────────────────────────────────────
  if (url.pathname === '/account/login' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        if (!data.email) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Email required' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        const r = await db.execute({
          sql: "SELECT COUNT(*) as cnt FROM vehicles WHERE owner_email = ?",
          args: [data.email]
        });
        if (!r.rows[0]?.cnt) {
          res.writeHead(200, cors);
          res.end(JSON.stringify({ success: true, message: 'If an account exists, a login link was sent.' }));
          return;
        }

        const magicToken = crypto.randomBytes(24).toString('hex');
        const expiresAt = Date.now() + (30 * 60 * 1000); // 30 minutes
        await db.execute({
          sql: 'INSERT INTO magic_tokens (token, email, expires_at) VALUES (?,?,?)',
          args: [magicToken, data.email, expiresAt]
        });

        const siteUrl = process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro';
        const loginUrl = `${siteUrl}/account.html?magic=${magicToken}`;
        await sendEmail(data.email, 'TexTrack — Your Login Link', `
          <div style="font-family:sans-serif;background:#0c0c0c;color:#ccc;padding:24px;">
            <h2 style="color:#e09020;">TexTrack Account Access</h2>
            <p>Click below to access your account:</p>
            <a href="${loginUrl}" style="display:inline-block;background:#e09020;color:#000;padding:12px 24px;text-decoration:none;font-weight:bold;border-radius:4px;">Open My Account</a>
            <p style="margin-top:16px;font-size:12px;color:#666;">This link expires in 30 minutes.</p>
          </div>
        `);

        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, message: 'If an account exists, a login link was sent.' }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── ACCOUNT — Verify magic token + get vehicles ──────────────
  if (url.pathname === '/account/verify' && req.method === 'GET') {
    const magicToken = url.searchParams.get('token');
    if (!magicToken || !db) {
      res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Invalid request' })); return;
    }
    try {
      const r = await db.execute({
        sql: 'SELECT * FROM magic_tokens WHERE token = ? AND used = 0 AND expires_at > ?',
        args: [magicToken, Date.now()]
      });
      const mt = r.rows[0];
      if (!mt) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Link expired or invalid' })); return; }

      // Mark as used
      await db.execute({ sql: 'UPDATE magic_tokens SET used = 1 WHERE token = ?', args: [magicToken] });

      const email = mt.email;
      const vehiclesResult = await db.execute({
        sql: 'SELECT * FROM vehicles WHERE owner_email = ? ORDER BY created_at DESC',
        args: [email]
      });

      const siteUrl = process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro';
      const vehicles = vehiclesResult.rows.map(v => ({
        vin: v.vin, make: v.make, model: v.model, year: v.year,
        color: v.color, plate: v.plate, status: v.status,
        plan: v.plan, install_date: v.install_date,
        control_url: v.status === 'active' ? `${siteUrl}/control.html?token=${v.token}` : null
      }));

      res.writeHead(200, cors);
      res.end(JSON.stringify({ success: true, email, vehicles }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── DEFAULT ─────────────────────────────────────────────────
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('TexTrack relay active\n');
});

// ── WEBSOCKET ─────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

// Heartbeat — ping every 30s, terminate dead sockets
const HEARTBEAT_INTERVAL = 30000;

function heartbeat() { this.isAlive = true; }

const heartbeatTimer = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      console.log('[-] Terminating dead socket');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_INTERVAL);

wss.on('close', () => clearInterval(heartbeatTimer));

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);

  let role = null, token = null;
  let authTimer = setTimeout(() => { if (!role) ws.terminate(); }, 15000);

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'ping') { ws.isAlive = true; return; }

      if (msg.type === 'register') {
        if (!msg.token || !(await isValidToken(msg.token))) { ws.terminate(); return; }
        clearTimeout(authTimer); authTimer = null;
        token = msg.token; role = msg.role;
        const room = getRoom(token);
        if (role === 'device') {
          // Clean up old device connection first
          if (room.device && room.device !== ws && room.device.readyState === 1) {
            room.device.terminate();
          }
          room.device = ws;
          console.log('[*] Device registered — token ok');
          safeBroadcast(room.controls, { type: 'device_online' });
          // If recovery mode is active, tell device immediately
          if (room.recoveryMode) {
            safeSend(ws, { type: 'recovery_mode', active: true });
          }
        }
        if (role === 'control') {
          room.controls.push(ws);
          console.log('[*] Control registered (' + room.controls.length + ' active)');
          if (room.lastLocation) safeSend(ws, { ...room.lastLocation, type: 'location' });
          // Tell control if recovery mode is active
          if (room.recoveryMode) {
            safeSend(ws, { type: 'recovery_status', active: true });
          }
        }
        return;
      }

      if (!token || !role) return;
      const room = getRoom(token);

      if (msg.type === 'location' || msg.type === 'location_final') {
        room.lastLocation = msg;
        safeBroadcast(room.controls, msg);
        bufferPing(token, msg);
        return;
      }

      if (msg.type === 'voltage_alert') { safeBroadcast(room.controls, msg); return; }

      if (msg.type === 'screen_off') {
        if (room.device && room.device.readyState === 1) safeSend(room.device, msg);
        return;
      }

      // Flash, alarm, intercom commands from control → device
      if (['flash', 'stop_flash', 'alarm', 'stop_alarm', 'intercom_start', 'intercom_stop'].includes(msg.type)) {
        if (room.device && room.device.readyState === 1) safeSend(room.device, msg);
        return;
      }

      if (msg.type === 'webrtc_offer') {
        if (room.device && room.device.readyState === 1) safeSend(room.device, msg); return;
      }
      if (msg.type === 'webrtc_answer') {
        const ctrl = room.controls.find(c => c.readyState === 1);
        if (ctrl) safeSend(ctrl, msg); return;
      }
      if (msg.type === 'ice_candidate') {
        if (role === 'device') {
          const ctrl = room.controls.find(c => c.readyState === 1);
          if (ctrl) safeSend(ctrl, msg);
        } else {
          if (room.device && room.device.readyState === 1) safeSend(room.device, msg);
        }
        return;
      }
    } catch(e) { console.error('Message error:', e.message); }
  });

  ws.on('close', () => {
    if (authTimer) { clearTimeout(authTimer); authTimer = null; }
    if (!token || !role) return;
    const room = getRoom(token);
    if (role === 'device') {
      // Only clear if this is still the current device
      if (room.device === ws) {
        room.device = null;
        safeBroadcast(room.controls, { type: 'device_offline' });
      }
    }
    if (role === 'control') {
      room.controls = room.controls.filter(c => c !== ws);
    }
    console.log('[-] Disconnected (' + role + ')');
  });

  ws.on('error', (e) => console.error('WS error:', e.message));
});

function safeSend(ws, obj) {
  try { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); } catch(e) {}
}
function safeBroadcast(clients, obj) { clients.forEach(c => safeSend(c, obj)); }

process.on('uncaughtException', (e) => console.error('Uncaught:', e.message));
process.on('unhandledRejection', (e) => console.error('Unhandled:', e));

setInterval(() => console.log('[heartbeat] rooms:', rooms.size, '| tokens cached:', validTokenCache.size, '| ping buffer:', pingBuffer.length), 60000);

initDB().then(() => {
  httpServer.listen(PORT, () => console.log('TexTrack relay listening on port', PORT));
});
