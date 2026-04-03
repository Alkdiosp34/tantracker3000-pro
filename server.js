// TanTracker3000 — Backend with Stripe + WebSocket
// Uses Turso for GPS history, Stripe for billing, WebSocket for live tracking

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const { createClient } = require('@libsql/client');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

const PORT = process.env.PORT || 8080;
const HISTORY_DAYS = 60;

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
      }
    ], 'write');
    console.log('[db] Turso connected and ready');
    pruneHistory();
  } catch(e) {
    console.error('[db] Init error:', e.message);
    db = null;
  }
}

async function pruneHistory() {
  if (!db) return;
  const cutoff = Date.now() - (HISTORY_DAYS * 24 * 60 * 60 * 1000);
  try {
    const r = await db.execute({ sql: 'DELETE FROM pings WHERE ts < ?', args: [cutoff] });
    if (r.rowsAffected > 0) console.log(`[db] Pruned ${r.rowsAffected} old pings`);
  } catch(e) { console.error('[db] Prune error:', e.message); }
}
setInterval(pruneHistory, 60 * 60 * 1000);

async function getHistory(token, days) {
  if (!db) return [];
  const since = Date.now() - (days * 24 * 60 * 60 * 1000);
  try {
    const r = await db.execute({
      sql: 'SELECT ts, lat, lon, mph, acc, moving, voltage FROM pings WHERE token = ? AND ts >= ? ORDER BY ts ASC',
      args: [token, since]
    });
    return r.rows;
  } catch(e) { return []; }
}

async function storePing(token, msg) {
  if (!db) return;
  try {
    await db.execute({
      sql: 'INSERT INTO pings (token, ts, lat, lon, mph, acc, moving, voltage) VALUES (?,?,?,?,?,?,?,?)',
      args: [token, msg.ts || Date.now(), msg.lat, msg.lon, msg.mph||0, msg.acc||0, msg.moving?1:0, msg.voltage||null]
    });
  } catch(e) { console.error('[db] Insert error:', e.message); }
}

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
    return token;
  } catch(e) { console.error('[db] Create vehicle error:', e.message); return null; }
}

// ── ADMIN AUTH ────────────────────────────────────────────────
const ADMIN_KEY = process.env.ADMIN_KEY;
if (!ADMIN_KEY) throw new Error('[startup] ADMIN_KEY environment variable is required — server will not start without it');

function isAdmin(req) {
  const key = req.headers['x-admin-key'] || new URL(req.url, 'http://localhost').searchParams.get('key');
  return key === ADMIN_KEY;
}

// ── CUSTOMER TOKENS ───────────────────────────────────────────
// NOTE: Add PERSONAL_TEST_TOKEN env var in Render for your own device during testing
const validTokenCache = new Set(
  process.env.PERSONAL_TEST_TOKEN ? [process.env.PERSONAL_TEST_TOKEN] : []
);

async function isValidToken(token) {
  if (validTokenCache.has(token)) return true;
  const vehicle = await getVehicleByToken(token);
  if (vehicle && vehicle.status === 'active') {
    validTokenCache.add(token);
    return true;
  }
  return false;
}

// ── ROOMS ─────────────────────────────────────────────────────
const rooms = new Map();
function getRoom(token) {
  if (!rooms.has(token)) rooms.set(token, { device: null, controls: [], lastLocation: null });
  return rooms.get(token);
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
    req.on('data', chunk => body += chunk);
    req.on('end', async () => {
      const sig = req.headers['stripe-signature'];
      let event;
      try {
        event = stripe.webhooks.constructEvent(body, sig, process.env.STRIPE_WEBHOOK_SECRET);
      } catch(e) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Webhook error: ' + e.message);
        return;
      }

      console.log('[stripe] webhook:', event.type);

      if (event.type === 'checkout.session.completed') {
        const session = event.data.object;
        // Parse custom fields — keys must match what paymentLinks.create sets above
        const fields = {};
        (session.custom_fields || []).forEach(f => {
          fields[f.key] = f.text?.value || f.dropdown?.value || '';
        });
        // fields now has: vin, make, model, year, color, plate
        // plate_state: not a separate field — stored together in plate (e.g. "ABC-1234 TX")

        // IMPORTANT: metadata key is installer_stripe_id (the Stripe acct_ ID)
        const installerStripeId = session.metadata?.installer_stripe_id;
        const installerName = session.metadata?.installer_name;

        // Determine plan based on existing subscriptions for this customer
        const existingSubs = await stripe.subscriptions.list({ customer: session.customer, limit: 10 });
        const plan = existingSubs.data.length > 1 ? 'multi' : 'single';

        // Look up installer DB record by their Stripe account ID
        let installerDbId = null;
        if (installerStripeId && db) {
          try {
            const ins = await db.execute({
              sql: 'SELECT id FROM installers WHERE stripe_account_id = ?',
              args: [installerStripeId]
            });
            installerDbId = ins.rows[0]?.id || null;
          } catch(e) { console.error('[stripe] Installer lookup error:', e.message); }
        }

        const token = await createVehicle({
          vin: fields.vin || '',
          make: fields.make || '',
          model: fields.model || '',
          year: fields.year || '',
          color: fields.color || '',
          plate: fields.plate || '',
          plate_state: '',  // combined in plate field
          owner_name: session.customer_details?.name || '',
          owner_email: session.customer_details?.email || '',
          owner_phone: session.customer_details?.phone || '',
          installer_id: installerDbId,        // DB integer id for FK
          installer_name: installerName || '',
          stripe_customer_id: session.customer,
          stripe_subscription_id: session.subscription,
          plan
        });

        console.log('[stripe] New vehicle registered, token:', token);

        // Transfer $18 install fee to installer — uses Stripe acct_ id, not DB id
        if (installerStripeId && token) {
          try {
            await stripe.transfers.create({
              amount: 1800,
              currency: 'usd',
              destination: installerStripeId,  // MUST be Stripe acct_ ID
              description: `Install fee for vehicle ${fields.vin}`
            });
            console.log('[stripe] Install fee transferred to installer', installerStripeId);
          } catch(e) { console.error('[stripe] Transfer error:', e.message); }
        }
      }

      if (event.type === 'invoice.payment_succeeded') {
        const invoice = event.data.object;
        if (invoice.subscription && db) {
          try {
            const r = await db.execute({
              sql: 'SELECT v.*, i.stripe_account_id as installer_stripe_id FROM vehicles v LEFT JOIN installers i ON v.installer_id = i.id WHERE v.stripe_subscription_id = ?',
              args: [invoice.subscription]
            });
            const vehicle = r.rows[0];
            // Transfer $5/month to installer using their Stripe acct_ ID from the JOIN
            if (vehicle?.installer_stripe_id) {
              await stripe.transfers.create({
                amount: 500,
                currency: 'usd',
                destination: vehicle.installer_stripe_id,  // MUST be Stripe acct_ ID
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
            await db.execute({
              sql: "UPDATE vehicles SET status = 'cancelled' WHERE stripe_subscription_id = ?",
              args: [sub.id]
            });
            console.log('[stripe] Subscription cancelled for', sub.id);

            const r = await db.execute({
              sql: 'SELECT * FROM vehicles WHERE stripe_subscription_id = ?',
              args: [sub.id]
            });
            const vehicle = r.rows[0];
            if (vehicle && vehicle.install_date) {
              const monthsActive = Math.floor((Date.now() - vehicle.install_date) / (1000 * 60 * 60 * 24 * 30));
              const remainingMonths = 4 - monthsActive;
              if (remainingMonths > 0 && vehicle.stripe_customer_id) {
                const etfAmount = remainingMonths * (vehicle.plan === 'multi' ? 19 : 22) * 100;
                try {
                  await stripe.invoiceItems.create({
                    customer: vehicle.stripe_customer_id,
                    amount: etfAmount,
                    currency: 'usd',
                    description: `Early termination fee - ${remainingMonths} remaining month(s) (VIN: ${vehicle.vin})`
                  });
                  const inv = await stripe.invoices.create({
                    customer: vehicle.stripe_customer_id,
                    collection_method: 'send_invoice',
                    days_until_due: 30
                  });
                  await stripe.invoices.finalizeInvoice(inv.id);
                  await stripe.invoices.sendInvoice(inv.id);
                  console.log('[stripe] ETF invoice sent: $' + (etfAmount/100) + ' for ' + remainingMonths + ' months');
                } catch(e) { console.error('[stripe] ETF invoice error:', e.message); }
              }
            }
          } catch(e) { console.error('[stripe] Cancellation handler error:', e.message); }
        }
      }

  // ── HISTORY API ─────────────────────────────────────────────
  if (url.pathname === '/history' && req.method === 'GET') {
    const token = url.searchParams.get('token');
    const days = Math.min(parseInt(url.searchParams.get('days') || '7'), HISTORY_DAYS);
    if (!token || !(await isValidToken(token))) {
      res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Invalid token' })); return;
    }
    const pings = await getHistory(token, days);
    res.writeHead(200, cors);
    res.end(JSON.stringify({ pings, days, count: pings.length }));
    return;
  }

  // ── ADMIN — GET ALL VEHICLES ─────────────────────────────────
  if (url.pathname === '/admin/vehicles' && req.method === 'GET') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    const vehicles = await getAllVehicles();
    res.writeHead(200, cors); res.end(JSON.stringify(vehicles)); return;
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
        // Create Stripe Express account
        const account = await stripe.accounts.create({
          type: 'express',
          email: data.email,
          capabilities: { transfers: { requested: true } },
          business_type: 'individual',
          metadata: { name: data.name, phone: data.phone || '' }
        });
        // Generate onboarding link
        const link = await stripe.accountLinks.create({
          account: account.id,
          refresh_url: `https://tantracker3000.onrender.com/admin/installers`,
          return_url: `https://tantracker3000.onrender.com/admin/installers`,
          type: 'account_onboarding'
        });
        // Save installer to DB
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

  // ── ADMIN — GET PAYMENT LINK FOR INSTALLER ───────────────────
  if (url.pathname === '/admin/payment-link' && req.method === 'POST') {
    if (!isAdmin(req)) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        // data must include: installer_stripe_id, installer_name, installer_db_id
        const link = await stripe.paymentLinks.create({
          line_items: [
            { price: process.env.STRIPE_INSTALL_PRICE_ID, quantity: 1 },
            { price: process.env.STRIPE_SINGLE_PRICE_ID, quantity: 1 }
          ],
         custom_fields: [
            { key: 'vin', label: { type: 'custom', custom: 'Vehicle VIN (17 characters)' }, type: 'text', optional: false },
            { key: 'year_make_model', label: { type: 'custom', custom: 'Year, Make, Model (e.g. 2021 Ford F-150)' }, type: 'text', optional: false },
            { key: 'color_plate', label: { type: 'custom', custom: 'Color + License Plate (e.g. White ABC-1234 TX)' }, type: 'text', optional: false }
          ],
          after_completion: {
            type: 'redirect',
            redirect: { url: process.env.WELCOME_URL || 'https://alkdiosp34.github.io/tantracker3000-pro/welcome.html' }
          }
        });

        // FIX: Save the payment link URL back to the installer record in DB
        if (db && data.installer_stripe_id) {
          await db.execute({
            sql: 'INSERT INTO installers (name, email, stripe_account_id, stripe_payment_link, status) VALUES (?, ?, ?, ?, ?) ON CONFLICT(stripe_account_id) DO UPDATE SET stripe_payment_link = excluded.stripe_payment_link',
            args: [data.installer_name || 'Installer', data.installer_name + '@installer.com', data.installer_stripe_id, link.url, 'active']
          });
          console.log(`[admin] Payment link saved for installer ${data.installer_stripe_id}`);
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

      // Validate required fields
      const required = ['full_name','email','vin','sig_name','sig_timestamp','tos_version'];
      const missing = required.filter(f => !data[f]);
      if (missing.length) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: 'Missing fields: ' + missing.join(', ') }));
        return;
      }

      // Signature name must match full name (case-insensitive)
      if (data.sig_name.trim().toLowerCase() !== data.full_name.trim().toLowerCase()) {
        res.writeHead(400, cors);
        res.end(JSON.stringify({ error: 'Signature name does not match full name' }));
        return;
      }

      // Capture IP for E-SIGN records
      const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim()
               || req.socket.remoteAddress || 'unknown';

      try {
        // Store signature record
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
              data.vin, data.year || null, data.make || null,
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

        // Get Stripe payment link for this installer
        // installer_id in the URL param is the Stripe acct_ ID (baked into QR code)
        let stripe_url = null;
        if (data.installer_id && db) {
          try {
            const ins = await db.execute({
              sql: 'SELECT stripe_payment_link FROM installers WHERE stripe_account_id = ? AND status = ?',
              args: [data.installer_id, 'active']
            });
            if (ins.rows.length > 0 && ins.rows[0].stripe_payment_link) {
              const base = ins.rows[0].stripe_payment_link;
              stripe_url = base + '?prefilled_email=' + encodeURIComponent(data.email);
            }
          } catch(e) { console.error('[sign] Installer lookup error:', e.message); }
        }

        // Fallback to single-vehicle payment link from env
        if (!stripe_url && process.env.STRIPE_PAYMENT_LINK_SINGLE) {
          stripe_url = process.env.STRIPE_PAYMENT_LINK_SINGLE
            + '?prefilled_email=' + encodeURIComponent(data.email);
        }

        res.writeHead(200, cors);
        res.end(JSON.stringify({
          success: true,
          stripe_url,
          message: 'Signature recorded successfully'
        }));
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
      const rows = await db.execute({
        sql: 'SELECT * FROM signatures ORDER BY created_at DESC LIMIT 200',
        args: []
      });
      res.writeHead(200, cors);
      res.end(JSON.stringify(rows.rows));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('TanTracker3000 relay active\n');
});

// ── WEBSOCKET ─────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  let role = null, token = null;
  let authTimer = setTimeout(() => { if (!role) ws.terminate(); }, 15000);

  ws.on('message', async (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'ping') return;

      if (msg.type === 'register') {
        if (!msg.token || !(await isValidToken(msg.token))) { ws.terminate(); return; }
        clearTimeout(authTimer); authTimer = null;
        token = msg.token; role = msg.role;
        const room = getRoom(token);
        if (role === 'device') {
          if (room.device && room.device.readyState === 1) room.device.terminate();
          room.device = ws;
          console.log('[*] Device registered');
          safeBroadcast(room.controls, { type: 'device_online' });
        }
        if (role === 'control') {
          room.controls.push(ws);
          console.log('[*] Control registered (' + room.controls.length + ' active)');
          if (room.lastLocation) safeSend(ws, { ...room.lastLocation, type: 'location' });
        }
        return;
      }

      if (!token || !role) return;
      const room = getRoom(token);

      if (msg.type === 'location' || msg.type === 'location_final') {
        room.lastLocation = msg;
        safeBroadcast(room.controls, msg);
        storePing(token, msg);
        return;
      }

      if (msg.type === 'voltage_alert') { safeBroadcast(room.controls, msg); return; }

      if (msg.type === 'screen_off') {
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
    if (role === 'device') { room.device = null; safeBroadcast(room.controls, { type: 'device_offline' }); }
    if (role === 'control') { room.controls = room.controls.filter(c => c !== ws); }
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

setInterval(() => console.log('[heartbeat] rooms:', rooms.size, '| vehicles cached:', validTokenCache.size), 60000);

initDB().then(() => {
  httpServer.listen(PORT, () => console.log('TanTracker3000 relay listening on port', PORT));
});
