// TanTracker3000 — Backend with Stripe + WebSocket
// Uses Turso for GPS history, Stripe for billing, WebSocket for live tracking

const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');
const { createClient } = require('@libsql/client');
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

const PORT = process.env.PORT || 8080;
const HISTORY_DAYS = 60;

// ── EMAIL (Resend) ────────────────────────────────────────────
async function sendEmail({ to, subject, html }) {
  if (!process.env.RESEND_API_KEY) {
    console.warn('[email] RESEND_API_KEY not set — email skipped:', subject, '->', to);
    return false;
  }
  try {
    const res = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + process.env.RESEND_API_KEY,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        from: process.env.FROM_EMAIL || 'TexTrack <noreply@textrack.com>',
        to: [to],
        subject,
        html
      })
    });
    const data = await res.json();
    if (!res.ok) { console.error('[email] Send error:', data); return false; }
    console.log('[email] Sent:', subject, '->', to);
    return true;
  } catch(e) {
    console.error('[email] Error:', e.message);
    return false;
  }
}

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
        sql: `CREATE TABLE IF NOT EXISTS pins (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT UNIQUE NOT NULL,
          pin_hash TEXT NOT NULL,
          reset_code TEXT,
          reset_expires INTEGER,
          completed_min_period INTEGER DEFAULT 0,
          setup_attempts INTEGER DEFAULT 0,
          setup_locked_until INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (strftime('%s','now')),
          updated_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS verify_codes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token TEXT NOT NULL,
          code TEXT NOT NULL,
          expires_at INTEGER NOT NULL,
          used INTEGER DEFAULT 0,
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
          stripe_customer_id TEXT NOT NULL,
          email TEXT,
          vin TEXT, make TEXT, model TEXT, year TEXT, color TEXT, plate TEXT,
          expires_at INTEGER NOT NULL,
          used INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (strftime('%s','now'))
        )`, args: []
      },
      {
        sql: `CREATE TABLE IF NOT EXISTS transfers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code TEXT UNIQUE NOT NULL,
          token TEXT NOT NULL,
          old_customer_id TEXT NOT NULL,
          old_email TEXT,
          vin TEXT, make TEXT, model TEXT, year TEXT, color TEXT, plate TEXT,
          expires_at INTEGER NOT NULL,
          used INTEGER DEFAULT 0,
          new_customer_id TEXT,
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

        // Send welcome email with control panel link
        if (token && session.customer_details?.email) {
          const vehicleLabel = [fields.year_make_model || ''].join('').trim() || 'your vehicle';
          const controlUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/control.html?token=' + token;
          const accountUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/account.html';
          await sendEmail({
            to: session.customer_details.email,
            subject: 'Welcome to TexTrack — your tracker is ready',
            html: `
              <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
                <div style="font-size:26px;font-weight:700;color:#e09020;letter-spacing:4px;margin-bottom:4px;">TexTrack</div>
                <div style="font-size:10px;color:#666;letter-spacing:3px;margin-bottom:24px;">VEHICLE PROTECTION · HOUSTON, TX</div>

                <h2 style="color:#fff;font-size:20px;margin-bottom:12px;">You're all set. 🎉</h2>
                <p style="color:#aaa;line-height:1.7;margin-bottom:24px;">
                  Payment confirmed. Your installer will contact you within 24 hours to schedule installation.
                  Installation takes about 1 hour and you'll need to be present or leave your keys.
                </p>

                <div style="background:#111;border:1px solid #1a4a2a;border-radius:6px;padding:16px;margin-bottom:24px;">
                  <div style="font-size:10px;letter-spacing:2px;color:#666;margin-bottom:8px;">YOUR CONTROL PANEL</div>
                  <p style="color:#aaa;font-size:12px;line-height:1.7;margin-bottom:12px;">
                    Once your tracker is installed, use this link to track your vehicle in real time.
                    Bookmark it or add TexTrack to your home screen for quick access.
                  </p>
                  <a href="${controlUrl}" style="display:block;background:#00cc66;color:#000;text-align:center;padding:14px;border-radius:4px;font-weight:bold;font-size:15px;text-decoration:none;letter-spacing:2px;">
                    OPEN MY TRACKER →
                  </a>
                </div>

                <div style="background:#111;border:1px solid #222;border-radius:6px;padding:16px;margin-bottom:24px;">
                  <div style="font-size:10px;letter-spacing:2px;color:#666;margin-bottom:8px;">NEXT STEPS</div>
                  <div style="font-size:12px;color:#aaa;line-height:2;">
                    <div>1 · Your installer contacts you within 24 hours</div>
                    <div>2 · Installation takes about 1 hour</div>
                    <div>3 · You'll receive a PIN setup prompt on first use</div>
                    <div>4 · Your $22/month subscription starts today</div>
                  </div>
                </div>

                <p style="font-size:11px;color:#555;text-align:center;line-height:1.7;">
                  Manage your account anytime at <a href="${accountUrl}" style="color:#e09020;">${accountUrl.replace('https://','')}</a><br>
                  Questions? Reply to this email or text us directly.
                </p>
              </div>
            `
          });
          console.log('[email] Welcome email sent to:', session.customer_details.email);
        }

        // Check if this is a REACTIVATION
        const reactivationCode = session.metadata?.reactivation_code;
        const reactivationToken = session.metadata?.reactivation_token;
        if (reactivationCode && reactivationToken && db) {
          try {
            const rr = await db.execute({ sql: 'SELECT * FROM reactivations WHERE code = ? AND used = 0', args: [reactivationCode] });
            const reactivation = rr.rows[0];
            if (reactivation) {
              // Reactivate the original vehicle record — flip status back to active, update subscription
              await db.execute({
                sql: "UPDATE vehicles SET status = 'active', stripe_customer_id = ?, stripe_subscription_id = ?, owner_email = ?, install_date = COALESCE(install_date, ?) WHERE token = ? AND status = 'cancelled'",
                args: [session.customer, session.subscription, session.customer_details?.email || reactivation.email, Date.now(), reactivationToken]
              });
              // Mark reactivation used
              await db.execute({ sql: 'UPDATE reactivations SET used = 1 WHERE code = ?', args: [reactivationCode] });
              // Clear from token cache so it re-validates from DB
              validTokenCache.delete(reactivationToken);
              console.log('[reactivate] Vehicle reactivated — token:', reactivationToken);
            }
          } catch(e) { console.error('[reactivate] Error:', e.message); }
        }

        // Check if this is a transfer completion
        const transferCode = session.metadata?.transfer_code;
        if (transferCode && db) {
          try {
            const tr = await db.execute({ sql: 'SELECT * FROM transfers WHERE code = ? AND used = 0', args: [transferCode] });
            const transfer = tr.rows[0];
            if (transfer) {
              // 1. Look up the old vehicle record for history preservation
              const oldRec = await db.execute({ sql: 'SELECT * FROM vehicles WHERE token = ?', args: [transfer.token] });
              const oldVehicle = oldRec.rows[0];

              // 2. Mark old vehicle as TRANSFERRED (keep all history intact)
              await db.execute({
                sql: "UPDATE vehicles SET status = 'transferred', stripe_subscription_id = NULL WHERE token = ?",
                args: [transfer.token]
              });

              // 3. Cancel old customer's Stripe subscription
              if (oldVehicle?.stripe_subscription_id) {
                await stripe.subscriptions.cancel(oldVehicle.stripe_subscription_id).catch(e => {
                  console.error('[transfer] Cancel old sub error:', e.message);
                });
              }

              // 4. Create NEW vehicle record for new owner with the SAME token
              //    This preserves full chain of custody — old record stays intact
              await db.execute({
                sql: `INSERT INTO vehicles (vin, make, model, year, color, plate, plate_state,
                      owner_name, owner_email, owner_phone, installer_id, installer_name,
                      stripe_customer_id, stripe_subscription_id, token, plan, status, install_date)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
                args: [
                  oldVehicle?.vin || transfer.vin,
                  oldVehicle?.make || transfer.make,
                  oldVehicle?.model || transfer.model,
                  oldVehicle?.year || transfer.year,
                  oldVehicle?.color || transfer.color,
                  oldVehicle?.plate || transfer.plate,
                  oldVehicle?.plate_state || '',
                  session.customer_details?.name || transfer.new_owner_name || '',
                  session.customer_details?.email || transfer.new_owner_email || '',
                  session.customer_details?.phone || '',
                  oldVehicle?.installer_id || null,
                  oldVehicle?.installer_name || null,
                  session.customer,
                  session.subscription,
                  transfer.token, // Same token — device keeps working
                  oldVehicle?.plan || 'single',
                  'active',
                  Date.now()
                ]
              });

              // 5. Mark transfer as used
              await db.execute({
                sql: 'UPDATE transfers SET used = 1, new_customer_id = ? WHERE code = ?',
                args: [session.customer, transferCode]
              });

              console.log('[transfer] Complete — VIN:', transfer.vin, '| Old owner:', oldVehicle?.stripe_customer_id, '→ New owner:', session.customer);
            }
          } catch(e) { console.error('[transfer] Completion error:', e.message); }
        }

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

            // Check if customer has now completed 4-month minimum period
            if (vehicle.install_date) {
              const monthsActive = Math.floor((Date.now() - vehicle.install_date) / (1000 * 60 * 60 * 24 * 30));
              if (monthsActive >= 4) {
                await db.execute({
                  sql: "UPDATE pins SET completed_min_period = 1 WHERE token = ?",
                  args: [vehicle.token]
                }).catch(() => {});
              }
            }
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

            // Auto-generate and attach reactivation link to ETF invoice (or send separately)
            try {
              const cancelledVehicle = await db.execute({
                sql: "SELECT * FROM vehicles WHERE stripe_subscription_id = ? AND status = 'cancelled'",
                args: [sub.id]
              });
              const cv = cancelledVehicle.rows[0];
              if (cv && cv.stripe_customer_id) {
                const reactCode = crypto.randomBytes(12).toString('hex');
                const reactExpiry = Date.now() + (30 * 24 * 60 * 60 * 1000); // 30 days to reactivate
                await db.execute({
                  sql: 'INSERT OR IGNORE INTO reactivations (code, token, stripe_customer_id, email, vin, make, model, year, color, plate, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                  args: [reactCode, cv.token, cv.stripe_customer_id, cv.owner_email, cv.vin, cv.make, cv.model, cv.year, cv.color, cv.plate, reactExpiry]
                });
                const reactUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + reactCode;
                console.log('[reactivate] Auto-link generated for lapsed customer:', cv.stripe_customer_id, '|', reactUrl);

                // Save reactivation URL to Stripe customer metadata (for manual fallback)
                await stripe.customers.update(cv.stripe_customer_id, {
                  metadata: { reactivate_url: reactUrl, reactivate_expires: new Date(reactExpiry).toISOString() }
                }).catch(() => {});

                // Auto-send reactivation email
                if (cv.owner_email) {
                  const vehicleLabel = [cv.year, cv.make, cv.model].filter(Boolean).join(' ') || 'your vehicle';
                  await sendEmail({
                    to: cv.owner_email,
                    subject: 'Reactivate your TexTrack service — ' + vehicleLabel,
                    html: `
                      <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
                        <div style="font-family:'Rajdhani',Arial,sans-serif;font-size:28px;font-weight:700;color:#e09020;letter-spacing:6px;margin-bottom:4px;">TEXTRACK</div>
                        <div style="font-size:10px;color:#666;letter-spacing:3px;margin-bottom:24px;">VEHICLE PROTECTION</div>

                        <h2 style="color:#fff;font-size:20px;margin-bottom:12px;">We miss you — come back for $22</h2>

                        <p style="color:#aaa;line-height:1.7;margin-bottom:16px;">
                          Your TexTrack service for your <strong style="color:#fff;">${vehicleLabel}</strong> has been cancelled.
                          The good news: your device is still installed and ready to go.
                        </p>

                        <div style="background:#111;border:1px solid #222;border-radius:6px;padding:16px;margin-bottom:24px;">
                          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1a1a1a;font-size:13px;">
                            <span style="color:#666;">Installation fee</span>
                            <span><span style="text-decoration:line-through;color:#444;">$99.00</span> <span style="color:#00cc66;font-weight:bold;">FREE</span></span>
                          </div>
                          <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1a1a1a;font-size:13px;">
                            <span style="color:#666;">First month</span>
                            <span style="color:#fff;">$22.00</span>
                          </div>
                          <div style="display:flex;justify-content:space-between;padding:10px 0 0;font-size:16px;font-weight:bold;">
                            <span style="color:#fff;">Due today</span>
                            <span style="color:#e09020;">$22.00</span>
                          </div>
                        </div>

                        <a href="${reactUrl}" style="display:block;background:#e09020;color:#000;text-align:center;padding:16px;border-radius:4px;font-weight:bold;font-size:16px;text-decoration:none;letter-spacing:2px;margin-bottom:16px;">
                          REACTIVATE FOR $22 →
                        </a>

                        <p style="font-size:11px;color:#555;text-align:center;line-height:1.6;">
                          This link expires in 30 days.<br>
                          No installation visit required — your device is already in the vehicle.<br>
                          Questions? Reply to this email or text us directly.
                        </p>
                      </div>
                    `
                  });
                }
              }
            } catch(e) { console.error('[reactivate] Auto-generate error:', e.message); }

            // ── ETF AUTO-INVOICE ──────────────────────────────────
            // If cancelled before 4-month minimum, invoice remaining months
            const r = await db.execute({
              sql: 'SELECT * FROM vehicles WHERE stripe_subscription_id = ?',
              args: [sub.id]
            });
            const vehicle = r.rows[0];
            if (vehicle && vehicle.install_date) {
              const monthsActive = Math.floor((Date.now() - vehicle.install_date) / (1000 * 60 * 60 * 24 * 30));
              const MIN_MONTHS = 4;
              const remainingMonths = MIN_MONTHS - monthsActive;
              if (remainingMonths > 0 && vehicle.stripe_customer_id) {
                const plan = vehicle.plan || 'single';
                const monthlyRate = plan === 'multi' ? 19 : 22;
                const etfAmount = remainingMonths * monthlyRate * 100; // cents
                try {
                  // Create a one-off invoice for the ETF
                  await stripe.invoiceItems.create({
                    customer: vehicle.stripe_customer_id,
                    amount: etfAmount,
                    currency: 'usd',
                    description: `Early termination fee — \${remainingMonths} remaining month(s) of \${MIN_MONTHS}-month minimum commitment (VIN: \${vehicle.vin})`
                  });
                  const invoice = await stripe.invoices.create({
                    customer: vehicle.stripe_customer_id,
                    collection_method: 'send_invoice',
                    days_until_due: 30,
                    description: `TexTrack Early Termination Fee — \${remainingMonths} month(s) remaining`
                  });
                  await stripe.invoices.finalizeInvoice(invoice.id);
                  await stripe.invoices.sendInvoice(invoice.id);
                  console.log(`[stripe] ETF invoice sent: \$\${etfAmount/100} for \${remainingMonths} months — customer \${vehicle.stripe_customer_id}`);
                } catch(e) {
                  console.error('[stripe] ETF invoice error:', e.message);
                }
              } else {
                console.log('[stripe] No ETF owed — customer completed minimum period or past 4 months');
              }
            }
          } catch(e) { console.error('[stripe] Cancellation handler error:', e.message); }
        }
      }

      res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok');
    });
    return;
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
            { key: 'vin', label: { type: 'custom', custom: 'Vehicle VIN' }, type: 'text', optional: false },
            { key: 'make', label: { type: 'custom', custom: 'Make (e.g. Ford)' }, type: 'text', optional: false },
            { key: 'model', label: { type: 'custom', custom: 'Model (e.g. F-150)' }, type: 'text', optional: false },
            { key: 'year', label: { type: 'custom', custom: 'Year (e.g. 2021)' }, type: 'text', optional: false },
            { key: 'color', label: { type: 'custom', custom: 'Vehicle Color' }, type: 'text', optional: false },
            { key: 'plate', label: { type: 'custom', custom: 'License Plate + State' }, type: 'text', optional: false }
          ],
          phone_number_collection: { enabled: true },
          // IMPORTANT: metadata uses installer_stripe_id so webhook can look up correct Stripe account
          metadata: {
            installer_stripe_id: data.installer_stripe_id,
            installer_name: data.installer_name
          },
          after_completion: {
            type: 'redirect',
            redirect: { url: process.env.WELCOME_URL || 'https://alkdiosp34.github.io/tantracker3000-pro/welcome.html' }
          }
        });

        // FIX: Save the payment link URL back to the installer record in DB
        if (db && data.installer_stripe_id) {
          await db.execute({
            sql: 'UPDATE installers SET stripe_payment_link = ? WHERE stripe_account_id = ?',
            args: [link.url, data.installer_stripe_id]
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

  // ── TRANSFER: INITIATE ────────────────────────────────────
  // Customer clicks "Sell/Transfer" in control.html → creates a transfer code
  if (url.pathname === '/transfer/initiate' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token } = data;
        if (!token) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing token' })); return; }

        // Look up the vehicle
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
        const r = await db.execute({ sql: 'SELECT * FROM vehicles WHERE token = ? AND status = ?', args: [token, 'active'] });
        const vehicle = r.rows[0];
        if (!vehicle) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Vehicle not found or inactive' })); return; }

        // Generate unique transfer code
        const code = crypto.randomBytes(12).toString('hex');
        const expiresAt = Date.now() + (48 * 60 * 60 * 1000); // 48 hours

        await db.execute({
          sql: 'INSERT INTO transfers (code, token, old_customer_id, old_email, vin, make, model, year, color, plate, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
          args: [code, token, vehicle.stripe_customer_id, vehicle.owner_email, vehicle.vin, vehicle.make, vehicle.model, vehicle.year, vehicle.color, vehicle.plate, expiresAt]
        });

        const transferUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/transfer.html?code=' + code;
        console.log('[transfer] Initiated for VIN:', vehicle.vin, '| Code:', code);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, code, transfer_url: transferUrl, expires_at: expiresAt }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── TRANSFER: GET INFO (new owner loads transfer page) ────
  if (url.pathname.startsWith('/transfer/') && req.method === 'GET') {
    const code = url.pathname.split('/transfer/')[1];
    if (!code) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const r = await db.execute({ sql: 'SELECT * FROM transfers WHERE code = ?', args: [code] });
      const transfer = r.rows[0];
      if (!transfer) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Transfer not found' })); return; }
      if (transfer.used) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Transfer already completed' })); return; }
      if (Date.now() > transfer.expires_at) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Transfer link expired' })); return; }

      res.writeHead(200, cors);
      res.end(JSON.stringify({
        valid: true,
        vin: transfer.vin,
        make: transfer.make,
        model: transfer.model,
        year: transfer.year,
        color: transfer.color,
        plate: transfer.plate,
        expires_at: transfer.expires_at
      }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── TRANSFER: COMPLETE (webhook handles payment, this records signature) ──
  if (url.pathname === '/transfer/sign' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { code, full_name, email, phone, state, zip, sig_name, sig_timestamp, sig_timezone } = data;
        if (!code || !full_name || !email || !sig_name) {
          res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing required fields' })); return;
        }
        if (sig_name.trim().toLowerCase() !== full_name.trim().toLowerCase()) {
          res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Signature name does not match' })); return;
        }

        const r = await db.execute({ sql: 'SELECT * FROM transfers WHERE code = ?', args: [code] });
        const transfer = r.rows[0];
        if (!transfer || transfer.used || Date.now() > transfer.expires_at) {
          res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Transfer invalid or expired' })); return;
        }

        const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || 'unknown';

        // Store signature record for the transfer
        await db.execute({
          sql: `INSERT INTO signatures (full_name, email, phone, state, zip, vin, make, model, year, color, plate,
            sig_name, sig_timestamp, sig_timezone, tos_version, ip_address)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          args: [full_name, email, phone||null, state||null, zip||null,
            transfer.vin, transfer.make, transfer.model, transfer.year, transfer.color, transfer.plate,
            sig_name, sig_timestamp, sig_timezone||null, 'v7-transfer-2026-04-03', ip]
        });

        // Generate Stripe payment link for monthly subscription only (no install fee)
        let stripe_url = null;
        if (process.env.STRIPE_SINGLE_PRICE_ID) {
          try {
            const link = await stripe.paymentLinks.create({
              line_items: [{ price: process.env.STRIPE_SINGLE_PRICE_ID, quantity: 1 }],
              phone_number_collection: { enabled: true },
              metadata: {
                transfer_code: code,
                new_owner_email: email,
                new_owner_name: full_name
              },
              after_completion: {
                type: 'redirect',
                redirect: { url: (process.env.WELCOME_URL || 'https://alkdiosp34.github.io/tantracker3000-pro/welcome.html') + '?transfer=1' }
              }
            });
            stripe_url = link.url + '?prefilled_email=' + encodeURIComponent(email);
          } catch(e) { console.error('[transfer] Payment link error:', e.message); }
        }

        console.log('[transfer] Signed by new owner:', full_name, '| Code:', code);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, stripe_url }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── SETUP: VERIFY EMAIL (first-time PIN setup) ────────────
  if (url.pathname === '/pin/setup/request' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, email } = data;
        if (!token || !email) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing fields' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        // Check lockout
        const pinRow = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
        if (pinRow.rows[0]?.setup_locked_until && Date.now() < pinRow.rows[0].setup_locked_until) {
          const wait = Math.ceil((pinRow.rows[0].setup_locked_until - Date.now()) / 60000);
          res.writeHead(429, cors); res.end(JSON.stringify({ error: 'locked', wait_minutes: wait })); return;
        }

        // Check vehicle exists and email matches
        const v = await db.execute({ sql: 'SELECT * FROM vehicles WHERE token = ?', args: [token] });
        const vehicle = v.rows[0];
        if (!vehicle) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Invalid token' })); return; }

        // Check email matches (case-insensitive)
        if (!vehicle.owner_email || vehicle.owner_email.toLowerCase() !== email.toLowerCase().trim()) {
          // Increment attempt counter
          const attempts = (pinRow.rows[0]?.setup_attempts || 0) + 1;
          const lockedUntil = attempts >= 4 ? Date.now() + (15 * 60 * 1000) : 0;
          await db.execute({
            sql: 'INSERT INTO pins (token, pin_hash, setup_attempts, setup_locked_until) VALUES (?,?,?,?) ON CONFLICT(token) DO UPDATE SET setup_attempts=excluded.setup_attempts, setup_locked_until=excluded.setup_locked_until',
            args: [token, '', attempts, lockedUntil]
          });
          const remaining = 4 - attempts;
          if (lockedUntil) {
            res.writeHead(429, cors); res.end(JSON.stringify({ error: 'locked', wait_minutes: 15 })); return;
          }
          res.writeHead(403, cors); res.end(JSON.stringify({ error: 'email_mismatch', attempts_remaining: remaining })); return;
        }

        // Email matches — reset attempt counter, send 6-digit code
        const code = String(Math.floor(100000 + Math.random() * 900000));
        const expiresAt = Date.now() + (15 * 60 * 1000);
        await db.execute({
          sql: 'INSERT INTO verify_codes (token, code, expires_at) VALUES (?,?,?)',
          args: [token, code, expiresAt]
        });
        // Reset attempt counter
        await db.execute({
          sql: 'INSERT INTO pins (token, pin_hash, setup_attempts, setup_locked_until) VALUES (?,?,0,0) ON CONFLICT(token) DO UPDATE SET setup_attempts=0, setup_locked_until=0',
          args: [token, '']
        });

        const vehicleLabel = [vehicle.year, vehicle.make, vehicle.model].filter(Boolean).join(' ') || 'your vehicle';
        await sendEmail({
          to: email,
          subject: 'TexTrack verification code — ' + vehicleLabel,
          html: `
            <div style="font-family:Arial,sans-serif;max-width:440px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
              <div style="font-size:24px;font-weight:700;color:#e09020;letter-spacing:4px;margin-bottom:24px;">TexTrack</div>
              <h2 style="color:#fff;font-size:18px;margin-bottom:12px;">Your verification code</h2>
              <p style="color:#aaa;line-height:1.7;margin-bottom:20px;">
                Use this code to verify your identity and set up your PIN for <strong style="color:#fff;">${vehicleLabel}</strong>.
              </p>
              <div style="background:#111;border:1px solid #333;border-radius:8px;padding:24px;text-align:center;margin-bottom:20px;">
                <div style="font-family:monospace;font-size:40px;font-weight:900;letter-spacing:12px;color:#e09020;">${code}</div>
                <div style="font-size:11px;color:#555;margin-top:8px;letter-spacing:1px;">EXPIRES IN 15 MINUTES</div>
              </div>
              <p style="font-size:11px;color:#555;text-align:center;">If you didn't request this, ignore this email. Your tracker is still secure.</p>
            </div>`
        });

        console.log('[setup] Verification code sent to:', email, 'for token:', token.slice(0,12) + '...');
        res.writeHead(200, cors); res.end(JSON.stringify({ success: true, message: 'Verification code sent' }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── SETUP: VERIFY CODE ─────────────────────────────────────
  if (url.pathname === '/pin/setup/verify' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, code } = data;
        if (!token || !code) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing fields' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        // Check lockout
        const pinRow = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
        if (pinRow.rows[0]?.setup_locked_until && Date.now() < pinRow.rows[0].setup_locked_until) {
          const wait = Math.ceil((pinRow.rows[0].setup_locked_until - Date.now()) / 60000);
          res.writeHead(429, cors); res.end(JSON.stringify({ error: 'locked', wait_minutes: wait })); return;
        }

        // Find most recent valid code for this token
        const cr = await db.execute({
          sql: 'SELECT * FROM verify_codes WHERE token = ? AND used = 0 AND expires_at > ? ORDER BY created_at DESC LIMIT 1',
          args: [token, Date.now()]
        });
        const verifyRow = cr.rows[0];
        if (!verifyRow) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Code expired or not found' })); return; }

        if (verifyRow.code !== code.trim()) {
          // Wrong code — increment lockout counter
          const attempts = (pinRow.rows[0]?.setup_attempts || 0) + 1;
          const lockedUntil = attempts >= 4 ? Date.now() + (15 * 60 * 1000) : 0;
          await db.execute({
            sql: 'INSERT INTO pins (token, pin_hash, setup_attempts, setup_locked_until) VALUES (?,?,?,?) ON CONFLICT(token) DO UPDATE SET setup_attempts=excluded.setup_attempts, setup_locked_until=excluded.setup_locked_until',
            args: [token, pinRow.rows[0]?.pin_hash || '', attempts, lockedUntil]
          });
          if (lockedUntil) {
            res.writeHead(429, cors); res.end(JSON.stringify({ error: 'locked', wait_minutes: 15 })); return;
          }
          res.writeHead(403, cors); res.end(JSON.stringify({ error: 'wrong_code', attempts_remaining: 4 - attempts })); return;
        }

        // Code correct — mark used, reset attempts
        await db.execute({ sql: 'UPDATE verify_codes SET used = 1 WHERE id = ?', args: [verifyRow.id] });
        await db.execute({
          sql: 'INSERT INTO pins (token, pin_hash, setup_attempts, setup_locked_until) VALUES (?,?,0,0) ON CONFLICT(token) DO UPDATE SET setup_attempts=0, setup_locked_until=0',
          args: [token, pinRow.rows[0]?.pin_hash || '']
        });

        console.log('[setup] Email verified for token:', token.slice(0,12) + '...');
        res.writeHead(200, cors); res.end(JSON.stringify({ success: true, verified: true }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── PIN: SET OR CHANGE ────────────────────────────────────
  if (url.pathname === '/pin/set' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, new_pin, current_pin } = data;
        if (!token || !new_pin) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing fields' })); return; }
        if (!/^\d{5}$/.test(new_pin)) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'PIN must be exactly 5 digits' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        // Verify token is valid
        const v = await db.execute({ sql: "SELECT * FROM vehicles WHERE token = ? AND status = 'active'", args: [token] });
        if (!v.rows[0]) { res.writeHead(403, cors); res.end(JSON.stringify({ error: 'Invalid token' })); return; }

        // Check if PIN exists
        const existing = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
        if (existing.rows[0]) {
          // Changing PIN — require current PIN
          if (!current_pin) { res.writeHead(403, cors); res.end(JSON.stringify({ error: 'Current PIN required to change PIN' })); return; }
          const currentHash = crypto.createHash('sha256').update(current_pin + token).digest('hex');
          if (existing.rows[0].pin_hash !== currentHash) { res.writeHead(403, cors); res.end(JSON.stringify({ error: 'Current PIN incorrect' })); return; }
        }

        const pin_hash = crypto.createHash('sha256').update(new_pin + token).digest('hex');
        await db.execute({
          sql: "INSERT INTO pins (token, pin_hash) VALUES (?,?) ON CONFLICT(token) DO UPDATE SET pin_hash=excluded.pin_hash, updated_at=strftime('%s','now')",
          args: [token, pin_hash]
        });

        console.log('[pin] PIN set/updated for token:', token.slice(0,12) + '...');
        res.writeHead(200, cors); res.end(JSON.stringify({ success: true }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── PIN: VERIFY ────────────────────────────────────────────
  if (url.pathname === '/pin/verify' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, pin } = data;
        if (!token || !pin) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing fields' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        const r = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
        if (!r.rows[0]) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'no_pin' })); return; }

        const pin_hash = crypto.createHash('sha256').update(pin + token).digest('hex');
        if (r.rows[0].pin_hash !== pin_hash) {
          res.writeHead(403, cors); res.end(JSON.stringify({ error: 'Incorrect PIN' })); return;
        }

        res.writeHead(200, cors); res.end(JSON.stringify({ success: true, completed_min_period: r.rows[0].completed_min_period }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── PIN: CHECK EXISTS (does this token have a PIN set?) ────
  if (url.pathname === '/pin/exists' && req.method === 'GET') {
    const token = url.searchParams.get('token');
    if (!token) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing token' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const r = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
      const row = r.rows[0];
      const pinSet = row && row.pin_hash && row.pin_hash.length > 0;
      const pinEmpty = row && (!row.pin_hash || row.pin_hash.length === 0); // row exists but no PIN set
      const locked = row?.setup_locked_until && Date.now() < row.setup_locked_until;
      const waitMinutes = locked ? Math.ceil((row.setup_locked_until - Date.now()) / 60000) : 0;
      res.writeHead(200, cors); res.end(JSON.stringify({ exists: pinSet, pin_empty: !!pinEmpty, locked: !!locked, wait_minutes: waitMinutes }));
    } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    return;
  }

  // ── PIN: RESET REQUEST (send email) ───────────────────────
  if (url.pathname === '/pin/reset' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token } = data;
        if (!token) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing token' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        const v = await db.execute({ sql: 'SELECT * FROM vehicles WHERE token = ?', args: [token] });
        const vehicle = v.rows[0];
        if (!vehicle) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Vehicle not found' })); return; }

        const resetCode = crypto.randomBytes(16).toString('hex');
        const resetExpires = Date.now() + (15 * 60 * 1000); // 15 minutes

        await db.execute({
          sql: "INSERT INTO pins (token, pin_hash, reset_code, reset_expires) VALUES (?,?,?,?) ON CONFLICT(token) DO UPDATE SET reset_code=excluded.reset_code, reset_expires=excluded.reset_expires",
          args: [token, '', resetCode, resetExpires]
        });

        const resetUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/control.html?token=' + token + '&reset=' + resetCode;
        const vehicleLabel = [vehicle.year, vehicle.make, vehicle.model].filter(Boolean).join(' ') || 'your vehicle';

        if (vehicle.owner_email) {
          await sendEmail({
            to: vehicle.owner_email,
            subject: 'TexTrack PIN reset — ' + vehicleLabel,
            html: `
              <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
                <div style="font-size:26px;font-weight:700;color:#e09020;letter-spacing:4px;margin-bottom:24px;">TexTrack</div>
                <h2 style="color:#fff;font-size:18px;margin-bottom:12px;">PIN Reset Request</h2>
                <p style="color:#aaa;line-height:1.7;margin-bottom:8px;">Someone requested a PIN reset for your TexTrack tracker on <strong style="color:#fff;">${vehicleLabel}</strong>.</p>
                <p style="color:#aaa;line-height:1.7;margin-bottom:24px;">If this was you, click below to set a new PIN. This link expires in 15 minutes.</p>
                <a href="${resetUrl}" style="display:block;background:#e09020;color:#000;text-align:center;padding:14px;border-radius:4px;font-weight:bold;font-size:15px;text-decoration:none;letter-spacing:2px;margin-bottom:16px;">RESET MY PIN →</a>
                <p style="font-size:11px;color:#555;text-align:center;">If you didn't request this, ignore this email. Your PIN has not been changed.</p>
              </div>`
          });
        }

        console.log('[pin] Reset email sent for token:', token.slice(0,12) + '...');
        res.writeHead(200, cors); res.end(JSON.stringify({ success: true, message: 'Check your email for a PIN reset link' }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── PIN: VALIDATE RESET CODE ──────────────────────────────
  if (url.pathname === '/pin/reset/validate' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, reset_code, new_pin } = data;
        if (!token || !reset_code || !new_pin) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing fields' })); return; }
        if (!/^\d{5}$/.test(new_pin)) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'PIN must be 5 digits' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        const r = await db.execute({ sql: 'SELECT * FROM pins WHERE token = ?', args: [token] });
        const pinRow = r.rows[0];
        if (!pinRow || pinRow.reset_code !== reset_code) { res.writeHead(403, cors); res.end(JSON.stringify({ error: 'Invalid reset code' })); return; }
        if (Date.now() > pinRow.reset_expires) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Reset link expired' })); return; }

        const pin_hash = crypto.createHash('sha256').update(new_pin + token).digest('hex');
        await db.execute({
          sql: "UPDATE pins SET pin_hash=?, reset_code=NULL, reset_expires=NULL, updated_at=strftime('%s','now') WHERE token=?",
          args: [pin_hash, token]
        });

        console.log('[pin] PIN reset complete for token:', token.slice(0,12) + '...');
        res.writeHead(200, cors); res.end(JSON.stringify({ success: true }));
      } catch(e) { res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message })); }
    });
    return;
  }

  // ── ACCOUNT: MAGIC LOGIN ──────────────────────────────────
  // Customer enters email → get magic link emailed to them
  if (url.pathname === '/account/login' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { email } = data;
        if (!email) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Email required' })); return; }
        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        // Check if this email has any vehicles (active, cancelled, or transferred)
        const r = await db.execute({
          sql: "SELECT COUNT(*) as cnt FROM vehicles WHERE owner_email = ? AND status != 'transferred'",
          args: [email.toLowerCase().trim()]
        });
        // We don't reveal whether they have an account — always say "check your email"
        // This prevents account enumeration

        // Generate magic token
        const token = crypto.randomBytes(24).toString('hex');
        const expiresAt = Date.now() + (15 * 60 * 1000); // 15 minutes

        await db.execute({
          sql: 'INSERT INTO magic_tokens (token, email, expires_at) VALUES (?,?,?)',
          args: [token, email.toLowerCase().trim(), expiresAt]
        });
        console.log('[account] Magic token created for:', email.toLowerCase().trim());

        const emailLower = email.toLowerCase().trim();
        const loginUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/account.html?token=' + token;

        // Send magic link email
        await sendEmail({
          to: emailLower,
          subject: 'Your TexTrack login link',
          html: `
            <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
              <div style="font-size:26px;font-weight:700;color:#e09020;letter-spacing:6px;margin-bottom:24px;">TEXTRACK</div>
              <h2 style="color:#fff;font-size:18px;margin-bottom:12px;">Your login link</h2>
              <p style="color:#aaa;line-height:1.7;margin-bottom:24px;">Click the button below to access your TexTrack account. This link expires in 15 minutes.</p>
              <a href="${loginUrl}" style="display:block;background:#e09020;color:#000;text-align:center;padding:14px;border-radius:4px;font-weight:bold;font-size:15px;text-decoration:none;letter-spacing:2px;margin-bottom:16px;">
                ACCESS MY ACCOUNT →
              </a>
              <p style="font-size:11px;color:#555;text-align:center;">If you didn't request this, ignore this email. Link expires in 15 minutes.</p>
            </div>
          `
        });

        console.log('[account] Magic link sent to:', email);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, message: 'Check your email for a login link' }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── ACCOUNT: VALIDATE MAGIC TOKEN + RETURN VEHICLES ───────
  if (url.pathname.startsWith('/account/') && req.method === 'GET') {
    const token = url.pathname.split('/account/')[1];
    if (!token || token === 'login') { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing token' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

      const r = await db.execute({ sql: 'SELECT * FROM magic_tokens WHERE token = ?', args: [token] });
      const magic = r.rows[0];
      if (!magic) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Invalid login link' })); return; }
      if (magic.used) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'already_used' })); return; }
      if (Date.now() > magic.expires_at) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'expired' })); return; }

      // Mark token used (one-time use)
      await db.execute({ sql: 'UPDATE magic_tokens SET used = 1 WHERE token = ?', args: [token] });

      // Get all vehicles for this email
      const vr = await db.execute({
        sql: "SELECT * FROM vehicles WHERE owner_email = ? AND status != 'transferred' ORDER BY install_date DESC",
        args: [magic.email]
      });

      // For each cancelled vehicle, check if there's an active reactivation code
      const vehicles = await Promise.all(vr.rows.map(async v => {
        let reactivate_url = null;
        if (v.status === 'cancelled') {
          // Check for existing unused reactivation code
          const rr = await db.execute({
            sql: 'SELECT code, expires_at FROM reactivations WHERE token = ? AND used = 0 AND expires_at > ? ORDER BY created_at DESC LIMIT 1',
            args: [v.token, Date.now()]
          });
          if (rr.rows[0]) {
            reactivate_url = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + rr.rows[0].code;
          } else {
            // Auto-generate a fresh reactivation code
            const reactCode = crypto.randomBytes(12).toString('hex');
            const reactExpiry = Date.now() + (30 * 24 * 60 * 60 * 1000);
            await db.execute({
              sql: 'INSERT OR IGNORE INTO reactivations (code, token, stripe_customer_id, email, vin, make, model, year, color, plate, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
              args: [reactCode, v.token, v.stripe_customer_id, v.owner_email, v.vin, v.make, v.model, v.year, v.color, v.plate, reactExpiry]
            });
            reactivate_url = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + reactCode;
          }
        }

        const control_url = v.status === 'active'
          ? (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/control.html?token=' + v.token
          : null;

        return {
          vin: v.vin, make: v.make, model: v.model, year: v.year,
          color: v.color, plate: v.plate, status: v.status,
          plan: v.plan, install_date: v.install_date,
          control_url, reactivate_url
        };
      }));

      // Issue a session token for this logged-in session (1 hour)
      const sessionToken = crypto.randomBytes(24).toString('hex');
      const sessionExpiry = Date.now() + (60 * 60 * 1000);
      await db.execute({
        sql: 'INSERT INTO magic_tokens (token, email, expires_at) VALUES (?,?,?)',
        args: [sessionToken, magic.email, sessionExpiry]
      });

      res.writeHead(200, cors);
      res.end(JSON.stringify({
        success: true,
        email: magic.email,
        session_token: sessionToken,
        session_expires: sessionExpiry,
        vehicles
      }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── ACCOUNT: SESSION REFRESH (stay logged in) ──────────────
  if (url.pathname === '/account/session' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { session_token, email } = data;
        if (!session_token || !email) { res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }

        const r = await db.execute({
          sql: 'SELECT * FROM magic_tokens WHERE token = ? AND email = ? AND used = 0',
          args: [session_token, email]
        });
        const session = r.rows[0];
        if (!session || Date.now() > session.expires_at) {
          res.writeHead(401, cors); res.end(JSON.stringify({ error: 'Session expired' })); return;
        }

        // Get vehicles
        const vr = await db.execute({
          sql: "SELECT * FROM vehicles WHERE owner_email = ? AND status != 'transferred' ORDER BY install_date DESC",
          args: [email]
        });

        const vehicles = await Promise.all(vr.rows.map(async v => {
          let reactivate_url = null;
          if (v.status === 'cancelled') {
            const rr = await db.execute({
              sql: 'SELECT code FROM reactivations WHERE token = ? AND used = 0 AND expires_at > ? ORDER BY created_at DESC LIMIT 1',
              args: [v.token, Date.now()]
            });
            if (rr.rows[0]) {
              reactivate_url = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + rr.rows[0].code;
            } else {
              const reactCode = crypto.randomBytes(12).toString('hex');
              const reactExpiry = Date.now() + (30 * 24 * 60 * 60 * 1000);
              await db.execute({
                sql: 'INSERT OR IGNORE INTO reactivations (code, token, stripe_customer_id, email, vin, make, model, year, color, plate, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                args: [reactCode, v.token, v.stripe_customer_id, v.owner_email, v.vin, v.make, v.model, v.year, v.color, v.plate, reactExpiry]
              });
              reactivate_url = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + reactCode;
            }
          }
          const control_url = v.status === 'active'
            ? (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/control.html?token=' + v.token
            : null;
          return {
            vin: v.vin, make: v.make, model: v.model, year: v.year,
            color: v.color, plate: v.plate, status: v.status,
            plan: v.plan, install_date: v.install_date,
            control_url, reactivate_url
          };
        }));

        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, email, vehicles }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── REACTIVATION: INITIATE (admin or auto-triggered) ─────
  if (url.pathname === '/reactivate/initiate' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { token, customer_id } = data;
        if (!token && !customer_id) {
          res.writeHead(400, cors); res.end(JSON.stringify({ error: 'token or customer_id required' })); return;
        }

        if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }

        // Look up cancelled vehicle
        let vehicle;
        if (token) {
          const r = await db.execute({ sql: "SELECT * FROM vehicles WHERE token = ? AND status = 'cancelled' ORDER BY created_at DESC LIMIT 1", args: [token] });
          vehicle = r.rows[0];
        } else {
          const r = await db.execute({ sql: "SELECT * FROM vehicles WHERE stripe_customer_id = ? AND status = 'cancelled' ORDER BY created_at DESC LIMIT 1", args: [customer_id] });
          vehicle = r.rows[0];
        }

        if (!vehicle) {
          res.writeHead(404, cors); res.end(JSON.stringify({ error: 'No cancelled vehicle found' })); return;
        }

        // Generate reactivation code
        const code = crypto.randomBytes(12).toString('hex');
        const expiresAt = Date.now() + (7 * 24 * 60 * 60 * 1000); // 7 days

        await db.execute({
          sql: 'INSERT INTO reactivations (code, token, stripe_customer_id, email, vin, make, model, year, color, plate, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
          args: [code, vehicle.token, vehicle.stripe_customer_id, vehicle.owner_email, vehicle.vin, vehicle.make, vehicle.model, vehicle.year, vehicle.color, vehicle.plate, expiresAt]
        });

        const reactivateUrl = (process.env.SITE_URL || 'https://alkdiosp34.github.io/tantracker3000-pro') + '/reactivate.html?code=' + code;
        console.log('[reactivate] Link generated for VIN:', vehicle.vin, '| Customer:', vehicle.stripe_customer_id);

        // Send email if requested (admin-triggered or auto)
        let emailSent = false;
        if ((data.send_email || data.auto) && vehicle.owner_email) {
          const vehicleLabel = [vehicle.year, vehicle.make, vehicle.model].filter(Boolean).join(' ') || 'your vehicle';
          emailSent = await sendEmail({
            to: vehicle.owner_email,
            subject: 'Reactivate your TexTrack service — ' + vehicleLabel,
            html: `
              <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;background:#0a0a0a;color:#ccc;padding:32px;border-radius:8px;">
                <div style="font-family:Arial,sans-serif;font-size:28px;font-weight:700;color:#e09020;letter-spacing:6px;margin-bottom:4px;">TEXTRACK</div>
                <div style="font-size:10px;color:#666;letter-spacing:3px;margin-bottom:24px;">VEHICLE PROTECTION</div>
                <h2 style="color:#fff;font-size:20px;margin-bottom:12px;">We miss you — come back for $22</h2>
                <p style="color:#aaa;line-height:1.7;margin-bottom:16px;">
                  Your TexTrack service for your <strong style="color:#fff;">${vehicleLabel}</strong> has been cancelled.
                  The good news: your device is still installed and ready to go.
                </p>
                <div style="background:#111;border:1px solid #222;border-radius:6px;padding:16px;margin-bottom:24px;">
                  <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1a1a1a;font-size:13px;">
                    <span style="color:#666;">Installation fee</span>
                    <span><span style="text-decoration:line-through;color:#444;">$99.00</span> <span style="color:#00cc66;font-weight:bold;">FREE</span></span>
                  </div>
                  <div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1a1a1a;font-size:13px;">
                    <span style="color:#666;">First month</span>
                    <span style="color:#fff;">$22.00</span>
                  </div>
                  <div style="display:flex;justify-content:space-between;padding:10px 0 0;font-size:16px;font-weight:bold;">
                    <span style="color:#fff;">Due today</span>
                    <span style="color:#e09020;">$22.00</span>
                  </div>
                </div>
                <a href="${reactivateUrl}" style="display:block;background:#e09020;color:#000;text-align:center;padding:16px;border-radius:4px;font-weight:bold;font-size:16px;text-decoration:none;letter-spacing:2px;margin-bottom:16px;">
                  REACTIVATE FOR $22 →
                </a>
                <p style="font-size:11px;color:#555;text-align:center;line-height:1.6;">
                  This link expires in 30 days.<br>
                  No installation visit required — your device is already in the vehicle.<br>
                  Questions? Reply to this email or text us directly.
                </p>
              </div>
            `
          });
        }

        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, code, reactivate_url: reactivateUrl, email: vehicle.owner_email, expires_at: expiresAt, email_sent: emailSent }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── REACTIVATION: GET INFO ─────────────────────────────────
  if (url.pathname.startsWith('/reactivate/') && req.method === 'GET') {
    const code = url.pathname.split('/reactivate/')[1];
    if (!code) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing code' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const r = await db.execute({ sql: 'SELECT * FROM reactivations WHERE code = ?', args: [code] });
      const reactivation = r.rows[0];
      if (!reactivation) { res.writeHead(404, cors); res.end(JSON.stringify({ error: 'Reactivation link not found' })); return; }
      if (reactivation.used) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'already_used' })); return; }
      if (Date.now() > reactivation.expires_at) { res.writeHead(410, cors); res.end(JSON.stringify({ error: 'expired' })); return; }

      res.writeHead(200, cors);
      res.end(JSON.stringify({
        valid: true,
        email: reactivation.email,
        vin: reactivation.vin,
        make: reactivation.make,
        model: reactivation.model,
        year: reactivation.year,
        color: reactivation.color,
        plate: reactivation.plate,
        expires_at: reactivation.expires_at
      }));
    } catch(e) {
      res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── REACTIVATION: PAY (generate Stripe payment link) ──────
  if (url.pathname === '/reactivate/pay' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const data = JSON.parse(body);
        const { code } = data;
        if (!code) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing code' })); return; }

        const r = await db.execute({ sql: 'SELECT * FROM reactivations WHERE code = ? AND used = 0', args: [code] });
        const reactivation = r.rows[0];
        if (!reactivation || Date.now() > reactivation.expires_at) {
          res.writeHead(410, cors); res.end(JSON.stringify({ error: 'Invalid or expired' })); return;
        }

        // Create Stripe payment link — monthly only, no install fee
        const link = await stripe.paymentLinks.create({
          line_items: [{ price: process.env.STRIPE_SINGLE_PRICE_ID, quantity: 1 }],
          phone_number_collection: { enabled: true },
          metadata: {
            reactivation_code: code,
            reactivation_token: reactivation.token,
            reactivation_customer_id: reactivation.stripe_customer_id
          },
          after_completion: {
            type: 'redirect',
            redirect: { url: (process.env.WELCOME_URL || 'https://alkdiosp34.github.io/tantracker3000-pro/welcome.html') + '?reactivated=1' }
          }
        });

        const stripe_url = link.url + '?prefilled_email=' + encodeURIComponent(reactivation.email || '');
        console.log('[reactivate] Payment link generated for', reactivation.vin);
        res.writeHead(200, cors);
        res.end(JSON.stringify({ success: true, stripe_url }));
      } catch(e) {
        res.writeHead(500, cors); res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // ── CUSTOMER VEHICLES ─────────────────────────────────────
  if (url.pathname === '/customer-vehicles' && req.method === 'GET') {
    const customerId = url.searchParams.get('customer_id');
    if (!customerId) { res.writeHead(400, cors); res.end(JSON.stringify({ error: 'Missing customer_id' })); return; }
    try {
      if (!db) { res.writeHead(503, cors); res.end(JSON.stringify({ error: 'DB unavailable' })); return; }
      const r = await db.execute({
        sql: "SELECT token, vin, make, model, year, color, plate, owner_name, plan, status FROM vehicles WHERE stripe_customer_id = ? AND status = 'active' ORDER BY install_date ASC",
        args: [customerId]
      });
      res.writeHead(200, cors);
      res.end(JSON.stringify({ vehicles: r.rows }));
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
