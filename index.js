import http from 'node:http';
import WebSocket from 'ws';
import { XMLParser } from 'fast-xml-parser';

// ============================================================================
// Configuration (Set via Render Environment Variables)
// ============================================================================
const CONFIG = {
  port: Number(process.env.PORT || 10000),
  baseUrl: (process.env.PLAYFIVE_BASE_URL || 'http://www.playfive.net').replace(/\/+$/, ''),
  wsUrl: process.env.PLAYFIVE_WS_URL || 'wss://jabb.im/websocket',
  xmppDomain: 'anon.jabb.im',
  mucPassword: process.env.PLAYFIVE_MUC_PASSWORD || 'q58L6soWA60f',

  // Required registered Playfive account credentials
  username: (process.env.PLAYFIVE_USERNAME || '').trim(),
  password: process.env.PLAYFIVE_PASSWORD || '',

  // Discord webhooks
  webhookChat: (process.env.DISCORD_WEBHOOK_CHAT || '').trim(),
  webhookTable: (process.env.DISCORD_WEBHOOK_TABLE || '').trim(),
  // Optional: separate webhook for the first (non-ping) table notice; defaults to webhookTable
  webhookTableEarly: (process.env.DISCORD_WEBHOOK_TABLE || '').trim(), //DISCORD_WEBHOOK_TABLE_EARLY kdyby se lišil

  // Optional: Discord Role IDs (digits only) to ping safely
  chatPingRoleId: (process.env.DISCORD_CHAT_PING_ROLE_ID || '1573748305492574382').trim(),
  tablePingRoleId: (process.env.DISCORD_TABLE_PING_ROLE_ID || '1573748305492574382').trim(),
  tableEarlyPingRoleId: (process.env.DISCORD_TABLE_EARLY_PING_ROLE_ID || '').trim(),


  

  // Main chat monitor settings
  chatTrigger: (process.env.CHAT_TRIGGER || 'GCnotifpls')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase(),
  chatCooldownMs: Number(process.env.CHAT_COOLDOWN_SECONDS || 900) * 1000,
  ignoreSelfChat: (process.env.IGNORE_SELF_CHAT || 'false').toLowerCase() === 'true',
  ignoreGuestChat: (process.env.IGNORE_GUEST_CHAT || 'true').toLowerCase() === 'true',
  

  // Solo waiting table monitor settings
  earlyWaitThresholdMs: Number(process.env.EARLY_WAIT_THRESHOLD_SECONDS || 30) * 1000, // 30s -> first (non-ping) notice
  pingMessageTimeDelayIncrementMs:
    Number(process.env.PING_MESSAGE_TIME_DELAY_INCREMENT || 180) * 1000, // 180s after first notice -> ping notice
  tableUserCooldownMs: Number(process.env.TABLE_COOLDOWN_SECONDS || 900) * 1000, // 15m per nick (triggered by first notice)
  tableBruteCooldownMs: Number(process.env.TABLE_BRUTE_COOLDOWN_HOURS || 5) * 3600 * 1000, // 5h AFK cooldown (0 = disabled)

  
  ignoredNicks: new Set(
    (process.env.IGNORED_NICKS || 'xobot')
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean)
  ),
  includeTeaching: (process.env.INCLUDE_TEACHING || 'false').toLowerCase() === 'true',

  // Backoff schedule (minutes) when Playfive is offline/shut down.
  // Default: 5m -> 15m -> 30m -> 60m. Set to "60" for strict 1-hour checks.
  reconnectBackoffMinutes: (process.env.RECONNECT_BACKOFF_MINUTES || '5,15,30,60')
    .split(',')
    .map((n) => Math.max(1, Number(n.trim())))
    .filter((n) => !Number.isNaN(n)),

  debugXmpp: (process.env.DEBUG_XMPP || 'false').toLowerCase() === 'true',
};

if (!CONFIG.username || !CONFIG.password) {
  console.error('[FATAL] PLAYFIVE_USERNAME and PLAYFIVE_PASSWORD must be set in Environment Variables.');
  process.exit(1);
}
if (!CONFIG.webhookChat || !CONFIG.webhookTable) {
  console.warn('[WARN] One or both Discord webhook URLs are empty. Missing webhooks will be skipped.');
}
if (CONFIG.reconnectBackoffMinutes.length === 0) {
  CONFIG.reconnectBackoffMinutes = [60];
}

// Never track the bot's own account at a table
CONFIG.ignoredNicks.add(CONFIG.username.toLowerCase());

const GAME_TYPES = {
  11: 'Piškvorky - klasika',
  12: 'Piškvorky - swap2',
  21: 'Renju - swap2',
  22: 'Renju - Soosyrv-8',
  23: 'Renju - Taraguchi',
  31: 'Pente',
  41: 'Teaching',
};

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  removeNSPrefix: true,
  parseTagValue: false,
  parseAttributeValue: false,
  trimValues: false,
  isArray: (tagName) => ['status', 'mechanism', 'x'].includes(tagName),
});

class AuthError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AuthError';
  }
}

// ============================================================================
// Runtime State
// ============================================================================
const state = {
  status: 'STARTING', // STARTING | ONLINE | OFFLINE_WAIT
  nick: null,
  activeOrigin: CONFIG.baseUrl,
  startedAt: Date.now(),
  connectedAt: null,
  nextRetryAt: null,
  offlineStreak: 0,
  lastError: null,
  lastChatAlertAt: 0,
  // Map<number, TableInfo>
  tables: new Map(),
  
  // Map<string, { firstSeen: number, firstNotifiedAt: number|null, pingNotified: boolean, suppressedLogged: boolean, table: TableInfo }>
  waitingSolo: new Map(),
  // Map<string, { lastFirstAlertAt: number, recentPingAlerts: number[], bruteCooldownUntil: number }>
  tableAlertStateByNick: new Map(),
  
};

let activeWs = null;
let shuttingDown = false;
let evaluatingTables = false;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// HTTP Keep-Alive Server (Render + UptimeRobot)
// ============================================================================
const server = http.createServer((req, res) => {
  const path = (req.url || '/').split('?')[0];
  if (path === '/' || path === '/health') {
    const payload = JSON.stringify({
      ok: true,
      status: state.status,
      nick: state.nick,
      uptimeSeconds: Math.floor((Date.now() - state.startedAt) / 1000),
      connectedSince: state.connectedAt ? new Date(state.connectedAt).toISOString() : null,
      activeTables: state.tables.size,
      trackedSoloTables: state.waitingSolo.size,
      nextRetryAt: state.nextRetryAt ? new Date(state.nextRetryAt).toISOString() : null,
      lastError: state.lastError,
      memoryMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    });

    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    });
    if (req.method === 'HEAD') {
      res.end();
    } else {
      res.end(payload);
    }
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

server.listen(CONFIG.port, '0.0.0.0', () => {
  console.log(`[HTTP] Health server listening on port ${CONFIG.port} (account=${CONFIG.username})`);
});

// ============================================================================
// Discord Webhooks (Safe Mentions + Markdown Escape + 429 Retry)
// ============================================================================
function escapeDiscordMd(str) {
  return String(str).replace(/([\\*_~`|>\[\]])/g, '\\$1');
}

async function sendDiscordWebhook(url, { content = '', embeds = [], roleId = '' }) {
  if (!url) {
    console.warn('[DISCORD] Webhook URL not configured; skipping.');
    return false;
  }

  const pingPrefix = roleId ? `-# <@&${roleId}> ` : '';
  const body = {
    content: `${pingPrefix}${content}`.trim() || undefined,
    embeds,
    // Strictly prevent Playfive users from triggering @everyone or @here
    allowed_mentions: {
      parse: [],
      roles: roleId ? [roleId] : [],
    },
  };

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (res.ok) return true;

      if (res.status === 429) {
        const data = await res.json().catch(() => ({}));
        const waitMs = Math.ceil((data.retry_after || 2) * 1000);
        console.warn(`[DISCORD] Rate-limited (429). Retrying in ${waitMs}ms...`);
        await sleep(waitMs);
        continue;
      }

      const errText = await res.text().catch(() => '');
      console.error(`[DISCORD] HTTP ${res.status}: ${errText}`);
      if (res.status >= 500 && attempt < 3) {
        await sleep(2000 * attempt);
        continue;
      }
      return false;
    } catch (err) {
      console.error(`[DISCORD] Network error (attempt ${attempt}):`, err.message);
      if (attempt < 3) await sleep(2000 * attempt);
    }
  }
  return false;
}

async function notifyChatTrigger(nick, text) {
  const hernaLink = `${state.activeOrigin}/cz/herna`;
  const pragueTime = new Date().toLocaleTimeString('cs-CZ', {
    timeZone: 'Europe/Prague',
    hour: '2-digit',
    minute: '2-digit',
  });

  const safeNick = escapeDiscordMd(nick);
  const safeText = escapeDiscordMd(text.slice(0, 500));

  console.log(`[ALERT:CHAT] ${nick}: ${text}`);
  await sendDiscordWebhook(CONFIG.webhookChat, {
    roleId: CONFIG.chatPingRoleId,
    embeds: [
      {
        //pragueTime
        title: 'Somebody wants you to wake up!',
        description: `- **WHO:**⠀ ${safeNick}\n- **MESSAGE:**⠀ ${safeText}\n⠀\n-# [Join Gameroom](${hernaLink})`,
        color: 1,
      },
    ],
  });
}

async function notifyEarlyWaitingTable(table, waitedSeconds) {
  const hernaLink = `${state.activeOrigin}/cz/herna`;
  const ratedLabel = table.isRated ? '🔴 Rated 🔴' : '🟢 Friendly 🟢';

  console.log(
    `[ALERT:TABLE:FIRST] #${table.id} | ${table.soloNick} | ${table.gamestyle} | ${table.timeControl} | ${ratedLabel} (${waitedSeconds}s)`
  );

  return sendDiscordWebhook(CONFIG.webhookTableEarly, {
    roleId: CONFIG.tableEarlyPingRoleId, // Empty by default -> no ping
    embeds: [
      {
        title: 'Somebody is looking for a game!',
        color: 0x95a5a6,
        description: `- **WHO:**⠀ ${escapeDiscordMd(table.soloNick)}\n- **GAME TYPE:**⠀ ${table.gamestyle}\n- **TIME+FISCHER:**⠀ ${table.timeControl}\n- **MODE:**⠀ ${ratedLabel}\n⠀\n-# [Join Gameroom](${hernaLink})`,
      },
    ],
  });
}

async function notifyWaitingTable(table, waitedSeconds) {
  const hernaLink = `${state.activeOrigin}/cz/herna`;
  const waitedMin = Math.floor(waitedSeconds / 60);
  const ratedLabel = table.isRated ? '🔴 Rated 🔴' : '🟢 Friendly 🟢';
  const color = table.isRated ? 0xe74c3c : 0x2ecc71;

  console.log(
    `[ALERT:TABLE:PING] #${table.id} | ${table.soloNick} | ${table.gamestyle} | ${table.timeControl} | ${ratedLabel} (${waitedSeconds}s)`
  );

  return sendDiscordWebhook(CONFIG.webhookTable, {
    roleId: CONFIG.tablePingRoleId,
    embeds: [
      {
        title: 'Somebody is waiting desperately!',
        // ${table.id}
        color: 16777215,
        description: `- **WHO:**⠀ ${escapeDiscordMd(table.soloNick)}\n- **GAME TYPE:**⠀ ${table.gamestyle}\n- **TIME+FISCHER:**⠀ ${table.timeControl}\n- **MODE:**⠀ ${ratedLabel}\n⠀\n-# [Join Gameroom](${hernaLink})`,
        /*fields: [
          { name: 'Nick: ', value: `****`, inline: true },
          { name: 'Game Type: ', value: , inline: true },
          { name: 'Time + Fischer: ', value: , inline: false },
          { name: 'Mode: ', value: , inline: true },
          //${waitedMin}
          { name: '', value: `[Join Gameroom]()`, inline: true },
        ],*/
      },
    ],
  });
}

// ============================================================================
// Pure HTTP Login for Registered Account (Zero Browser)
// ============================================================================
function updateCookieJar(jar, res) {
  const setCookies =
    typeof res.headers.getSetCookie === 'function'
      ? res.headers.getSetCookie()
      : (res.headers.get('set-cookie') || '').split(/,(?=\s*[^;,\s]+=)/).filter(Boolean);

  for (const raw of setCookies) {
    const parts = raw.split(';').map((p) => p.trim());
    const pair = parts[0];
    const eq = pair.indexOf('=');
    if (eq <= 0) continue;

    const name = pair.slice(0, eq).trim();
    const value = pair.slice(eq + 1).trim();
    const attrsLower = parts.slice(1).join(';').toLowerCase();

    if (value === 'deleted' || /max-age\s*=\s*0\b/.test(attrsLower)) {
      jar.delete(name);
    } else {
      jar.set(name, value);
    }
  }
}

function serializeCookies(jar) {
  return [...jar.entries()].map(([k, v]) => `${k}=${v}`).join('; ');
}

async function fetchWithCookies(url, options, jar, maxRedirects = 5) {
  let currentUrl = url;
  let currentOpts = { ...options };

  for (let i = 0; i <= maxRedirects; i++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20_000);
    try {
      const headers = new Headers(currentOpts.headers || {});
      headers.set('User-Agent', 'Mozilla/5.0 (X11; Linux x86_64) PlayfiveMonitorBot/2.1');
      const cookieHeader = serializeCookies(jar);
      if (cookieHeader) headers.set('Cookie', cookieHeader);

      const res = await fetch(currentUrl, {
        ...currentOpts,
        headers,
        redirect: 'manual',
        signal: controller.signal,
      });

      updateCookieJar(jar, res);

      if (res.status >= 300 && res.status < 400 && res.headers.get('location')) {
        currentUrl = new URL(res.headers.get('location'), currentUrl).toString();
        if (res.status === 307 || res.status === 308) {
          // Preserve method & body
        } else {
          currentOpts = { method: 'GET' };
        }
        continue;
      }

      return { res, finalUrl: currentUrl };
    } finally {
      clearTimeout(timer);
    }
  }
  throw new Error('Too many HTTP redirects while contacting Playfive.');
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#0*39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function parseRegisteredLoginForm(html, pageUrl) {
  const formMatches = html.matchAll(/<form\b([^>]*)>([\s\S]*?)<\/form>/gi);
  for (const match of formMatches) {
    const formAttrs = match[1];
    const formBody = match[2];
    if (!formBody.includes('feu_input_username')) continue;

    const actionMatch = formAttrs.match(/action\s*=\s*["']([^"']*)["']/i);
    const actionUrl = new URL(
      decodeHtmlEntities(actionMatch ? actionMatch[1] : '/'),
      pageUrl
    ).toString();

    const params = new URLSearchParams();
    const inputMatches = formBody.matchAll(/<input\b([^>]*)>/gi);
    let submitAdded = false;

    for (const inp of inputMatches) {
      const attrs = inp[1];
      const nameMatch = attrs.match(/\bname\s*=\s*["']([^"']+)["']/i);
      if (!nameMatch) continue;

      const name = decodeHtmlEntities(nameMatch[1]);
      const typeMatch = attrs.match(/\btype\s*=\s*["']([^"']+)["']/i);
      const type = (typeMatch ? typeMatch[1] : 'text').toLowerCase();
      const valMatch = attrs.match(/\bvalue\s*=\s*["']([^"']*)["']/i);
      const value = valMatch ? decodeHtmlEntities(valMatch[1]) : '';

      if (type === 'button' || type === 'reset' || type === 'image') continue;
      if ((type === 'checkbox' || type === 'radio') && !/\bchecked\b/i.test(attrs)) continue;
      if (type === 'submit') {
        if (submitAdded) continue;
        submitAdded = true;
      }

      if (name.endsWith('feu_input_username')) {
        params.set(name, CONFIG.username);
      } else if (name.endsWith('feu_input_password')) {
        params.set(name, CONFIG.password);
      } else if (name.endsWith('feu__data')) {
        // Hidden honeypot field in Playfive's FEU form must remain empty
        params.set(name, '');
      } else {
        params.set(name, value);
      }
    }

    return { actionUrl, params };
  }
  throw new Error('Could not find registered login form on Playfive homepage (site may be in maintenance).');
}

function parsePepaFromHernaHtml(html) {
  const match = html.match(/var\s+Pepa\s*=\s*\{([\s\S]*?)\}/i);
  if (!match) {
    throw new Error('Could not find `var Pepa` in /cz/herna HTML (session expired or site down).');
  }
  const block = match[1];

  const getField = (key) => {
    const m = block.match(new RegExp(`(?:^|[,{\\s])${key}\\s*:\\s*(['"])([\\s\\S]*?)\\1`));
    return m ? m[2] : '';
  };

  const token = getField('a');
  const nick = getField('d');
  const extra = getField('e');
  const room = getField('room');

  if (!token) {
    throw new Error('`Pepa.a` auth token is empty in /cz/herna.');
  }
  return { a: token, d: nick, e: extra, room };
}

async function loginAndFetchSession() {
  const jar = new Map();
  const homeUrl = `${CONFIG.baseUrl}/`;

  console.log(`[HTTP] Fetching Playfive homepage (${homeUrl})...`);
  const { res: homeRes, finalUrl: homeFinalUrl } = await fetchWithCookies(homeUrl, { method: 'GET' }, jar);
  if (!homeRes.ok) {
    throw new Error(`Playfive homepage returned HTTP ${homeRes.status}`);
  }
  const homeHtml = await homeRes.text();

  const { actionUrl, params } = parseRegisteredLoginForm(homeHtml, homeFinalUrl);

  console.log(`[HTTP] Logging in as registered user "${CONFIG.username}"...`);
  const { res: loginRes, finalUrl: loginFinalUrl } = await fetchWithCookies(
    actionUrl,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        Referer: homeFinalUrl,
      },
      body: params.toString(),
    },
    jar
  );

  if (!loginRes.ok) {
    throw new Error(`Playfive login POST returned HTTP ${loginRes.status}`);
  }
  const loginHtml = await loginRes.text();

  if (!/id\s*=\s*["']loggedUser["']/i.test(loginHtml)) {
    throw new AuthError(
      'Registered login failed (#loggedUser not found). Double-check PLAYFIVE_USERNAME and PLAYFIVE_PASSWORD.'
    );
  }

  // Follow the exact origin that accepted our login cookie
  const activeOrigin = new URL(loginFinalUrl).origin;
  state.activeOrigin = activeOrigin;
  const hernaUrl = `${activeOrigin}/cz/herna`;

  console.log(`[HTTP] Entering Herna (${hernaUrl})...`);
  const { res: hernaRes } = await fetchWithCookies(
    hernaUrl,
    { method: 'GET', headers: { Referer: loginFinalUrl } },
    jar
  );
  if (!hernaRes.ok) {
    throw new Error(`Playfive /cz/herna returned HTTP ${hernaRes.status}`);
  }
  const hernaHtml = await hernaRes.text();
  const pepa = parsePepaFromHernaHtml(hernaHtml);

  if (pepa.d.toLowerCase() !== CONFIG.username.toLowerCase()) {
    throw new AuthError(
      `Expected registered nick "${CONFIG.username}", but /cz/herna assigned "${pepa.d}".`
    );
  }

  return pepa;
}

// ============================================================================
// Table State & Solo-Wait Timer Logic
// ============================================================================
function upsertTableFromFields(f) {
  // Format: [cmd, id, owner, rated, gameTime, fischer, p1, p2, gameType, undo, priv]
  if (f.length < 11) return;

  const id = parseInt(f[1], 10);
  if (Number.isNaN(id)) return;

  const p1 = (f[6] || '').trim();
  const p2 = (f[7] || '').trim();
  const gameType = parseInt(f[8], 10);
  const isPrivate = f[10] === '1';

  // Mirror Playfive's own rule: tables with any Guest player are forced friendly (přátelská)
  let isRated = f[3] === '1';
  if (isRated && (p1.startsWith('Guest') || p2.startsWith('Guest'))) {
    isRated = false;
  }

  const timeMin = (f[4] || '0').trim();
  const fischerSec = (f[5] || '0').trim();

  state.tables.set(id, {
    id,
    owner: (f[2] || '').trim(),
    isRated,
    timeControl: `${timeMin}m+${fischerSec}s`,
    p1,
    p2,
    gameType,
    gamestyle: GAME_TYPES[gameType] || `Typ ${gameType}`,
    isPrivate,
  });
}

function setSeatOnTable(tableId, seatNum, nick) {
  const t = state.tables.get(tableId);
  if (!t) return;
  if (seatNum === 1 && !t.p1) {
    t.p1 = nick;
    if (t.isRated && nick.startsWith('Guest')) t.isRated = false;
  } else if (seatNum === 2 && !t.p2) {
    t.p2 = nick;
    if (t.isRated && nick.startsWith('Guest')) t.isRated = false;
  }
}

function clearSeatOnTable(tableId, seatNum, expectedNick) {
  const t = state.tables.get(tableId);
  if (!t) return;
  if (seatNum === 1 && (!expectedNick || t.p1 === expectedNick)) t.p1 = '';
  if (seatNum === 2 && (!expectedNick || t.p2 === expectedNick)) t.p2 = '';
}

function removePlayerFromTable(tableId, nick) {
  const t = state.tables.get(tableId);
  if (!t) return;
  if (t.p1 === nick) t.p1 = '';
  if (t.p2 === nick) t.p2 = '';
}

function removePlayerFromAllTables(nick) {
  for (const t of state.tables.values()) {
    if (t.p1 === nick) t.p1 = '';
    if (t.p2 === nick) t.p2 = '';
  }
}

function isIgnoredNick(nick) {
  const lower = nick.toLowerCase();
  return CONFIG.ignoredNicks.has(lower) || lower.startsWith('xobot');
}

async function evaluateWaitingTables() {
  if (state.status !== 'ONLINE' || evaluatingTables) return;
  evaluatingTables = true;

  try {
    const now = Date.now();
    const bruteWindowMs = CONFIG.tableUserCooldownMs * 4;
    const activeSoloKeys = new Set();

    const getNickRecord = (soloLower) => {
      let rec = state.tableAlertStateByNick.get(soloLower);
      if (!rec) {
        rec = { lastFirstAlertAt: 0, recentPingAlerts: [], bruteCooldownUntil: 0 };
        state.tableAlertStateByNick.set(soloLower, rec);
      }
      return rec;
    };

    for (const table of state.tables.values()) {
      if (table.isPrivate) continue; // Ignore soukromý tables
      if (!CONFIG.includeTeaching && table.gameType === 41) continue;

      const seated = [table.p1, table.p2].filter((n) => n.length > 0);
      if (seated.length !== 1) continue; // Must be sitting alone

      const soloNick = seated[0];
      if (isIgnoredNick(soloNick)) continue; // Ignore xobot & bot account
      if (state.nick && soloNick.toLowerCase() === state.nick.toLowerCase()) continue;

      const soloLower = soloNick.toLowerCase();
      const key = `${table.id}::${soloLower}`;
      activeSoloKeys.add(key);

      const snapshot = { ...table, soloNick };
      let entry = state.waitingSolo.get(key);

      if (!entry) {
        entry = {
          firstSeen: now,
          firstNotifiedAt: null, // null = not sent yet, >0 = sent timestamp, -1 = permanently skipped for this sit
          pingNotified: false,
          suppressedLogged: false,
          table: snapshot,
        };
        state.waitingSolo.set(key, entry);
        console.log(
          `[TABLE] Tracking solo player: #${table.id} ${soloNick} (${table.gamestyle}, ${table.timeControl}, ${table.isRated ? 'bodovaná' : 'přátelská'})`
        );
      } else {
        // Keep metadata fresh in case player changes time/gamestyle/rated while waiting
        entry.table = snapshot;
      }

      const waitedMs = now - entry.firstSeen;

      // Stage 1: First (non-ping) notification after earlyWaitThresholdMs (default 30s)
      if (entry.firstNotifiedAt === null && waitedMs >= CONFIG.earlyWaitThresholdMs) {
        const rec = getNickRecord(soloLower);

        // 1a. Active 5-hour brute cooldown -> mute this entire sit so it won't fire 5h later
        if (now < rec.bruteCooldownUntil) {
          const minsLeft = Math.ceil((rec.bruteCooldownUntil - now) / 60_000);
          console.log(
            `[TABLE] First alert for ${soloNick} (#${table.id}) suppressed by brute cooldown (${minsLeft}m left).`
          );
          entry.firstNotifiedAt = -1;
          continue;
        }

        // 1b. Normal per-user cooldown -> wait until cooldown expires if they remain seated
        if (now - rec.lastFirstAlertAt < CONFIG.tableUserCooldownMs) {
          if (!entry.suppressedLogged) {
            console.log(
              `[TABLE] First alert for ${soloNick} (#${table.id}) suppressed by per-user cooldown.`
            );
            entry.suppressedLogged = true;
          }
          continue;
        }

        // 1c. Send first (non-ping) notification; only start cooldown + ping timer if Discord accepted it
        const sent = await notifyEarlyWaitingTable(entry.table, Math.floor(waitedMs / 1000));
        if (state.status !== 'ONLINE') return;

        if (sent) {
          const sentAt = Date.now();
          rec.lastFirstAlertAt = sentAt;
          entry.firstNotifiedAt = sentAt;
        } else {
          entry.firstNotifiedAt = -1;
          console.warn(
            `[TABLE] First alert for ${soloNick} (#${table.id}) failed to deliver; ping alert skipped for this sit.`
          );
        }
        continue;
      }

      // Stage 2: Second (ping) notification after pingMessageTimeDelayIncrementMs (default 180s)
      // Only fires if the first notification WAS sent and the player is still sitting alone here.
      if (
        typeof entry.firstNotifiedAt === 'number' &&
        entry.firstNotifiedAt > 0 &&
        !entry.pingNotified &&
        now - entry.firstNotifiedAt >= CONFIG.pingMessageTimeDelayIncrementMs
      ) {
        const rec = getNickRecord(soloLower);

        // Defensive check in case brute cooldown is already active
        if (now < rec.bruteCooldownUntil) {
          entry.pingNotified = true;
          continue;
        }

        // Brute cooldown trigger: instead of sending a 3rd ping alert within cooldown * 4
        rec.recentPingAlerts = rec.recentPingAlerts.filter((ts) => now - ts <= bruteWindowMs);

        if (CONFIG.tableBruteCooldownMs > 0 && rec.recentPingAlerts.length >= 2) {
          rec.bruteCooldownUntil = now + CONFIG.tableBruteCooldownMs;
          rec.recentPingAlerts = [];
          entry.pingNotified = true;
          console.log(
            `[TABLE] Brute cooldown (${(CONFIG.tableBruteCooldownMs / 3600_000).toFixed(1)}h) triggered for ${soloNick} (#${table.id}) instead of 3rd ping alert within ${Math.round(bruteWindowMs / 60_000)}m.`
          );
          continue;
        }

        entry.pingNotified = true;
        const sent = await notifyWaitingTable(entry.table, Math.floor(waitedMs / 1000));
        if (state.status !== 'ONLINE') return;

        if (sent) {
          rec.recentPingAlerts.push(Date.now());
        }
      }
    }

    // Stop tracking tables that closed, turned soukromý, emptied, or gained an opponent
    for (const key of state.waitingSolo.keys()) {
      if (!activeSoloKeys.has(key)) {
        console.log(`[TABLE] Stopped tracking ${key}`);
        state.waitingSolo.delete(key);
      }
    }

    // Periodically prune old entries without dropping active cooldowns or brute windows
    if (state.tableAlertStateByNick.size > 200) {
      for (const [k, rec] of state.tableAlertStateByNick.entries()) {
        const bruteActive = rec.bruteCooldownUntil > now;
        const normalActive = now - rec.lastFirstAlertAt < CONFIG.tableUserCooldownMs;
        const hasRecentPings = rec.recentPingAlerts.some((ts) => now - ts <= bruteWindowMs);
        if (!bruteActive && !normalActive && !hasRecentPings) {
          state.tableAlertStateByNick.delete(k);
        }
      }
    }
  } finally {
    evaluatingTables = false;
  }
}


// ============================================================================
// Pure RFC 7395 XMPP-over-WebSocket Client (Zero Browser)
// ============================================================================
function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function extractText(node) {
  if (typeof node === 'string') return node;
  if (node && typeof node === 'object' && typeof node['#text'] === 'string') {
    return node['#text'];
  }
  return '';
}

function getBareJid(jid = '') {
  return jid.split('/')[0];
}

function getResource(jid = '') {
  const idx = jid.indexOf('/');
  return idx >= 0 ? jid.slice(idx + 1) : '';
}

function runXmppSession(pepa) {
  return new Promise((resolve, reject) => {
    const room = `xogameroom${pepa.room}@chat.jabb.im`;
    const refNick = 'referee';
    const refJid = `xoreferee${pepa.room}@jabbim.cz`;
    let myNick = pepa.d;

    let stage = 'OPEN_1'; // OPEN_1 -> AUTH -> OPEN_2 -> BIND -> SESSION? -> REFEREE_AUTH -> JOIN_MUC -> ONLINE
    let needsSession = false;
    let joined = false;
    let settled = false;
    let pingSeq = 0;
    let lastServerFrameAt = Date.now();
    let pingTimer = null;
    let tableCheckTimer = null;
    let stabilityTimer = null;

    const ws = new WebSocket(CONFIG.wsUrl, ['xmpp'], {
      headers: { Origin: state.activeOrigin },
      handshakeTimeout: 15_000,
    });
    activeWs = ws;

    const send = (xml) => {
      if (CONFIG.debugXmpp) console.log('[XMPP OUT]', xml);
      if (ws.readyState === WebSocket.OPEN) ws.send(xml);
    };

    const cleanupTimers = () => {
      clearInterval(pingTimer);
      clearInterval(tableCheckTimer);
      clearTimeout(connectTimeout);
      clearTimeout(stabilityTimer);
    };

    const fail = (reason) => {
      if (settled) return;
      settled = true;
      cleanupTimers();
      if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
        try {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send('<close xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>');
          }
          ws.close();
        } catch {}
      }
      activeWs = null;
      reject(new Error(reason));
    };

    const connectTimeout = setTimeout(() => {
      if (!joined) fail('Timed out waiting for Playfive referee / MUC room join.');
    }, 35_000);

    const startRefereeAuth = () => {
      stage = 'REFEREE_AUTH';
      send('<presence xmlns="jabber:client"><priority>-1</priority></presence>');
      send(
        `<message to="${escapeXml(refJid)}" type="chat" xmlns="jabber:client">` +
          `<body>${escapeXml(pepa.a)}</body></message>`
      );
    };

    const handleRefereeCommand = (cmd, isDelayed) => {
      if (!cmd) return;
      const f = cmd.split('!');
      switch (f[0]) {
        case '101': {
          if (f.length >= 3 && f[2]) {
            myNick = f[2];
          }
          if (stage === 'REFEREE_AUTH') {
            stage = 'JOIN_MUC';
            console.log(`[XMPP] Referee authorized "${myNick}". Joining ${room}...`);
            send(
              `<presence to="${escapeXml(`${room}/${myNick}`)}" xmlns="jabber:client">` +
                `<x xmlns="http://jabber.org/protocol/muc">` +
                `<password>${escapeXml(CONFIG.mucPassword)}</password>` +
                `</x></presence>`
            );
          }
          break;
        }
        case '202':
        case '222':
        case '206':
          if (!isDelayed) upsertTableFromFields(f);
          break;
        case '250':
          if (!isDelayed) state.tables.clear();
          break;
        case '299':
          if (!isDelayed && f.length >= 2) {
            state.tables.delete(parseInt(f[1], 10));
          }
          break;
        default:
          break;
      }
    };

    const handleChatMessage = async (senderNick, text) => {
      if (CONFIG.ignoreSelfChat && senderNick.toLowerCase() === myNick.toLowerCase()) return;

      const normalized = text.replace(/\s+/g, ' ').toLowerCase();
      if (!normalized.includes(CONFIG.chatTrigger)) return;

      //
      if (CONFIG.ignoreGuestChat && senderNick.toLowerCase().startsWith('guest')) {
        console.log(`[CHAT] Ignored trigger from guest "${senderNick}".`);
        return;
      }

      const now = Date.now();
      if (now - state.lastChatAlertAt >= CONFIG.chatCooldownMs) {
        state.lastChatAlertAt = now;
        await notifyChatTrigger(senderNick, text);
      } else {
        console.log(`[CHAT] Trigger from ${senderNick} suppressed by cooldown.`);
      }
    };

    ws.on('open', () => {
      console.log(`[XMPP] WebSocket connected to ${CONFIG.wsUrl}`);
      send(
        `<open xmlns="urn:ietf:params:xml:ns:xmpp-framing" to="${CONFIG.xmppDomain}" version="1.0"/>`
      );
    });

    ws.on('pong', () => {
      lastServerFrameAt = Date.now();
    });

    ws.on('message', (raw) => {
      lastServerFrameAt = Date.now();
      const xmlStr = raw.toString('utf8');
      if (CONFIG.debugXmpp) console.log('[XMPP IN]', xmlStr.slice(0, 1200));

      let doc;
      try {
        doc = xmlParser.parse(xmlStr);
      } catch (err) {
        console.warn('[XMPP] Failed to parse stanza:', err.message);
        return;
      }

      if (doc.close !== undefined) {
        fail('XMPP server closed the stream.');
        return;
      }

      if (doc.error !== undefined) {
        fail(`XMPP stream error: ${JSON.stringify(doc.error)}`);
        return;
      }

      // 1. Stream features (SASL or Bind/Session)
      if (doc.features !== undefined) {
        if (stage === 'OPEN_1' && doc.features.mechanisms) {
          stage = 'AUTH';
          send('<auth xmlns="urn:ietf:params:xml:ns:xmpp-sasl" mechanism="ANONYMOUS"/>');
          return;
        }
        if (stage === 'OPEN_2' && doc.features.bind !== undefined) {
          needsSession = doc.features.session !== undefined;
          stage = 'BIND';
          send(
            '<iq type="set" id="bind_1" xmlns="jabber:client">' +
              '<bind xmlns="urn:ietf:params:xml:ns:xmpp-bind"/>' +
              '</iq>'
          );
          return;
        }
      }

      // 2. SASL resresult
      if (doc.success !== undefined && stage === 'AUTH') {
        stage = 'OPEN_2';
        send(
          `<open xmlns="urn:ietf:params:xml:ns:xmpp-framing" to="${CONFIG.xmppDomain}" version="1.0"/>`
        );
        return;
      }

      if (doc.failure !== undefined) {
        fail(`SASL authentication failed: ${JSON.stringify(doc.failure)}`);
        return;
      }

      // 3. IQ results (bind, session, server pings)
      if (doc.iq !== undefined) {
        const iqId = doc.iq['@_id'];
        const iqType = doc.iq['@_type'];

        if (iqId === 'bind_1' && stage === 'BIND') {
          if (iqType !== 'result') {
            fail(`Resource bind failed: ${JSON.stringify(doc.iq)}`);
            return;
          }
          if (needsSession) {
            stage = 'SESSION';
            send(
              '<iq type="set" id="sess_1" xmlns="jabber:client">' +
                '<session xmlns="urn:ietf:params:xml:ns:xmpp-session"/>' +
                '</iq>'
            );
          } else {
            startRefereeAuth();
          }
          return;
        }

        if (iqId === 'sess_1' && stage === 'SESSION') {
          if (iqType !== 'result') {
            fail(`XMPP session establishment failed: ${JSON.stringify(doc.iq)}`);
            return;
          }
          startRefereeAuth();
          return;
        }

        // Answer server-initiated XMPP pings
        if (iqType === 'get' && doc.iq.ping !== undefined) {
          const from = doc.iq['@_from'];
          const toAttr = from ? ` to="${escapeXml(from)}"` : '';
          send(`<iq type="result" id="${escapeXml(iqId || '')}"${toAttr} xmlns="jabber:client"/>`);
        }
        return;
      }

      // 4. Presence stanzas (room join, player departures, referee shutdown)
      if (doc.presence !== undefined) {
        const pres = doc.presence;
        const from = pres['@_from'] || '';
        const type = pres['@_type'] || '';
        if (getBareJid(from) !== room) return;

        const resource = getResource(from);

        if (type === 'error' && !joined) {
          fail(`MUC join error from ${from}: ${JSON.stringify(pres.error || '')}`);
          return;
        }

        if (type === 'unavailable') {
          if (resource === refNick) {
            fail('Playfive referee left the room (Playfive shutdown or maintenance).');
            return;
          }
          if (resource === myNick) {
            fail('Bot account disconnected from Herna (duplicate login or server restart).');
            return;
          }
          removePlayerFromAllTables(resource);
          return;
        }

        if (!joined) {
          const xList = Array.isArray(pres.x) ? pres.x : [];
          const codes = new Set();
          for (const x of xList) {
            const statuses = Array.isArray(x?.status) ? x.status : [];
            for (const s of statuses) {
              if (s?.['@_code']) codes.add(String(s['@_code']));
            }
          }

          if (codes.has('110')) {
            if (codes.has('210') && resource) {
              myNick = resource;
            }
            joined = true;
            stage = 'ONLINE';
            clearTimeout(connectTimeout);

            state.status = 'ONLINE';
            state.nick = myNick;
            state.connectedAt = Date.now();
            state.nextRetryAt = null;
            state.lastError = null;

            // Only reset backoff after staying stably online for 60s (prevents rapid flap loops)
            stabilityTimer = setTimeout(() => {
              state.offlineStreak = 0;
            }, 60_000);

            // Announce presence to room just like Playfive's browser client
            const hostName = new URL(state.activeOrigin).hostname;
            const announceBody = `120!${myNick}!${pepa.e}?107!${myNick}!websocket!PlayfiveMonitorBot/2.1!${hostName}`;
            send(
              `<message to="${escapeXml(room)}" type="groupchat" xmlns="jabber:client">` +
                `<body>${escapeXml(announceBody)}</body></message>`
            );

            console.log(`[BOT] Online in Playfive Herna as "${myNick}"!`);

            // Keep-alive WebSocket + XMPP ping every 45s
            pingTimer = setInterval(() => {
              if (Date.now() - lastServerFrameAt > 120_000) {
                fail('No XMPP frames received for >120s (stale connection).');
                return;
              }
              try {
                ws.ping();
                send(
                  `<iq type="get" id="ping_${++pingSeq}" to="${CONFIG.xmppDomain}" xmlns="jabber:client">` +
                    `<ping xmlns="urn:xmpp:ping"/></iq>`
                );
              } catch {}
            }, 45_000);

            // Evaluate solo waiting timers every 7s
            tableCheckTimer = setInterval(() => {
              evaluateWaitingTables().catch((err) =>
                console.error('[TABLE] Evaluation error:', err.message)
              );
            }, 7000);
          }
        }
        return;
      }

      // 5. MessMessage stanzas (referee commands, main chat, table events, DMs)
      if (doc.message !== undefined) {
        const msg = doc.message;
        const from = msg['@_from'] || '';
        const type = msg['@_type'] || '';
        const bare = getBareJid(from);
        const resource = getResource(from);

        // Immediately detect if Playfive's referee JID bounced our auth message
        if (type === 'error' && bare === refJid) {
          fail('Playfive referee is unreachable (XMPP error stanza from referee).');
          return;
        }

        const body = extractText(msg.body);
        if (!body) return;

        const xList = Array.isArray(msg.x) ? msg.x : [];
        const isDelayed =
          msg.delay !== undefined ||
          xList.some((x) => x?.['@_xmlns'] === 'jabber:x:delay');

        // Direct chat messages (referee updates OR player DMs)
        if (type === 'chat') {
          if (bare === refJid || (bare === room && resource === refNick)) {
            for (const cmd of body.split('?')) {
              handleRefereeCommand(cmd, isDelayed);
            }
          } else if (bare === room && resource && !isDelayed) {
            // Politely tell players who DM the bot that it doesn't accept private messages
            if (body.startsWith('106!')) {
              send(
                `<message to="${escapeXml(`${room}/${resource}`)}" type="chat" xmlns="jabber:client">` +
                  `<body>108</body></message>`
              );
            }
          }
          return;
        }

        // Groupchat inside xogameroom@chat.jabb.im
        if (type === 'groupchat' && bare === room) {
          if (resource === refNick) {
            for (const cmd of body.split('?')) {
              handleRefereeCommand(cmd, isDelayed);
            }
            return;
          }

          if (!resource || isDelayed) return;

          // Playfive rule: messages starting with "10" (chat) are NOT split on "?"
          const cmds = body.startsWith('10') ? [body] : body.split('?');
          for (const cmd of cmds) {
            const parts = cmd.split('!');
            const code = parts[0];

            if (code === '100') {
              const chatText = parts.slice(1).join('!');
              handleChatMessage(resource, chatText).catch((err) =>
                console.error('[CHAT] Error handling message:', err.message)
              );
            } else if (code === '209' || code === '219') {
              const tableId = parseInt(parts[1], 10);
              if (!Number.isNaN(tableId)) removePlayerFromTable(tableId, resource);
            } else if (code === '210' && parts.length >= 3) {
              const tableId = parseInt(parts[1], 10);
              const table = state.tables.get(tableId);
              if (table && table.owner === resource) {
                removePlayerFromTable(tableId, parts[2]);
              }
            } else if (code === '211') {
              const tableId = parseInt(parts[1], 10);
              if (!Number.isNaN(tableId)) setSeatOnTable(tableId, 1, resource);
            } else if (code === '212') {
              const tableId = parseInt(parts[1], 10);
              if (!Number.isNaN(tableId)) setSeatOnTable(tableId, 2, resource);
            } else if (code === '213') {
              const tableId = parseInt(parts[1], 10);
              if (!Number.isNaN(tableId)) clearSeatOnTable(tableId, 1, resource);
            } else if (code === '214') {
              const tableId = parseInt(parts[1], 10);
              if (!Number.isNaN(tableId)) clearSeatOnTable(tableId, 2, resource);
            } else if (code === '215' && parts.length >= 3 && parts[2] === myNick) {
              // Politely reject table invitations sent to the bot
              send(
                `<message to="${escapeXml(`${room}/${resource}`)}" type="chat" xmlns="jabber:client">` +
                  `<body>217</body></message>`
              );
            }
          }
        }
      }
    });

    ws.on('error', (err) => {
      fail(`WebSocket error: ${err.message}`);
    });

    ws.on('close', (code, reasonBuf) => {
      const reason = reasonBuf?.toString('utf8') || '';
      if (shuttingDown) {
        if (!settled) {
          settled = true;
          cleanupTimers();
          resolve();
        }
      } else {
        fail(`WebSocket closed (code=${code}${reason ? `, reason=${reason}` : ''})`);
      }
    });
  });
}

// ============================================================================
// Main ResResilient Loop (Low-Power Offline Wait on Shutdowns)
// ============================================================================
async function mainLoop() {
  while (!shuttingDown) {
    try {
      state.tables.clear();
      state.waitingSolo.clear();

      const pepa = await loginAndFetchSession();
      await runXmppSession(pepa);
    } catch (err) {
      if (shuttingDown) break;

      state.status = 'OFFLINE_WAIT';
      state.connectedAt = null;
      state.nick = null;
      state.tables.clear();
      state.waitingSolo.clear();
      state.lastError = err.message;

      const schedule = CONFIG.reconnectBackoffMinutes;
      let waitMin;

      if (err instanceof AuthError) {
        // Never hammer Playfive's login page with invalid credentials
        waitMin = Math.max(60, schedule[schedule.length - 1]);
        console.error(`[AUTH ERROR] ${err.message}`);
      } else {
        waitMin = schedule[Math.min(state.offlineStreak, schedule.length - 1)];
        state.offlineStreak += 1;
        console.error(`[BOT] Disconnected: ${err.message}`);
      }

      state.nextRetryAt = Date.now() + waitMin * 60_000;
      console.log(`[BOT] Idling (0% CPU). Next Playfive check in ${waitMin} minute(s)...`);

      const waitUntil = state.nextRetryAt;
      while (!shuttingDown && Date.now() < waitUntil) {
        await sleep(5000);
      }
    }
  }
}

function shutdown(signal) {
  console.log(`[BOT] Received ${signal}. Shutting down...`);
  shuttingDown = true;
  if (activeWs) {
    try {
      if (activeWs.readyState === WebSocket.OPEN) {
        activeWs.send('<close xmlns="urn:ietf:params:xml:ns:xmpp-framing"/>');
      }
      activeWs.close();
    } catch {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

mainLoop();
