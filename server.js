/* Plyon-Server: liefert die Index.html aus und stellt die API bereit
   (REST + Server-Sent-Events). Nur "express" wird benötigt. */
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(process.env.DATA_DIR || __dirname, 'data.json');
const INDEX_FILE = ['index.html', 'Index.html'].map(f => path.join(__dirname, f)).find(f => fs.existsSync(f));

const MAX_PER_ROOM = 200;
const MAX_TOTAL = 600;
const MAX_DATA_BYTES = 60e6; // Summe aller Bilder im Speicher
const MAX_DATA_CHARS = 6e6;  // pro Nachricht (GIFs bis 4 MB als Base64)

const DEFAULT_EVENTS = [
  { id: 'seed-bio-test-2026-11-02', type: 'test', subject: 'Biologie', title: 'Bio-Test', date: '2026-11-02', time: '08:00' },
  { id: 'seed-en-test-2026-11-16', type: 'test', subject: 'Englisch', title: 'Englisch-Test', date: '2026-11-16', time: '12:25' },
];

/* ---------- Daten laden / speichern ---------- */
let db = {
  config: null,
  events: DEFAULT_EVENTS.map(e => ({ ...e, created: Date.now() })),
  messages: [],
  scores: {},
  users: {},
};

try {
  const saved = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  db = { ...db, ...saved };
} catch (e) { /* erste Ausführung */ }

let saveTimer = null;
function saveNow() {
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) { console.warn('Speichern fehlgeschlagen:', e.message); }
}
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(saveNow, 1500);
}
process.on('SIGTERM', () => { saveNow(); process.exit(0); });
process.on('SIGINT', () => { saveNow(); process.exit(0); });

/* ---------- Hilfsfunktionen ---------- */
const newId = () => crypto.randomBytes(6).toString('hex');
const cleanUid = v => (typeof v === 'string' && /^[a-z0-9_-]{1,40}$/i.test(v) ? v : '');
const cleanName = v => (typeof v === 'string' ? v.trim().slice(0, 40) : '');
const roomParts = room => room.split(':').slice(1);

function validRoom(room, uid) {
  if (room === 'global') return true;
  if (typeof room !== 'string' || !/^dm:[a-z0-9_-]{1,40}:[a-z0-9_-]{1,40}$/i.test(room)) return false;
  return roomParts(room).includes(uid);
}

/* ---------- Live-Verbindungen (SSE) ---------- */
const clients = new Set();
const presence = new Map(); // uid -> Anzahl Verbindungen

function sendTo(client, event, data) {
  client.res.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n');
}
function broadcast(event, data, filter) {
  for (const c of clients) {
    if (!filter || filter(c)) {
      try { sendTo(c, event, data); } catch (e) { /* Verbindung wird beim close entfernt */ }
    }
  }
}
function usersView() {
  const out = {};
  for (const [uid, u] of Object.entries(db.users)) out[uid] = { uid, name: u.name, online: presence.has(uid) };
  return out;
}
const broadcastUsers = () => broadcast('users', usersView());

setInterval(() => {
  for (const c of clients) { try { c.res.write(': ping\n\n'); } catch (e) { /* ignorieren */ } }
}, 20000);

/* ---------- Nachrichten-Limits ---------- */
function pruneMessages(room) {
  const inRoom = db.messages.filter(m => m.room === room);
  for (let i = 0; i < inRoom.length - MAX_PER_ROOM; i++) {
    db.messages.splice(db.messages.indexOf(inRoom[i]), 1);
  }
  while (db.messages.length > MAX_TOTAL) db.messages.shift();
  let bytes = db.messages.reduce((s, m) => s + (m.data ? m.data.length : 0), 0);
  for (let i = 0; bytes > MAX_DATA_BYTES && i < db.messages.length;) {
    if (db.messages[i].data) { bytes -= db.messages[i].data.length; db.messages.splice(i, 1); }
    else i++;
  }
}

/* ---------- App ---------- */
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '8mb' }));

function sendIndex(req, res) {
  if (!INDEX_FILE) return res.status(500).send('index.html fehlt neben der server.js');
  res.sendFile(INDEX_FILE);
}
app.get('/', sendIndex);
app.get('/index.html', sendIndex);

app.get('/api/ping', (req, res) => res.json({ ok: true }));

app.get('/api/stream', (req, res) => {
  const uid = cleanUid(req.query.uid);
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write('retry: 3000\n\n');
  const client = { res, uid };
  clients.add(client);
  if (uid) { presence.set(uid, (presence.get(uid) || 0) + 1); broadcastUsers(); }
  req.on('close', () => {
    clients.delete(client);
    if (uid) {
      const n = (presence.get(uid) || 1) - 1;
      if (n <= 0) presence.delete(uid); else presence.set(uid, n);
      broadcastUsers();
    }
  });
});

/* Nutzer */
app.get('/api/users', (req, res) => res.json(usersView()));

app.post('/api/users', (req, res) => {
  const uid = cleanUid(req.body && req.body.uid);
  const name = cleanName(req.body && req.body.name);
  if (!uid || !name) return res.status(400).json({ error: 'uid und name nötig' });
  db.users[uid] = { uid, name };
  save();
  broadcastUsers();
  res.json(usersView());
});

/* Nachrichten */
app.get('/api/messages', (req, res) => {
  const room = String(req.query.room || 'global');
  const uid = cleanUid(req.query.uid);
  if (!validRoom(room, uid)) return res.status(403).json({ error: 'Kein Zugriff auf diesen Raum' });
  res.json(db.messages.filter(m => m.room === room).sort((a, b) => a.ts - b.ts));
});

app.post('/api/messages', (req, res) => {
  const b = req.body || {};
  const uid = cleanUid(b.uid);
  if (!uid) return res.status(400).json({ error: 'uid fehlt' });
  if (!validRoom(b.room, uid)) return res.status(403).json({ error: 'Ungültiger Raum' });

  const name = cleanName(b.name) || (db.users[uid] && db.users[uid].name) || uid;
  const type = b.type;
  const msg = { id: newId(), ts: Date.now(), room: b.room, uid, name, type, text: '', data: '' };

  if (type === 'text') {
    msg.text = typeof b.text === 'string' ? b.text.trim().slice(0, 2000) : '';
    if (!msg.text) return res.status(400).json({ error: 'Leere Nachricht' });
  } else if (type === 'sticker') {
    msg.text = typeof b.text === 'string' ? b.text.slice(0, 16) : '';
    if (!msg.text) return res.status(400).json({ error: 'Leerer Sticker' });
  } else if (type === 'image') {
    const d = b.data;
    if (typeof d !== 'string' || d.length > MAX_DATA_CHARS || !/^data:image\/(jpeg|png|webp|gif);base64,[A-Za-z0-9+/=]+$/.test(d)) {
      return res.status(400).json({ error: 'Ungültiges Bild' });
    }
    msg.data = d;
  } else {
    return res.status(400).json({ error: 'Unbekannter Typ' });
  }

  if (!db.users[uid]) db.users[uid] = { uid, name };
  db.messages.push(msg);
  pruneMessages(msg.room);
  save();

  broadcast('message', msg, msg.room === 'global' ? null : c => c.uid && roomParts(msg.room).includes(c.uid));
  res.json(msg);
});

/* Einstellungen */
app.get('/api/config', (req, res) => res.json(db.config || {}));

app.put('/api/config', (req, res) => {
  const c = req.body;
  if (!c || typeof c !== 'object' || Array.isArray(c) || JSON.stringify(c).length > 200000) {
    return res.status(400).json({ error: 'Ungültige Einstellungen' });
  }
  db.config = c;
  save();
  broadcast('config', db.config);
  res.json(db.config);
});

/* Termine */
function cleanEvent(body, id, old) {
  if (!body || typeof body !== 'object' || Array.isArray(body) || JSON.stringify(body).length > 20000) return null;
  const ev = { ...(old || {}), ...body, id };
  delete ev.__proto__;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(ev.date || ''))) return null;
  if (ev.time && !/^\d{2}:\d{2}$/.test(String(ev.time))) return null;
  ev.created = (old && old.created) || ev.created || Date.now();
  return ev;
}

app.get('/api/events', (req, res) => res.json(db.events));

app.post('/api/events', (req, res) => {
  const ev = cleanEvent(req.body, newId(), null);
  if (!ev) return res.status(400).json({ error: 'Ungültiger Termin' });
  db.events.push(ev);
  save();
  broadcast('events', db.events);
  res.json(ev);
});

app.put('/api/events/:id', (req, res) => {
  const id = String(req.params.id).slice(0, 80);
  const old = db.events.find(e => e.id === id) || null;
  const ev = cleanEvent(req.body, id, old);
  if (!ev) return res.status(400).json({ error: 'Ungültiger Termin' });
  db.events = db.events.filter(e => e.id !== id).concat(ev);
  save();
  broadcast('events', db.events);
  res.json(ev);
});

app.delete('/api/events/:id', (req, res) => {
  const id = String(req.params.id);
  db.events = db.events.filter(e => e.id !== id);
  save();
  broadcast('events', db.events);
  res.json({ ok: true });
});

/* Arcade-Bestenlisten */
app.get('/api/scores', (req, res) => res.json(db.scores));

app.post('/api/scores', (req, res) => {
  const b = req.body || {};
  const game = typeof b.game === 'string' && /^[a-z0-9_-]{1,40}$/i.test(b.game) ? b.game : '';
  const uid = cleanUid(b.uid);
  const name = cleanName(b.name);
  const score = Number(b.score);
  if (!game || !uid || !name || !Number.isFinite(score) || score < 0 || score > 1e9) {
    return res.status(400).json({ error: 'Ungültiger Score' });
  }
  const list = db.scores[game] || (db.scores[game] = []);
  const mine = list.find(e => e.uid === uid);
  if (mine) { mine.name = name; if (score > mine.score) mine.score = Math.round(score); }
  else list.push({ uid, name, score: Math.round(score) });
  list.sort((a, c) => c.score - a.score);
  db.scores[game] = list.slice(0, 50);
  save();
  broadcast('scores', db.scores);
  res.json(db.scores[game]);
});

app.use('/api', (req, res) => res.status(404).json({ error: 'Nicht gefunden' }));

app.use((err, req, res, next) => {
  const tooLarge = err && err.type === 'entity.too.large';
  res.status(tooLarge ? 413 : 400).json({ error: tooLarge ? 'Datei zu groß' : 'Ungültige Anfrage' });
});

app.listen(PORT, () => console.log('Plyon läuft auf Port ' + PORT));
