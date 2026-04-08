'use strict';

const http = require('http');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');

const ROOT = __dirname;
const PLAYLIST_DIR = path.join(ROOT, 'Playlist');
const DEFAULT_PLAYLIST_DIR = path.join(ROOT, 'defaults', 'Playlist');
const DATA_DIR = path.join(ROOT, 'data');
const ASSETS_DIR = path.join(ROOT, 'assets');
const MANIFEST_PATH = path.join(PLAYLIST_DIR, 'playlists.json');
const DEFAULT_MANIFEST_PATH = path.join(DEFAULT_PLAYLIST_DIR, 'playlists.json');
const BOT_STATUS_PATH = path.join(DATA_DIR, 'bot_status.json');
const CHANNEL_STATE_PATH = path.join(DATA_DIR, 'channel_state.json');
const REMOVALS_PATH = path.join(DATA_DIR, 'removals.json');

const PORT = Number(process.env.PORT || 10000);
const HOST = '0.0.0.0';
const BOT_INTERVAL_MINUTES = Math.max(5, Number(process.env.BOT_INTERVAL_MINUTES || 60));
const BOT_START_DELAY_MS = Math.max(3000, Number(process.env.BOT_START_DELAY_MS || 15000));
const BOT_REQUEST_TIMEOUT_MS = Math.max(3000, Number(process.env.BOT_REQUEST_TIMEOUT_MS || 12000));
const AI_FAILURE_THRESHOLD = 10;
const AI_BOT_NAME = 'AI';
const GITHUB_SYNC_ENABLED = /^(1|true|yes)$/i.test(String(process.env.GITHUB_SYNC_ENABLED || ''));
const GITHUB_TOKEN = String(process.env.GITHUB_TOKEN || '').trim();
const GITHUB_OWNER = String(process.env.GITHUB_OWNER || '').trim();
const GITHUB_REPO = String(process.env.GITHUB_REPO || '').trim();
const GITHUB_BRANCH = String(process.env.GITHUB_BRANCH || 'main').trim() || 'main';
const GITHUB_PATH_PREFIX = String(process.env.GITHUB_PATH_PREFIX || '').trim().replace(/^\/+|\/+$/g, '');
const GITHUB_SYNC_DEBOUNCE_MS = Math.max(1000, Number(process.env.GITHUB_SYNC_DEBOUNCE_MS || 5000));

const SYNTHETIC = {
  ai: {
    id: 'ai_verified',
    name: 'AI Verified',
    filename: 'AI.m3u',
    legacyCandidates: ['AI_Tested_Channel.m3u', 'Bot_Tested_Channel.m3u', 'ai-tested.m3u'],
    icon: '/assets/icons/playlist-ai.svg',
    visible: true,
  },
  human: {
    id: 'human_verified',
    name: 'Human Verified',
    filename: 'Human.m3u',
    legacyCandidates: ['Human_Tested_Channel.m3u', 'Tested_Channel.m3u', 'human-tested.m3u'],
    icon: '/assets/icons/playlist-human.svg',
    visible: true,
  },
  review: {
    id: 'under_review',
    name: 'Under Review',
    filename: 'Review.m3u',
    legacyCandidates: [],
    icon: '/assets/icons/playlist-review.svg',
    visible: true,
  },
  failed: {
    id: 'failed_queue',
    name: 'Failed',
    filename: 'Failed.m3u',
    legacyCandidates: [],
    icon: '/assets/icons/alert.svg',
    visible: false,
  },
};

const SYNTHETIC_BY_FILENAME = new Map(Object.values(SYNTHETIC).map(item => [item.filename, item]));
const PROTECTED_FILENAMES = new Set([
  ...Object.values(SYNTHETIC).map(item => item.filename),
  'AI_Tested_Channel.m3u',
  'Bot_Tested_Channel.m3u',
  'Human_Tested_Channel.m3u',
  'Tested_Channel.m3u',
  'ai-tested.m3u',
  'human-tested.m3u',
]);

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.m3u': 'application/vnd.apple.mpegurl; charset=utf-8',
  '.m3u8': 'application/vnd.apple.mpegurl; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.txt': 'text/plain; charset=utf-8',
};

const botRuntime = {
  running: false,
  scheduled: false,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastError: null,
  lastSummary: null,
};

const githubSyncRuntime = {
  enabled: false,
  pending: new Set(),
  timer: null,
  flushing: false,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastError: null,
  lastResults: [],
};

function defaultPlaylists() {
  return [
    ['Bahrain', 'BH', 'Playlist/Bahrain.m3u'],
    ['Bangladesh', 'BD', 'Playlist/Bangladesh.m3u'],
    ['Egypt', 'EG', 'Playlist/Egypt.m3u'],
    ['Global Top', '', 'Playlist/Global Top.m3u'],
    ['India', 'IN', 'Playlist/India.m3u'],
    ['Iran', 'IR', 'Playlist/Iran.m3u'],
    ['Iraq', 'IQ', 'Playlist/Iraq.m3u'],
    ['Israel', 'IL', 'Playlist/Israel.m3u'],
    ['Kuwait', 'KW', 'Playlist/Kuwait.m3u'],
    ['Nepal', 'NP', 'Playlist/Nepal.m3u'],
    ['Oman', 'OM', 'Playlist/Oman.m3u'],
    ['Pakistan', 'PK', 'Playlist/Pakistan.m3u'],
    ['Qatar', 'QA', 'Playlist/Qatar.m3u'],
    ['Saudi Arabia', 'SA', 'Playlist/Saudi_Arabia.m3u'],
    ['Sports', '', 'Playlist/Sports.m3u'],
    ['United Arab Emirates', 'AE', 'Playlist/United_Arab_Emirates.m3u'],
    ['United States', 'US', 'Playlist/United_States.m3u'],
  ].map(([name, code, file]) => ({ name, code, file }));
}

async function readJson(filePath, fallback) {
  try {
    return JSON.parse(await fsp.readFile(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

async function writeJson(filePath, data) {
  await fsp.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function sanitizeId(value) {
  return String(value || '').replace(/[^a-z0-9]+/gi, '_').replace(/^_+|_+$/g, '').toLowerCase();
}

function getAttr(line, attr) {
  const match = line.match(new RegExp(`${attr}="([^"]*)"`, 'i'));
  return match ? match[1] : '';
}

function channelIdFor(name, url, tvgId = '') {
  return crypto.createHash('sha1').update(`${name}|${tvgId}|${url}`).digest('hex').slice(0, 16);
}

function normalizeManifestEntry(entry) {
  const rawFile = String(entry.file || entry.path || '').trim();
  const relativeFile = rawFile.startsWith('Playlist/') ? rawFile.slice('Playlist/'.length) : rawFile;
  const name = String(entry.name || path.basename(relativeFile, path.extname(relativeFile))).trim();
  const code = String(entry.code || '').trim().toUpperCase();
  return {
    id: sanitizeId(name),
    name,
    code,
    file: 'Playlist/' + relativeFile,
    relativeFile,
    absoluteFile: path.join(PLAYLIST_DIR, relativeFile),
  };
}

function parseM3U(text, playlistDef) {
  const lines = String(text || '').split(/\r?\n/);
  const header = lines.find(line => line.trim().startsWith('#EXTM3U')) || '#EXTM3U';
  const channels = [];
  let currentExtinf = null;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;

    if (line.startsWith('#EXTINF')) {
      currentExtinf = line;
      continue;
    }

    if (!line.startsWith('#') && currentExtinf) {
      const lastComma = currentExtinf.lastIndexOf(',');
      const name = lastComma >= 0 ? currentExtinf.slice(lastComma + 1).trim() : 'Unknown';
      const tvgId = getAttr(currentExtinf, 'tvg-id') || '';
      const countryCode = (getAttr(currentExtinf, 'tvg-country') || playlistDef.code || '').toUpperCase();
      const group = getAttr(currentExtinf, 'group-title') || 'General';
      channels.push({
        id: channelIdFor(name, line, tvgId),
        name,
        tvgId,
        tvgName: getAttr(currentExtinf, 'tvg-name') || name,
        logo: getAttr(currentExtinf, 'tvg-logo') || '',
        group,
        url: line,
        countryCode,
        countryName: playlistDef.name,
        playlist: playlistDef.name,
        playlistId: playlistDef.id,
        playlistCode: playlistDef.code,
        playlistFile: playlistDef.file,
        block: `${currentExtinf}\n${line}`,
      });
      currentExtinf = null;
    }
  }

  return { header, channels };
}

function serializeM3U(header, channels) {
  const cleanHeader = header && header.startsWith('#EXTM3U') ? header : '#EXTM3U';
  const body = channels.map(channel => channel.block).join('\n\n');
  return `${cleanHeader}\n${body ? `\n${body}\n` : ''}`;
}

async function ensureFileWithHeader(filePath) {
  try {
    await fsp.access(filePath, fs.constants.F_OK);
  } catch {
    await fsp.writeFile(filePath, '#EXTM3U\n', 'utf8');
  }
}

async function copyDirectory(sourceDir, targetDir) {
  await fsp.mkdir(targetDir, { recursive: true });
  const entries = await fsp.readdir(sourceDir, { withFileTypes: true });
  for (const entry of entries) {
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);
    if (entry.isDirectory()) {
      await copyDirectory(sourcePath, targetPath);
    } else if (entry.isFile()) {
      await fsp.copyFile(sourcePath, targetPath);
    }
  }
}

async function seedPlaylistDirectoryIfEmpty() {
  if (!fs.existsSync(DEFAULT_PLAYLIST_DIR)) return;

  let entries = [];
  try {
    entries = await fsp.readdir(PLAYLIST_DIR);
  } catch {
    entries = [];
  }

  if (entries.length > 0) return;
  await copyDirectory(DEFAULT_PLAYLIST_DIR, PLAYLIST_DIR);
}

async function migrateSyntheticFile(def) {
  const targetPath = path.join(PLAYLIST_DIR, def.filename);
  if (fs.existsSync(targetPath)) return;

  for (const legacyName of def.legacyCandidates) {
    const legacyPath = path.join(PLAYLIST_DIR, legacyName);
    if (fs.existsSync(legacyPath)) {
      await fsp.copyFile(legacyPath, targetPath);
      return;
    }
  }

  await ensureFileWithHeader(targetPath);
}


function buildGitHubRepoPath(relativePath) {
  const cleanRelative = String(relativePath || '').replace(/^\/+/, '');
  return GITHUB_PATH_PREFIX ? `${GITHUB_PATH_PREFIX}/${cleanRelative}` : cleanRelative;
}

function isGitHubSyncConfigured() {
  return GITHUB_SYNC_ENABLED && Boolean(GITHUB_TOKEN && GITHUB_OWNER && GITHUB_REPO);
}

function updateGitHubSyncEnabledState() {
  githubSyncRuntime.enabled = isGitHubSyncConfigured();
  if (!githubSyncRuntime.enabled && GITHUB_SYNC_ENABLED) {
    githubSyncRuntime.lastError = 'GitHub sync is enabled but missing GITHUB_TOKEN, GITHUB_OWNER, or GITHUB_REPO.';
  }
}

async function githubRequest(method, repoPath, body = null) {
  const apiUrl = `https://api.github.com/repos/${encodeURIComponent(GITHUB_OWNER)}/${encodeURIComponent(GITHUB_REPO)}/contents/${repoPath}?ref=${encodeURIComponent(GITHUB_BRANCH)}`;
  const response = await fetch(apiUrl, {
    method,
    headers: {
      'Authorization': `Bearer ${GITHUB_TOKEN}`,
      'Accept': 'application/vnd.github+json',
      'User-Agent': 'KestfordGitHubSync/1.0',
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { message: text };
  }

  if (!response.ok) {
    const error = new Error(data.message || `GitHub API returned ${response.status}`);
    error.status = response.status;
    error.payload = data;
    throw error;
  }

  return data;
}

async function syncFileToGitHub(relativePath) {
  const localPath = path.join(ROOT, relativePath);
  const repoPath = buildGitHubRepoPath(relativePath);
  const content = await fsp.readFile(localPath);
  let sha = undefined;

  try {
    const existing = await githubRequest('GET', repoPath);
    sha = existing.sha;
    if (existing.content) {
      const normalizedRemote = Buffer.from(String(existing.content).replace(/\n/g, ''), 'base64').toString('utf8').replace(/\r\n/g, '\n');
      const normalizedLocal = content.toString('utf8').replace(/\r\n/g, '\n');
      if (normalizedRemote === normalizedLocal) {
        return { path: relativePath, repoPath, updated: false, skipped: true };
      }
    }
  } catch (error) {
    if (error.status !== 404) throw error;
  }

  await githubRequest('PUT', repoPath, {
    message: `Auto sync ${relativePath}`,
    content: content.toString('base64'),
    branch: GITHUB_BRANCH,
    ...(sha ? { sha } : {}),
  });

  return { path: relativePath, repoPath, updated: true, skipped: false };
}

function queueGitHubSync(relativePath) {
  if (!githubSyncRuntime.enabled) return;
  githubSyncRuntime.pending.add(relativePath.replace(/^\/+/, ''));
  if (githubSyncRuntime.timer) return;
  githubSyncRuntime.timer = setTimeout(() => {
    flushGitHubSyncQueue().catch(error => {
      console.error('GitHub sync flush failed:', error);
    });
  }, GITHUB_SYNC_DEBOUNCE_MS);
}

async function flushGitHubSyncQueue() {
  if (!githubSyncRuntime.enabled) return { ok: false, skipped: true, reason: 'disabled' };
  if (githubSyncRuntime.flushing) return { ok: false, skipped: true, reason: 'already_flushing' };
  if (githubSyncRuntime.timer) {
    clearTimeout(githubSyncRuntime.timer);
    githubSyncRuntime.timer = null;
  }

  const paths = [...githubSyncRuntime.pending];
  githubSyncRuntime.pending.clear();
  if (!paths.length) return { ok: true, skipped: true, reason: 'nothing_pending', results: [] };

  githubSyncRuntime.flushing = true;
  githubSyncRuntime.lastStartedAt = new Date().toISOString();
  githubSyncRuntime.lastError = null;
  const results = [];

  try {
    for (const relativePath of paths) {
      const result = await syncFileToGitHub(relativePath);
      results.push(result);
    }
    githubSyncRuntime.lastResults = results;
    githubSyncRuntime.lastFinishedAt = new Date().toISOString();
    return { ok: true, results };
  } catch (error) {
    githubSyncRuntime.lastError = error.message;
    githubSyncRuntime.lastFinishedAt = new Date().toISOString();
    githubSyncRuntime.lastResults = results;
    throw error;
  } finally {
    githubSyncRuntime.flushing = false;
  }
}

async function flushGitHubSyncQueueSafe() {
  try {
    return await flushGitHubSyncQueue();
  } catch (error) {
    console.error('GitHub sync failed:', error);
    return { ok: false, error: error.message };
  }
}

async function ensurePaths() {
  await fsp.mkdir(PLAYLIST_DIR, { recursive: true });
  await fsp.mkdir(DATA_DIR, { recursive: true });
  await fsp.mkdir(ASSETS_DIR, { recursive: true });

  await seedPlaylistDirectoryIfEmpty();

  if (!fs.existsSync(MANIFEST_PATH)) {
    if (fs.existsSync(DEFAULT_MANIFEST_PATH)) {
      await fsp.copyFile(DEFAULT_MANIFEST_PATH, MANIFEST_PATH);
    } else {
      await writeJson(MANIFEST_PATH, { playlists: defaultPlaylists() });
    }
  }
  if (!fs.existsSync(BOT_STATUS_PATH)) {
    await writeJson(BOT_STATUS_PATH, {
      running: false,
      scheduled: false,
      lastStartedAt: null,
      lastFinishedAt: null,
      lastError: null,
      lastSummary: null,
      botName: AI_BOT_NAME,
    });
  }
  if (!fs.existsSync(CHANNEL_STATE_PATH)) {
    await writeJson(CHANNEL_STATE_PATH, { channels: {} });
  }
  if (!fs.existsSync(REMOVALS_PATH)) {
    await writeJson(REMOVALS_PATH, { removals: [] });
  }

  for (const def of Object.values(SYNTHETIC)) {
    await migrateSyntheticFile(def);
  }
}

function sendJson(res, status, data) {
  const body = JSON.stringify(data, null, 2);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(body);
}

function sendText(res, status, text, contentType = 'text/plain; charset=utf-8') {
  res.writeHead(status, {
    'Content-Type': contentType,
    'Content-Length': Buffer.byteLength(text),
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(text);
}

function notFound(res) {
  sendText(res, 404, 'Not found');
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw ? JSON.parse(raw) : {};
}

async function loadManifest() {
  const data = await readJson(MANIFEST_PATH, { playlists: defaultPlaylists() });
  const playlists = Array.isArray(data.playlists) ? data.playlists : [];
  return playlists
    .map(normalizeManifestEntry)
    .filter(item => !PROTECTED_FILENAMES.has(item.relativeFile));
}

async function saveManifest(playlists) {
  const visible = playlists.filter(item => !PROTECTED_FILENAMES.has(item.relativeFile));
  await writeJson(MANIFEST_PATH, {
    playlists: visible.map(item => ({ name: item.name, code: item.code, file: item.file })),
  });
  queueGitHubSync('Playlist/playlists.json');
}

function syntheticPlaylistPath(def) {
  return path.join(PLAYLIST_DIR, def.filename);
}

async function loadSyntheticParsed(def, canonicalMap = null) {
  const filePath = syntheticPlaylistPath(def);
  let parsed;
  try {
    const raw = await fsp.readFile(filePath, 'utf8');
    parsed = parseM3U(raw, {
      id: def.id,
      name: def.name,
      code: '',
      file: 'Playlist/' + def.filename,
    });
  } catch {
    parsed = { header: '#EXTM3U', channels: [] };
  }

  if (!canonicalMap) return parsed;

  const filtered = [];
  for (const item of parsed.channels) {
    const canonical = canonicalMap.get(item.id);
    if (!canonical) continue;
    filtered.push({
      ...canonical,
      playlist: def.name,
      playlistId: def.id,
      playlistCode: '',
      playlistFile: 'Playlist/' + def.filename,
    });
  }

  if (filtered.length !== parsed.channels.length || filtered.some((item, index) => item.block !== parsed.channels[index]?.block)) {
    await saveSyntheticChannels(def, filtered, parsed.header);
  }

  return { header: parsed.header, channels: filtered };
}

async function saveSyntheticChannels(def, channels, header = '#EXTM3U') {
  const filePath = syntheticPlaylistPath(def);
  await fsp.writeFile(filePath, serializeM3U(header, channels), 'utf8');
  queueGitHubSync(`Playlist/${def.filename}`);
}

async function removeChannelFromSynthetic(def, channelId) {
  const parsed = await loadSyntheticParsed(def);
  const kept = parsed.channels.filter(channel => channel.id !== channelId);
  const removed = kept.length !== parsed.channels.length;
  if (removed) {
    await saveSyntheticChannels(def, kept, parsed.header);
  }
  return { removed, count: kept.length };
}

async function upsertChannelInSynthetic(def, channel) {
  const parsed = await loadSyntheticParsed(def);
  const entry = {
    ...channel,
    playlist: def.name,
    playlistId: def.id,
    playlistCode: '',
    playlistFile: 'Playlist/' + def.filename,
  };

  const index = parsed.channels.findIndex(item => item.id === channel.id);
  let added = false;
  let updated = false;

  if (index === -1) {
    parsed.channels.push(entry);
    added = true;
  } else {
    const current = parsed.channels[index];
    if (current.block !== entry.block || current.name !== entry.name || current.url !== entry.url) {
      parsed.channels[index] = entry;
      updated = true;
    }
  }

  if (added || updated) {
    await saveSyntheticChannels(def, parsed.channels, parsed.header);
  }

  return { added, updated, count: parsed.channels.length };
}

async function loadChannelState() {
  return readJson(CHANNEL_STATE_PATH, { channels: {} });
}

async function writeChannelState(data) {
  await writeJson(CHANNEL_STATE_PATH, data);
}

function ensureChannelMeta(state, channel) {
  const current = state.channels[channel.id] || {
    id: channel.id,
    name: channel.name,
    url: channel.url,
    failureCount: 0,
    lastStatus: null,
    lastCheckSource: null,
    firstSeenAt: new Date().toISOString(),
  };
  current.name = channel.name;
  current.url = channel.url;
  current.lastPlaylistName = channel.playlist;
  current.lastPlaylistFile = channel.playlistFile;
  state.channels[channel.id] = current;
  return current;
}

async function purgeChannelState(channelId) {
  const state = await loadChannelState();
  if (!state.channels[channelId]) return false;
  delete state.channels[channelId];
  await writeChannelState(state);
  return true;
}

async function logRemoval(entry) {
  const data = await readJson(REMOVALS_PATH, { removals: [] });
  data.removals.unshift(entry);
  data.removals = data.removals.slice(0, 500);
  await writeJson(REMOVALS_PATH, data);
}

async function removeChannelFromPlaylistFile(playlistDef, channelId) {
  try {
    const raw = await fsp.readFile(playlistDef.absoluteFile, 'utf8');
    const parsed = parseM3U(raw, playlistDef);
    const kept = parsed.channels.filter(channel => channel.id !== channelId);
    if (kept.length === parsed.channels.length) {
      return { removed: false, becameEmpty: false };
    }
    if (!kept.length) {
      try {
        await fsp.unlink(playlistDef.absoluteFile);
      } catch {}
      return { removed: true, becameEmpty: true };
    }
    await fsp.writeFile(playlistDef.absoluteFile, serializeM3U(parsed.header, kept), 'utf8');
    return { removed: true, becameEmpty: false };
  } catch {
    return { removed: false, becameEmpty: false };
  }
}

async function removeChannelFromEverywhere(channelId) {
  const manifest = await loadManifest();
  const emptiedPlaylists = [];
  let removedAny = false;
  const keptManifest = [];

  for (const playlist of manifest) {
    const result = await removeChannelFromPlaylistFile(playlist, channelId);
    if (result.removed) removedAny = true;
    if (result.becameEmpty) {
      emptiedPlaylists.push({ id: playlist.id, name: playlist.name });
    } else {
      keptManifest.push(playlist);
    }
  }

  if (emptiedPlaylists.length) {
    await saveManifest(keptManifest);
  }

  const syntheticResults = {};
  for (const [key, def] of Object.entries(SYNTHETIC)) {
    syntheticResults[key] = await removeChannelFromSynthetic(def, channelId);
    if (syntheticResults[key].removed) removedAny = true;
  }

  await purgeChannelState(channelId);

  return {
    removedAny,
    emptiedPlaylists,
    removedFromAiVerified: syntheticResults.ai.removed,
    removedFromHumanVerified: syntheticResults.human.removed,
    removedFromUnderReview: syntheticResults.review.removed,
    removedFromFailed: syntheticResults.failed.removed,
  };
}

async function markChannelSuccess(channel, source) {
  const state = await loadChannelState();
  const record = ensureChannelMeta(state, channel);
  const hadFailures = Number(record.failureCount || 0) > 0;
  record.failureCount = 0;
  record.lastStatus = 'success';
  record.lastCheckSource = source;
  record.lastSuccessAt = new Date().toISOString();
  record.updatedAt = record.lastSuccessAt;
  await writeChannelState(state);
  return { hadFailures };
}

async function markChannelFailure(channel, source, reason) {
  const state = await loadChannelState();
  const record = ensureChannelMeta(state, channel);
  record.failureCount = Number(record.failureCount || 0) + 1;
  record.lastStatus = 'failure';
  record.lastCheckSource = source;
  record.lastFailureReason = reason || '';
  record.lastFailureAt = new Date().toISOString();
  record.updatedAt = record.lastFailureAt;

  if (record.failureCount >= AI_FAILURE_THRESHOLD) {
    await writeChannelState(state);
    const removal = await removeChannelFromEverywhere(channel.id);
    await logRemoval({
      channelId: channel.id,
      name: channel.name,
      playlistName: channel.playlist,
      playlistFile: channel.playlistFile,
      scope: 'permanent_after_10_ai_failures',
      source,
      failureCount: record.failureCount,
      removedAt: new Date().toISOString(),
    });
    return {
      permanentlyRemoved: true,
      failureCount: record.failureCount,
      removal,
    };
  }

  await writeChannelState(state);
  return {
    permanentlyRemoved: false,
    failureCount: record.failureCount,
  };
}

async function resetFailureCountById(channelId) {
  const state = await loadChannelState();
  const record = state.channels[channelId];
  if (!record || !Number(record.failureCount || 0)) return false;
  record.failureCount = 0;
  record.lastStatus = 'success';
  record.updatedAt = new Date().toISOString();
  await writeChannelState(state);
  return true;
}

async function loadCatalogAndPrune() {
  let manifest = await loadManifest();
  let manifestChanged = false;
  const validPlaylists = [];
  const allChannels = [];
  const autoRemovedPlaylists = [];

  for (const playlist of manifest) {
    try {
      const raw = await fsp.readFile(playlist.absoluteFile, 'utf8');
      const parsed = parseM3U(raw, playlist);
      if (!parsed.channels.length) {
        manifestChanged = true;
        autoRemovedPlaylists.push({ id: playlist.id, name: playlist.name, reason: 'empty' });
        continue;
      }
      validPlaylists.push(playlist);
      allChannels.push(...parsed.channels);
    } catch {
      manifestChanged = true;
      autoRemovedPlaylists.push({ id: playlist.id, name: playlist.name, reason: 'missing_or_unreadable' });
    }
  }

  if (manifestChanged) {
    await saveManifest(validPlaylists);
    manifest = validPlaylists;
  }

  const canonicalMap = new Map();
  for (const channel of allChannels) {
    if (!canonicalMap.has(channel.id)) canonicalMap.set(channel.id, channel);
  }
  const channels = [...canonicalMap.values()];

  const aiParsed = await loadSyntheticParsed(SYNTHETIC.ai, canonicalMap);
  const humanParsed = await loadSyntheticParsed(SYNTHETIC.human, canonicalMap);
  const reviewParsed = await loadSyntheticParsed(SYNTHETIC.review, canonicalMap);
  const failedParsed = await loadSyntheticParsed(SYNTHETIC.failed, canonicalMap);

  const aiIds = new Set(aiParsed.channels.map(channel => channel.id));
  const humanIds = new Set(humanParsed.channels.map(channel => channel.id));
  const reviewIds = new Set(reviewParsed.channels.map(channel => channel.id));
  const failedIds = new Set(failedParsed.channels.map(channel => channel.id));

  const playlistsOut = [SYNTHETIC.ai, SYNTHETIC.human, SYNTHETIC.review].map(def => {
    let count = 0;
    if (def.id === SYNTHETIC.ai.id) count = aiParsed.channels.length;
    if (def.id === SYNTHETIC.human.id) count = humanParsed.channels.length;
    if (def.id === SYNTHETIC.review.id) count = reviewParsed.channels.length;
    return {
      id: def.id,
      name: def.name,
      code: '',
      file: 'Playlist/' + def.filename,
      channelCount: count,
      synthetic: true,
      icon: def.icon,
    };
  });

  playlistsOut.push(...manifest.map(playlist => ({
    id: playlist.id,
    name: playlist.name,
    code: playlist.code,
    file: playlist.file,
    channelCount: channels.filter(channel => channel.playlistId === playlist.id).length,
    icon: '/assets/icons/playlist.svg',
  })));

  return {
    playlists: playlistsOut,
    channels,
    aiVerifiedChannelIds: [...aiIds],
    humanVerifiedChannelIds: [...humanIds],
    underReviewChannelIds: [...reviewIds],
    failedChannelIds: [...failedIds],
    failedCount: failedParsed.channels.length,
    autoRemovedPlaylists,
  };
}

async function writeBotStatusFile() {
  await writeJson(BOT_STATUS_PATH, {
    running: botRuntime.running,
    scheduled: botRuntime.scheduled,
    lastStartedAt: botRuntime.lastStartedAt,
    lastFinishedAt: botRuntime.lastFinishedAt,
    lastError: botRuntime.lastError,
    lastSummary: botRuntime.lastSummary,
    intervalMinutes: BOT_INTERVAL_MINUTES,
    botName: AI_BOT_NAME,
  });
}

async function testUrl(url) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), BOT_REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      redirect: 'follow',
      headers: { 'User-Agent': 'KestfordAI/1.0' },
      signal: controller.signal,
    });

    if (!response.ok) {
      return { ok: false, status: response.status, reason: `status_${response.status}` };
    }

    const contentType = response.headers.get('content-type') || '';
    const isManifest = /mpegurl|m3u8/i.test(contentType) || /\.m3u8?($|\?)/i.test(url);
    if (isManifest) {
      const text = await response.text();
      const mediaLines = text.split(/\r?\n/).map(line => line.trim()).filter(line => line && !line.startsWith('#'));
      if (!mediaLines.length) {
        return { ok: false, status: response.status, reason: 'empty_manifest' };
      }
      return { ok: true, manifest: true, status: response.status };
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    if (!buffer.length) {
      return { ok: false, status: response.status, reason: 'empty_response' };
    }
    return { ok: true, manifest: false, status: response.status };
  } catch (error) {
    return { ok: false, reason: error.name === 'AbortError' ? 'timeout' : 'fetch_error', detail: error.message };
  } finally {
    clearTimeout(timer);
  }
}

async function runBotCycle({ manual = false } = {}) {
  if (botRuntime.running) {
    return {
      skipped: true,
      reason: 'already_running',
      status: botRuntime,
    };
  }

  botRuntime.running = true;
  botRuntime.lastStartedAt = new Date().toISOString();
  botRuntime.lastError = null;
  await writeBotStatusFile();

  const summary = {
    manual,
    botName: AI_BOT_NAME,
    checkedMain: 0,
    passedMain: 0,
    failedMain: 0,
    checkedFailed: 0,
    passedFailed: 0,
    failedFailed: 0,
    addedToAiVerified: 0,
    keptUnderReview: 0,
    removedFromFailed: 0,
    permanentRemovals: 0,
    startedAt: botRuntime.lastStartedAt,
    finishedAt: null,
  };

  try {
    const catalog = await loadCatalogAndPrune();
    const reviewIds = new Set(catalog.underReviewChannelIds);
    const failedIds = new Set(catalog.failedChannelIds);

    for (const channel of catalog.channels) {
      if (failedIds.has(channel.id) || reviewIds.has(channel.id)) continue;
      summary.checkedMain += 1;
      const result = await testUrl(channel.url);
      if (result.ok) {
        summary.passedMain += 1;
        await markChannelSuccess(channel, 'ai_main');
        const upsert = await upsertChannelInSynthetic(SYNTHETIC.ai, channel);
        if (upsert.added) summary.addedToAiVerified += 1;
      } else {
        summary.failedMain += 1;
        const failure = await markChannelFailure(channel, 'ai_main', result.reason || result.detail || 'unknown_error');
        if (failure.permanentlyRemoved) summary.permanentRemovals += 1;
      }
    }

    const refreshedCatalog = await loadCatalogAndPrune();
    const canonicalMap = new Map(refreshedCatalog.channels.map(channel => [channel.id, channel]));
    const reviewIdsNow = new Set(refreshedCatalog.underReviewChannelIds);
    const failedParsed = await loadSyntheticParsed(SYNTHETIC.failed, canonicalMap);

    for (const failedChannel of failedParsed.channels) {
      const channel = canonicalMap.get(failedChannel.id) || failedChannel;
      summary.checkedFailed += 1;
      const result = await testUrl(channel.url);
      if (result.ok) {
        summary.passedFailed += 1;
        await markChannelSuccess(channel, 'ai_failed_review');
        const removedFailed = await removeChannelFromSynthetic(SYNTHETIC.failed, channel.id);
        if (removedFailed.removed) summary.removedFromFailed += 1;

        if (reviewIdsNow.has(channel.id)) {
          summary.keptUnderReview += 1;
        } else {
          const upsert = await upsertChannelInSynthetic(SYNTHETIC.ai, channel);
          if (upsert.added) summary.addedToAiVerified += 1;
        }
      } else {
        summary.failedFailed += 1;
        const failure = await markChannelFailure(channel, 'ai_failed_review', result.reason || result.detail || 'unknown_error');
        if (failure.permanentlyRemoved) summary.permanentRemovals += 1;
      }
    }

    summary.finishedAt = new Date().toISOString();
    botRuntime.lastFinishedAt = summary.finishedAt;
    botRuntime.lastSummary = summary;
    await writeBotStatusFile();
    await flushGitHubSyncQueueSafe();
    return summary;
  } catch (error) {
    botRuntime.lastError = error.message;
    await writeBotStatusFile();
    throw error;
  } finally {
    botRuntime.running = false;
    botRuntime.lastFinishedAt = new Date().toISOString();
    if (summary.finishedAt === null) summary.finishedAt = botRuntime.lastFinishedAt;
    botRuntime.lastSummary = summary;
    await writeBotStatusFile();
  }
}

function scheduleBotCycle() {
  if (botRuntime.scheduled) return;
  botRuntime.scheduled = true;

  const intervalMs = BOT_INTERVAL_MINUTES * 60 * 1000;
  setTimeout(() => {
    runBotCycle().catch(error => console.error('AI bot cycle failed:', error));
    setInterval(() => {
      runBotCycle().catch(error => console.error('AI bot cycle failed:', error));
    }, intervalMs);
  }, BOT_START_DELAY_MS);
}

function rewriteManifestLine(line, baseUrl) {
  if (!line.trim()) return line;
  if (line.startsWith('#')) {
    return line.replace(/URI="([^"]+)"/g, (_, uri) => {
      const absolute = new URL(uri, baseUrl).href;
      return `URI="/proxy?url=${encodeURIComponent(absolute)}"`;
    });
  }
  const absolute = new URL(line, baseUrl).href;
  return `/proxy?url=${encodeURIComponent(absolute)}`;
}

async function handleProxy(_req, res, urlObj) {
  const target = urlObj.searchParams.get('url');
  if (!target) return sendJson(res, 400, { error: 'Missing url parameter' });

  let parsedUrl;
  try {
    parsedUrl = new URL(target);
  } catch {
    return sendJson(res, 400, { error: 'Invalid target URL' });
  }

  if (!/^https?:$/.test(parsedUrl.protocol)) {
    return sendJson(res, 400, { error: 'Only http and https proxying are allowed' });
  }

  try {
    const upstream = await fetch(parsedUrl.href, {
      redirect: 'follow',
      headers: { 'User-Agent': 'KestfordOS/1.0' },
    });

    if (!upstream.ok) {
      return sendJson(res, upstream.status, { error: `Upstream returned ${upstream.status}` });
    }

    const contentType = upstream.headers.get('content-type') || '';
    const treatAsManifest = /mpegurl|m3u8/i.test(contentType) || /\.m3u8?($|\?)/i.test(parsedUrl.pathname);
    if (treatAsManifest) {
      const text = await upstream.text();
      const rewritten = text.split(/\r?\n/).map(line => rewriteManifestLine(line, parsedUrl.href)).join('\n');
      res.writeHead(200, {
        'Content-Type': 'application/vnd.apple.mpegurl; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      return res.end(rewritten);
    }

    const buffer = Buffer.from(await upstream.arrayBuffer());
    res.writeHead(200, {
      'Content-Type': contentType || 'application/octet-stream',
      'Content-Length': buffer.length,
      'Cache-Control': 'no-store',
    });
    res.end(buffer);
  } catch (error) {
    sendJson(res, 502, { error: 'Proxy fetch failed', detail: error.message });
  }
}

async function handleCatalog(_req, res) {
  const catalog = await loadCatalogAndPrune();
  sendJson(res, 200, {
    ok: true,
    rule: {
      botName: AI_BOT_NAME,
      aiVerifiedName: SYNTHETIC.ai.name,
      humanVerifiedName: SYNTHETIC.human.name,
      underReviewName: SYNTHETIC.review.name,
      failedName: SYNTHETIC.failed.name,
      permanentDeleteAfterAiFailures: AI_FAILURE_THRESHOLD,
      humanFailureMovesToFailed: true,
      aiVerifiedHumanFailureMovesToReview: true,
      humanSuccessAddsToHumanVerified: true,
      failedPlaylistCheckedBySecondAi: true,
      botIntervalMinutes: BOT_INTERVAL_MINUTES,
    },
    playlists: catalog.playlists,
    channels: catalog.channels,
    aiVerifiedChannelIds: catalog.aiVerifiedChannelIds,
    humanVerifiedChannelIds: catalog.humanVerifiedChannelIds,
    underReviewChannelIds: catalog.underReviewChannelIds,
    failedChannelIds: catalog.failedChannelIds,
    failedCount: catalog.failedCount,
    autoRemovedPlaylists: catalog.autoRemovedPlaylists,
    botStatus: await readJson(BOT_STATUS_PATH, {
      running: botRuntime.running,
      scheduled: botRuntime.scheduled,
      lastStartedAt: botRuntime.lastStartedAt,
      lastFinishedAt: botRuntime.lastFinishedAt,
      lastError: botRuntime.lastError,
      lastSummary: botRuntime.lastSummary,
      intervalMinutes: BOT_INTERVAL_MINUTES,
      botName: AI_BOT_NAME,
    }),
  });
}

async function handleReportSuccess(req, res) {
  const body = await readBody(req);
  const channelId = String(body.channelId || '').trim();
  if (!channelId) return sendJson(res, 400, { error: 'channelId is required' });

  const catalog = await loadCatalogAndPrune();
  const channel = catalog.channels.find(item => item.id === channelId);
  if (!channel) return sendJson(res, 404, { ok: false, error: 'Channel not found' });

  await markChannelSuccess(channel, 'human_success');
  const humanResult = await upsertChannelInSynthetic(SYNTHETIC.human, channel);
  const failedRemoval = await removeChannelFromSynthetic(SYNTHETIC.failed, channelId);
  const reviewRemoval = await removeChannelFromSynthetic(SYNTHETIC.review, channelId);

  const githubSync = await flushGitHubSyncQueueSafe();
  sendJson(res, 200, {
    ok: true,
    addedToHumanVerified: humanResult.added,
    humanVerifiedCount: humanResult.count,
    removedFromFailed: failedRemoval.removed,
    removedFromUnderReview: reviewRemoval.removed,
    message: reviewRemoval.removed
      ? `${channel.name} is now Human Verified and removed from Under Review.`
      : `${channel.name} is now Human Verified.`,
    githubSync,
  });
}

async function handleReportFailure(req, res) {
  const body = await readBody(req);
  const userId = String(body.userId || '').trim();
  const channelId = String(body.channelId || '').trim();
  const reason = String(body.reason || '').trim();
  if (!userId || !channelId) {
    return sendJson(res, 400, { error: 'userId and channelId are required' });
  }

  const catalog = await loadCatalogAndPrune();
  const channel = catalog.channels.find(item => item.id === channelId);
  if (!channel) {
    return sendJson(res, 404, { removedGlobally: true, message: 'Channel already removed from backend' });
  }

  const aiSet = new Set(catalog.aiVerifiedChannelIds);
  const humanSet = new Set(catalog.humanVerifiedChannelIds);
  const reviewSet = new Set(catalog.underReviewChannelIds);

  const failedUpsert = await upsertChannelInSynthetic(SYNTHETIC.failed, channel);
  let removedFromAiVerified = false;
  let addedToUnderReview = false;

  if (aiSet.has(channelId)) {
    const removal = await removeChannelFromSynthetic(SYNTHETIC.ai, channelId);
    removedFromAiVerified = removal.removed;
    const reviewUpsert = await upsertChannelInSynthetic(SYNTHETIC.review, channel);
    addedToUnderReview = reviewUpsert.added || reviewSet.has(channelId);
  }

  const messageParts = [];
  messageParts.push(`${channel.name} failed and was moved to Failed.`);
  if (removedFromAiVerified) {
    messageParts.push('It was AI Verified before, so it is now Under Review.');
  }
  if (humanSet.has(channelId) && !removedFromAiVerified) {
    messageParts.push('It stays Human Verified until it is permanently confirmed dead.');
  }

  const githubSync = await flushGitHubSyncQueueSafe();
  sendJson(res, 200, {
    ok: true,
    removedGlobally: false,
    addedToFailed: failedUpsert.added,
    removedFromAiVerified,
    addedToUnderReview,
    message: messageParts.join(' '),
    githubSync,
  });
}

async function handleBotStatus(_req, res) {
  sendJson(res, 200, {
    ...(await readJson(BOT_STATUS_PATH, {
      running: botRuntime.running,
      scheduled: botRuntime.scheduled,
      lastStartedAt: botRuntime.lastStartedAt,
      lastFinishedAt: botRuntime.lastFinishedAt,
      lastError: botRuntime.lastError,
      lastSummary: botRuntime.lastSummary,
      intervalMinutes: BOT_INTERVAL_MINUTES,
      botName: AI_BOT_NAME,
    })),
    githubSync: {
      enabled: githubSyncRuntime.enabled,
      branch: GITHUB_BRANCH,
      repo: githubSyncRuntime.enabled ? `${GITHUB_OWNER}/${GITHUB_REPO}` : null,
      pendingCount: githubSyncRuntime.pending.size,
      flushing: githubSyncRuntime.flushing,
      lastStartedAt: githubSyncRuntime.lastStartedAt,
      lastFinishedAt: githubSyncRuntime.lastFinishedAt,
      lastError: githubSyncRuntime.lastError,
      lastResults: githubSyncRuntime.lastResults,
    },
  });
}


async function handleGitHubSyncStatus(_req, res) {
  sendJson(res, 200, {
    ok: true,
    enabled: githubSyncRuntime.enabled,
    branch: GITHUB_BRANCH,
    repo: githubSyncRuntime.enabled ? `${GITHUB_OWNER}/${GITHUB_REPO}` : null,
    pendingCount: githubSyncRuntime.pending.size,
    flushing: githubSyncRuntime.flushing,
    lastStartedAt: githubSyncRuntime.lastStartedAt,
    lastFinishedAt: githubSyncRuntime.lastFinishedAt,
    lastError: githubSyncRuntime.lastError,
    lastResults: githubSyncRuntime.lastResults,
  });
}

async function handleRunGitHubSync(_req, res) {
  const result = await flushGitHubSyncQueueSafe();
  sendJson(res, 200, { ok: !result.error, ...result });
}

async function handleRunBot(_req, res) {
  const summary = await runBotCycle({ manual: true });
  const githubSync = await flushGitHubSyncQueueSafe();
  sendJson(res, 200, { ok: true, summary, githubSync });
}

async function serveStatic(_req, res, requestPath) {
  const cleanPath = requestPath === '/' ? '/index.html' : requestPath;
  const resolved = path.join(ROOT, cleanPath.replace(/^\/+/, ''));
  if (!resolved.startsWith(ROOT)) return notFound(res);

  try {
    const stat = await fsp.stat(resolved);
    if (stat.isDirectory()) {
      return serveStatic(null, res, path.join(cleanPath, 'index.html'));
    }
    const ext = path.extname(resolved).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';
    const data = await fsp.readFile(resolved);
    res.writeHead(200, {
      'Content-Type': contentType,
      'Content-Length': data.length,
      'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=300',
    });
    res.end(data);
  } catch {
    notFound(res);
  }
}

async function route(req, res) {
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  const urlObj = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = decodeURIComponent(urlObj.pathname);

  if (req.method === 'GET' && pathname === '/api/catalog') return handleCatalog(req, res);
  if (req.method === 'POST' && pathname === '/api/report-success') return handleReportSuccess(req, res);
  if (req.method === 'POST' && pathname === '/api/report-failure') return handleReportFailure(req, res);
  if (req.method === 'GET' && pathname === '/api/bot-status') return handleBotStatus(req, res);
  if (req.method === 'GET' && pathname === '/api/github-sync-status') return handleGitHubSyncStatus(req, res);
  if (req.method === 'POST' && pathname === '/api/run-bot') return handleRunBot(req, res);
  if (req.method === 'POST' && pathname === '/api/run-github-sync') return handleRunGitHubSync(req, res);
  if (req.method === 'GET' && pathname === '/proxy') return handleProxy(req, res, urlObj);
  if (req.method === 'GET' && pathname === '/health') {
    return sendJson(res, 200, {
      ok: true,
      port: PORT,
      botName: AI_BOT_NAME,
      botIntervalMinutes: BOT_INTERVAL_MINUTES,
      aiVerifiedPlaylist: SYNTHETIC.ai.filename,
      humanVerifiedPlaylist: SYNTHETIC.human.filename,
      underReviewPlaylist: SYNTHETIC.review.filename,
      failedPlaylist: SYNTHETIC.failed.filename,
      permanentDeleteAfterAiFailures: AI_FAILURE_THRESHOLD,
      githubSyncEnabled: githubSyncRuntime.enabled,
    });
  }

  return serveStatic(req, res, pathname);
}

async function start() {
  updateGitHubSyncEnabledState();
  await ensurePaths();
  if (githubSyncRuntime.enabled) {
    queueGitHubSync('Playlist/playlists.json');
    for (const def of Object.values(SYNTHETIC)) queueGitHubSync(`Playlist/${def.filename}`);
    flushGitHubSyncQueueSafe().catch(() => {});
  }
  scheduleBotCycle();

  const server = http.createServer((req, res) => {
    route(req, res).catch(error => {
      console.error(error);
      sendJson(res, 500, { error: 'Internal server error', detail: error.message });
    });
  });

  server.listen(PORT, HOST, () => {
    console.log(`Kestford OS running on http://${HOST}:${PORT}`);
    console.log(`${AI_BOT_NAME} bot checks channels every ${BOT_INTERVAL_MINUTES} minute(s).`);
  });
}

start().catch(error => {
  console.error(error);
  process.exit(1);
});
