'use strict';

const API_CATALOG = '/api/catalog';
const API_REPORT_FAILURE = '/api/report-failure';
const API_REPORT_SUCCESS = '/api/report-success';
const HIDDEN_KEY = 'kos_hidden_channels';
const RECENT_KEY = 'kos_recent';
const USER_ID_KEY = 'kos_user_id';
const CONTROLS_TIMEOUT = 3500;
const FAILURE_TIMEOUT_MS = 18000;
const HUMAN_VERIFIED_PLAYLIST_ID = 'human_verified';
const AI_VERIFIED_PLAYLIST_ID = 'ai_verified';
const UNDER_REVIEW_PLAYLIST_ID = 'under_review';

const COUNTRY_NAMES = {
  AE: 'United Arab Emirates',
  AU: 'Australia',
  BH: 'Bahrain',
  BD: 'Bangladesh',
  CA: 'Canada',
  CN: 'China',
  DE: 'Germany',
  EG: 'Egypt',
  ES: 'Spain',
  FR: 'France',
  GB: 'United Kingdom',
  IN: 'India',
  IR: 'Iran',
  IQ: 'Iraq',
  IL: 'Israel',
  JP: 'Japan',
  KW: 'Kuwait',
  NP: 'Nepal',
  OM: 'Oman',
  PK: 'Pakistan',
  QA: 'Qatar',
  SA: 'Saudi Arabia',
  US: 'United States'
};

const state = {
  playlists: [],
  channels: [],
  filteredChannels: [],
  currentList: [],
  currentIndex: -1,
  currentChannelId: null,
  hiddenChannels: new Set(),
  recentChannels: [],
  humanVerifiedChannelIds: new Set(),
  aiVerifiedChannelIds: new Set(),
  underReviewChannelIds: new Set(),
  activePlaylist: 'all',
  activeType: 'all',
  searchQuery: '',
  verifiedOnly: false,
  userId: '',
  hlsLevel: -1,
  isPlaying: false,
  wasPlayingBeforeHide: false,
  volume: 1,
  brightness: 1,
  isFitCover: false,
  isPlayerFolded: false,
  hlsInstance: null,
  controlsTimer: null,
  failureToken: 0,
  failureHandledToken: null,
  failureTimeout: null,
};

const video = document.getElementById('main-video');
const overlay = document.getElementById('player-overlay');
const loader = document.getElementById('loader');
const loaderText = document.getElementById('loader-text');
const errorState = document.getElementById('error-state');
const chNumber = document.getElementById('ch-number');
const chName = document.getElementById('ch-name');
const chPlaylistName = document.getElementById('ch-playlist-name');
const chFlag = document.getElementById('channel-country-flag');
const playPauseBtn = document.getElementById('play-pause-btn');
const playIcon = document.getElementById('play-icon');
const pauseIcon = document.getElementById('pause-icon');
const prevBtn = document.getElementById('prev-btn');
const nextBtn = document.getElementById('next-btn');
const fullscreenBtn = document.getElementById('fullscreen-btn');
const minimizeBtn = document.getElementById('minimize-btn');
const filterToggle = document.getElementById('filter-toggle-btn');
const filterPanel = document.getElementById('filter-panel');
const searchInput = document.getElementById('search-input');
const searchClear = document.getElementById('search-clear');
const playlistGrid = document.getElementById('country-grid');
const playlistStrip = document.getElementById('playlist-strip');
const typeBtns = document.querySelectorAll('.type-btn');
const applyFilterBtn = document.getElementById('apply-filter-btn');
const clearFilterBtn = document.getElementById('clear-filter-btn');
const secRecent = document.getElementById('sec-recent');
const rowRecent = document.getElementById('row-recent');
const gridAll = document.getElementById('grid-all');
const chCountLabel = document.getElementById('ch-count-label');
const noResults = document.getElementById('no-results');
const emptyLibrary = document.getElementById('empty-library');
const brightnessOverlay = document.getElementById('brightness-overlay');
const volIndicator = document.getElementById('vol-indicator');
const brightIndicator = document.getElementById('bright-indicator');
const volBarFill = document.getElementById('vol-bar-fill');
const brightBarFill = document.getElementById('bright-bar-fill');
const volValue = document.getElementById('vol-value');
const brightValue = document.getElementById('bright-value');
const fitHint = document.getElementById('fit-hint');
const fitHintText = document.getElementById('fit-hint-text');
const swipeHintText = document.getElementById('swipe-hint-text');
const langBtn = document.getElementById('lang-btn');
const resBtn = document.getElementById('res-btn');
const resLabel = document.getElementById('res-label');
const langPopup = document.getElementById('lang-popup');
const resPopup = document.getElementById('res-popup');
const langList = document.getElementById('lang-list');
const resList = document.getElementById('res-list');
const toast = document.getElementById('toast');
const wrapper = document.getElementById('player-wrapper');

const GROUP_MAP = {
  Top: { row: document.getElementById('row-top'), section: document.getElementById('sec-top') },
  News: { row: document.getElementById('row-news'), section: document.getElementById('sec-news') },
  Sports: { row: document.getElementById('row-sports'), section: document.getElementById('sec-sports') },
  Documentary: { row: document.getElementById('row-documentary'), section: document.getElementById('sec-documentary') },
  Movies: { row: document.getElementById('row-movies'), section: document.getElementById('sec-movies') },
  Songs: { row: document.getElementById('row-songs'), section: document.getElementById('sec-songs') },
  Cartoons: { row: document.getElementById('row-cartoons'), section: document.getElementById('sec-cartoons') },
};

function sanitizeId(value) {
  return String(value || '').replace(/[^a-z0-9]+/gi, '_').replace(/^_+|_+$/g, '').toLowerCase();
}

function getUserId() {
  const existing = localStorage.getItem(USER_ID_KEY);
  if (existing) return existing;
  const generated = 'user_' + Math.random().toString(36).slice(2) + Date.now().toString(36);
  localStorage.setItem(USER_ID_KEY, generated);
  return generated;
}

function loadHiddenChannels() {
  try {
    const raw = JSON.parse(localStorage.getItem(HIDDEN_KEY) || '[]');
    return new Set(Array.isArray(raw) ? raw : []);
  } catch {
    return new Set();
  }
}

function saveHiddenChannels() {
  localStorage.setItem(HIDDEN_KEY, JSON.stringify([...state.hiddenChannels]));
}

function hideChannelForUser(channelId) {
  state.hiddenChannels.add(channelId);
  saveHiddenChannels();
}

function loadRecentChannels() {
  try {
    const raw = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]');
    return Array.isArray(raw) ? raw : [];
  } catch {
    return [];
  }
}

function saveRecentChannels() {
  localStorage.setItem(RECENT_KEY, JSON.stringify(state.recentChannels.slice(0, 12)));
}

function showToast(message) {
  toast.textContent = message;
  toast.classList.add('show');
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => toast.classList.remove('show'), 2600);
}

function setLoader(active, text = 'Loading…') {
  loader.classList.toggle('active', active);
  loaderText.textContent = text;
}

function setError(active) {
  errorState.classList.toggle('active', active);
}

function setPlaying(active) {
  state.isPlaying = active;
  playIcon.style.display = active ? 'none' : 'block';
  pauseIcon.style.display = active ? 'block' : 'none';
}

function showControls() {
  overlay.classList.add('controls-visible');
  clearTimeout(state.controlsTimer);
  if (state.isPlayerFolded) return;
  state.controlsTimer = setTimeout(() => {
    if (state.isPlaying && !state.isPlayerFolded) overlay.classList.remove('controls-visible');
  }, CONTROLS_TIMEOUT);
}

function getPlaylistFlag(code = '') {
  const cc = String(code || '').trim().toLowerCase();
  return cc.length === 2 ? `https://flagcdn.com/w40/${cc}.png` : '';
}

function getPlaylistIconPath(playlist) {
  if (playlist && playlist.icon) return playlist.icon;
  if (playlist && playlist.id === HUMAN_VERIFIED_PLAYLIST_ID) return '/assets/icons/playlist-human.svg';
  if (playlist && playlist.id === AI_VERIFIED_PLAYLIST_ID) return '/assets/icons/playlist-ai.svg';
  if (playlist && playlist.id === UNDER_REVIEW_PLAYLIST_ID) return '/assets/icons/playlist-review.svg';
  return '/assets/icons/playlist.svg';
}

function isHumanVerified(channelId) {
  return state.humanVerifiedChannelIds.has(channelId);
}

function isAiVerified(channelId) {
  return state.aiVerifiedChannelIds.has(channelId);
}

function isVerified(channelId) {
  return isHumanVerified(channelId) || isAiVerified(channelId);
}

function isUnderReview(channelId) {
  return state.underReviewChannelIds.has(channelId);
}

function getChannelBadges(channelId) {
  const badges = [];
  if (isUnderReview(channelId)) badges.push('<span class="status-badge review">UNDER REVIEW</span>');
  if (isHumanVerified(channelId)) badges.push('<span class="status-badge human">HUMAN</span>');
  if (isAiVerified(channelId)) badges.push('<span class="status-badge ai">AI</span>');
  return badges.join(' ');
}

function updateFlagUI(channel) {
  chFlag.innerHTML = '';
  const code = String(channel?.playlistCode || channel?.countryCode || '').trim().toUpperCase();
  const countryName = channel?.playlist || COUNTRY_NAMES[code] || channel?.countryName || '';
  const flagUrl = getPlaylistFlag(code);

  if (flagUrl) {
    const img = document.createElement('img');
    img.src = flagUrl;
    img.alt = countryName;
    img.onerror = () => img.remove();
    chFlag.appendChild(img);
  }

  if (countryName) {
    const span = document.createElement('span');
    span.textContent = countryName;
    chFlag.appendChild(span);
  }
}

function getPlayableUrl(url) {
  if (/^https?:\/\//i.test(url)) {
    return `/proxy?url=${encodeURIComponent(url)}`;
  }
  return url;
}

async function fetchCatalog() {
  const res = await fetch(`${API_CATALOG}?t=${Date.now()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error('Failed to load catalog');
  return res.json();
}

function hasActiveFilter() {
  return Boolean(state.searchQuery.trim()) || state.activePlaylist !== 'all' || state.activeType !== 'all' || state.verifiedOnly;
}

function getVisibleChannels() {
  return state.channels.filter(channel => !state.hiddenChannels.has(channel.id));
}

function getActivePool() {
  return hasActiveFilter() ? state.filteredChannels : getVisibleChannels();
}

async function refreshCatalog({ keepCurrent = true, silent = false, autoPlayFallback = true, preferredChannelId = null } = {}) {
  if (!silent) setLoader(true, 'Loading playlists…');
  const previousChannelId = keepCurrent ? state.currentChannelId : null;
  const payload = await fetchCatalog();

  state.playlists = Array.isArray(payload.playlists) ? payload.playlists : [];
  state.channels = Array.isArray(payload.channels) ? payload.channels : [];
  state.humanVerifiedChannelIds = new Set(Array.isArray(payload.humanVerifiedChannelIds) ? payload.humanVerifiedChannelIds : []);
  state.aiVerifiedChannelIds = new Set(Array.isArray(payload.aiVerifiedChannelIds) ? payload.aiVerifiedChannelIds : []);
  state.underReviewChannelIds = new Set(Array.isArray(payload.underReviewChannelIds) ? payload.underReviewChannelIds : []);

  if (!state.playlists.find(playlist => playlist.id === state.activePlaylist) && state.activePlaylist !== 'all') {
    state.activePlaylist = 'all';
  }

  renderPlaylistSelectors();
  applyFilters();
  renderRecentRow();

  const preferredStillVisible = preferredChannelId && state.channels.some(ch => ch.id === preferredChannelId) && !state.hiddenChannels.has(preferredChannelId);
  if (preferredStillVisible) {
    playChannelById(preferredChannelId, { autoPlay: true });
  } else {
    const currentStillVisible = previousChannelId && state.channels.some(ch => ch.id === previousChannelId) && !state.hiddenChannels.has(previousChannelId);
    if (currentStillVisible) {
      const current = state.channels.find(ch => ch.id === previousChannelId);
      if (current) {
        state.currentChannelId = previousChannelId;
        updateNowPlayingUI(current);
        markActiveCard(current.id);
      }
    } else if (autoPlayFallback && state.filteredChannels.length > 0) {
      state.currentChannelId = null;
      playChannelById(state.filteredChannels[0].id, { autoPlay: true });
    } else if (!state.filteredChannels.length) {
      resetPlayerUI();
    }
  }

  if (!silent) setLoader(false);
}

function applyFilters() {
  const baseChannels = getVisibleChannels();
  const query = state.searchQuery.trim().toLowerCase();

  let working = baseChannels;

  if (query) {
    working = working.filter(channel => {
      return [channel.name, channel.group, channel.playlist, channel.countryName, channel.tvgName].some(value =>
        String(value || '').toLowerCase().includes(query)
      );
    });
  }

  if (state.activePlaylist === HUMAN_VERIFIED_PLAYLIST_ID) {
    working = working.filter(channel => state.humanVerifiedChannelIds.has(channel.id));
  } else if (state.activePlaylist === AI_VERIFIED_PLAYLIST_ID) {
    working = working.filter(channel => state.aiVerifiedChannelIds.has(channel.id));
  } else if (state.activePlaylist === UNDER_REVIEW_PLAYLIST_ID) {
    working = working.filter(channel => state.underReviewChannelIds.has(channel.id));
  } else if (state.activePlaylist !== 'all') {
    working = working.filter(channel => channel.playlistId === state.activePlaylist);
  }

  if (state.activeType !== 'all') {
    working = working.filter(channel => channel.group === state.activeType);
  }

  if (state.verifiedOnly) {
    working = working.filter(channel => isVerified(channel.id));
  }

  state.filteredChannels = working;
  state.currentList = working;
  renderSections();
}

function clearAllFilters() {
  state.activePlaylist = 'all';
  state.activeType = 'all';
  state.searchQuery = '';
  state.verifiedOnly = false;
  searchInput.value = '';
  searchClear.style.display = 'none';
  typeBtns.forEach(btn => btn.classList.toggle('active', btn.dataset.type === 'all'));
  renderPlaylistSelectors();
  applyFilters();
}
window.clearAllFilters = clearAllFilters;

function renderPlaylistSelectors() {
  const baseChannels = getVisibleChannels();
  const counts = new Map();
  for (const channel of baseChannels) {
    counts.set(channel.playlistId, (counts.get(channel.playlistId) || 0) + 1);
  }
  const humanVerifiedCount = baseChannels.filter(channel => isHumanVerified(channel.id)).length;
  const aiVerifiedCount = baseChannels.filter(channel => isAiVerified(channel.id)).length;
  const underReviewCount = baseChannels.filter(channel => isUnderReview(channel.id)).length;

  const makePill = (playlist, countValue) => {
    const button = document.createElement('button');
    button.className = 'playlist-pill' + (state.activePlaylist === playlist.id ? ' active' : '');
    button.dataset.playlist = playlist.id;
    const flagUrl = getPlaylistFlag(playlist.code);
    const iconUrl = getPlaylistIconPath(playlist);
    button.innerHTML = `
      <img class="playlist-pill-icon" src="${iconUrl}" alt="" aria-hidden="true">
      ${flagUrl ? `<img class="playlist-pill-flag" src="${flagUrl}" alt="${playlist.name}" onerror="this.remove()">` : ''}
      <span class="playlist-pill-label">${playlist.name}</span>
      <span class="playlist-count">${countValue || 0}</span>
    `;
    button.addEventListener('click', () => {
      state.activePlaylist = playlist.id;
      renderPlaylistSelectors();
      applyFilters();
      document.getElementById('sec-all').scrollIntoView({ behavior: 'smooth' });
    });
    return button;
  };

  const makeVerifiedOnlyPill = () => {
    const button = document.createElement('button');
    button.className = 'playlist-pill verified-only-pill' + (state.verifiedOnly ? ' active' : '');
    button.type = 'button';
    let scopedChannels = baseChannels;
    if (state.activePlaylist === HUMAN_VERIFIED_PLAYLIST_ID) {
      scopedChannels = baseChannels.filter(channel => isHumanVerified(channel.id));
    } else if (state.activePlaylist === AI_VERIFIED_PLAYLIST_ID) {
      scopedChannels = baseChannels.filter(channel => isAiVerified(channel.id));
    } else if (state.activePlaylist === UNDER_REVIEW_PLAYLIST_ID) {
      scopedChannels = baseChannels.filter(channel => isUnderReview(channel.id));
    } else if (state.activePlaylist !== 'all') {
      scopedChannels = baseChannels.filter(channel => channel.playlistId === state.activePlaylist);
    }
    const verifiedCount = scopedChannels.filter(channel => isVerified(channel.id)).length;
    button.innerHTML = `
      <span class="playlist-pill-check" aria-hidden="true">✓</span>
      <span class="playlist-pill-label">Verified Only</span>
      <span class="playlist-count">${verifiedCount}</span>
    `;
    button.addEventListener('click', () => {
      state.verifiedOnly = !state.verifiedOnly;
      renderPlaylistSelectors();
      applyFilters();
      document.getElementById('sec-all').scrollIntoView({ behavior: 'smooth' });
    });
    return button;
  };

  playlistStrip.innerHTML = '';
  playlistGrid.innerHTML = '';

  const allItem = { id: 'all', name: 'All Playlists', code: '', icon: '/assets/icons/playlist.svg' };
  playlistStrip.appendChild(makePill(allItem, baseChannels.length));
  playlistGrid.appendChild(makePill(allItem, baseChannels.length));

  state.playlists.forEach(playlist => {
    let count = counts.get(playlist.id) || playlist.channelCount || 0;
    if (playlist.id === HUMAN_VERIFIED_PLAYLIST_ID) count = humanVerifiedCount;
    if (playlist.id === AI_VERIFIED_PLAYLIST_ID) count = aiVerifiedCount;
    if (playlist.id === UNDER_REVIEW_PLAYLIST_ID) count = underReviewCount;
    playlistStrip.appendChild(makePill(playlist, count));
    playlistGrid.appendChild(makePill(playlist, count));
  });

  playlistStrip.appendChild(makeVerifiedOnlyPill());
  playlistGrid.appendChild(makeVerifiedOnlyPill());
}

function renderSections() {
  const visibleChannels = getVisibleChannels();
  const hasLibrary = visibleChannels.length > 0;
  emptyLibrary.style.display = hasLibrary ? 'none' : '';
  noResults.style.display = '';

  Object.values(GROUP_MAP).forEach(({ row, section }) => {
    row.innerHTML = '';
    section.style.display = 'none';
  });
  gridAll.innerHTML = '';

  if (!hasLibrary) {
    chCountLabel.textContent = '0 channels';
    return;
  }

  const queryActive = Boolean(state.searchQuery.trim());
  const filterActive = queryActive || state.activePlaylist !== 'all' || state.activeType !== 'all' || state.verifiedOnly;
  const displayChannels = state.filteredChannels;
  const isEmpty = filterActive && displayChannels.length === 0;
  noResults.style.display = isEmpty ? '' : 'none';

  const channelsForCategories = queryActive ? displayChannels : (state.activePlaylist === 'all' && state.activeType === 'all' ? visibleChannels : displayChannels);

  for (const [group, config] of Object.entries(GROUP_MAP)) {
    const items = channelsForCategories.filter(channel => channel.group === group).slice(0, 8);
    if (items.length === 0) continue;
    config.section.style.display = '';
    items.forEach(channel => config.row.appendChild(createCard(channel)));
  }

  displayChannels.forEach(channel => gridAll.appendChild(createCard(channel)));
  const labelParts = [`${displayChannels.length} channel${displayChannels.length === 1 ? '' : 's'}`];
  if (!queryActive && state.activePlaylist !== 'all') {
    const playlist = state.playlists.find(item => item.id === state.activePlaylist);
    if (playlist) labelParts.push(playlist.name);
  } else if (queryActive) {
    labelParts.push('global search');
  }
  if (state.verifiedOnly) labelParts.push('verified only');
  chCountLabel.textContent = labelParts.join(' • ');

  if (state.currentChannelId) markActiveCard(state.currentChannelId);
}

function createCard(channel) {
  const card = document.createElement('div');
  card.className = 'ch-card';
  card.dataset.id = channel.id;

  const flagUrl = getPlaylistFlag(channel.playlistCode || channel.countryCode || '');
  const groupIcon = getGroupIconPath(channel.group);
  const badges = getChannelBadges(channel.id);

  card.innerHTML = `
    <div class="ch-card-thumb">
      ${channel.logo ? `<img src="${channel.logo}" alt="${channel.name}" loading="lazy" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">` : ''}
      <div class="thumb-fallback" style="${channel.logo ? 'display:none' : ''}"><img class="thumb-fallback-icon" src="${groupIcon}" alt="${channel.group || 'Channel'}"></div>
    </div>
    <div class="ch-card-info">
      <div class="ch-card-playlist">
        <span>${channel.playlist}</span>
        ${badges ? `<span class="ch-card-badges">${badges}</span>` : ''}
      </div>
      <div class="ch-card-name">${channel.name}</div>
      <div class="ch-card-meta">
        ${flagUrl ? `<img class="ch-card-flag" src="${flagUrl}" alt="${channel.playlist}" onerror="this.remove()">` : ''}
        <span class="ch-card-cat">${channel.group}</span>
      </div>
    </div>
  `;

  card.addEventListener('click', () => {
    playChannelById(channel.id, { autoPlay: true });
    window.scrollTo({ top: 0, behavior: 'smooth' });
  });

  return card;
}

function getGroupIconPath(group) {
  const map = {
    Top: '/assets/icons/top.svg',
    News: '/assets/icons/news.svg',
    Sports: '/assets/icons/sports.svg',
    Documentary: '/assets/icons/documentary.svg',
    Movies: '/assets/icons/movies.svg',
    Songs: '/assets/icons/songs.svg',
    Cartoons: '/assets/icons/cartoons.svg'
  };
  return map[group] || '/assets/icons/all-channels.svg';
}

function markActiveCard(channelId) {
  document.querySelectorAll('.ch-card').forEach(card => {
    card.classList.toggle('playing', card.dataset.id === channelId);
  });
}

function resetPlayerUI() {
  chNumber.textContent = 'CH 00';
  chName.textContent = 'No channel available';
  chPlaylistName.textContent = 'Add or repair playlist files';
  chFlag.innerHTML = '';
  setPlaying(false);
  setLoader(false);
  setError(false);
  markActiveCard('');
}

function addToRecent(channelId) {
  state.recentChannels = state.recentChannels.filter(id => id !== channelId);
  state.recentChannels.unshift(channelId);
  state.recentChannels = state.recentChannels.slice(0, 10);
  saveRecentChannels();
  renderRecentRow();
}

function renderRecentRow() {
  const channels = state.recentChannels.map(id => state.channels.find(channel => channel.id === id)).filter(channel => channel && !state.hiddenChannels.has(channel.id)).slice(0, 8);
  rowRecent.innerHTML = '';
  secRecent.style.display = channels.length ? '' : 'none';
  channels.forEach(channel => rowRecent.appendChild(createCard(channel)));
}

function updateNowPlayingUI(channel) {
  const currentList = state.currentList.length ? state.currentList : getActivePool();
  const index = currentList.findIndex(item => item.id === channel.id);
  chNumber.textContent = 'CH ' + String(index >= 0 ? index + 1 : 1).padStart(2, '0');
  chName.textContent = channel.name;
  const statusParts = [];
  if (isUnderReview(channel.id)) statusParts.push('Under Review');
  if (isHumanVerified(channel.id)) statusParts.push('Human Verified');
  if (isAiVerified(channel.id)) statusParts.push('AI Verified');
  chPlaylistName.textContent = `${channel.playlist} • ${channel.group}${statusParts.length ? ` • ${statusParts.join(' • ')}` : ''}`;
  updateFlagUI(channel);
  markActiveCard(channel.id);
}

function clearFailureTimeout() {
  clearTimeout(state.failureTimeout);
  state.failureTimeout = null;
}

function armFailureTimeout(channel, token) {
  clearFailureTimeout();
  state.failureTimeout = setTimeout(() => {
    handleChannelFailure(channel, 'timeout', token);
  }, FAILURE_TIMEOUT_MS);
}

function destroyHls() {
  if (state.hlsInstance) {
    state.hlsInstance.destroy();
    state.hlsInstance = null;
  }
}

function playChannelById(channelId, { autoPlay = true } = {}) {
  const channel = state.channels.find(item => item.id === channelId);
  if (!channel) return;

  const displayList = getActivePool();
  state.currentList = displayList;
  state.currentIndex = displayList.findIndex(item => item.id === channelId);
  state.currentChannelId = channelId;
  state.failureToken += 1;
  const token = state.failureToken;
  state.failureHandledToken = null;

  updateNowPlayingUI(channel);
  addToRecent(channel.id);
  setLoader(true, `Loading ${channel.name}…`);
  setError(false);
  showControls();
  clearFailureTimeout();
  destroyHls();

  const sourceUrl = getPlayableUrl(channel.url);
  const shouldUseHls = /\.m3u8($|\?)/i.test(channel.url) || /application\/vnd\.apple\.mpegurl/i.test(channel.mimeType || '');
  armFailureTimeout(channel, token);

  if (shouldUseHls && window.Hls && Hls.isSupported()) {
    const hls = new Hls({
      maxBufferLength: 30,
      maxMaxBufferLength: 60,
      startLevel: -1,
      xhrSetup: xhr => { xhr.timeout = 15000; },
    });

    state.hlsInstance = hls;
    hls.loadSource(sourceUrl);
    hls.attachMedia(video);

    hls.on(Hls.Events.MANIFEST_PARSED, () => {
      state.hlsLevel = -1;
      updateQualityLabel();
      clearFailureTimeout();
      setLoader(false);
      if (!autoPlay) return;
      const playPromise = video.play();
      if (playPromise && typeof playPromise.then === 'function') {
        playPromise.then(() => setPlaying(true)).catch(err => {
          if (err && err.name === 'NotAllowedError') {
            setPlaying(false);
            showToast('Tap play to start streaming on this device.');
            return;
          }
          handleChannelFailure(channel, 'playback', token);
        });
      }
    });

    hls.on(Hls.Events.LEVEL_SWITCHED, (_, data) => {
      state.hlsLevel = data.level;
      updateQualityLabel();
    });

    hls.on(Hls.Events.ERROR, (_, data) => {
      if (data && data.fatal) {
        handleChannelFailure(channel, data.type || 'hls_fatal', token);
      }
    });

    return;
  }

  video.src = sourceUrl;
  video.load();
  video.onloadedmetadata = () => {
    clearFailureTimeout();
    setLoader(false);
    if (!autoPlay) return;
    const playPromise = video.play();
    if (playPromise && typeof playPromise.then === 'function') {
      playPromise.then(() => setPlaying(true)).catch(err => {
        if (err && err.name === 'NotAllowedError') {
          setPlaying(false);
          showToast('Tap play to start streaming on this device.');
          return;
        }
        handleChannelFailure(channel, 'native_playback', token);
      });
    }
  };
  video.onerror = () => handleChannelFailure(channel, 'native_error', token);
}

function getNextAvailableChannelIdFromPool(excludeId, pool) {
  if (!pool || !pool.length) return null;
  const index = pool.findIndex(channel => channel.id === excludeId);
  if (index === -1) return pool[0].id;
  for (let step = 1; step <= pool.length; step += 1) {
    const candidate = pool[(index + step) % pool.length];
    if (candidate && candidate.id !== excludeId && !state.hiddenChannels.has(candidate.id)) {
      return candidate.id;
    }
  }
  return null;
}

function getNextAvailableChannelId(excludeId) {
  return getNextAvailableChannelIdFromPool(excludeId, getActivePool());
}

async function reportChannelSuccess(channelId) {
  if (!channelId) return;
  try {
    const res = await fetch(API_REPORT_SUCCESS, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: state.userId, channelId })
    });
    if (!res.ok) return;
    const payload = await res.json();
    if (payload.ok) {
      state.humanVerifiedChannelIds.add(channelId);
      state.underReviewChannelIds.delete(channelId);
      renderPlaylistSelectors();
      renderSections();
    }
  } catch (error) {
    console.error('Failed to report channel success', error);
  }
}

async function handleChannelFailure(channel, reason, token) {
  if (!channel) return;
  if (token !== undefined && token !== state.failureToken) return;
  if (state.failureHandledToken === token) return;
  state.failureHandledToken = token;

  const poolBeforeFailure = state.currentList.length ? [...state.currentList] : [...getActivePool()];
  const nextIdHint = getNextAvailableChannelIdFromPool(channel.id, poolBeforeFailure);

  clearFailureTimeout();
  destroyHls();
  setError(true);
  setLoader(false);
  setPlaying(false);
  hideChannelForUser(channel.id);
  applyFilters();
  showToast(`${channel.name} failed. Moving to the next channel...`);

  try {
    const res = await fetch(API_REPORT_FAILURE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        userId: state.userId,
        channelId: channel.id,
        playlistId: channel.playlistId,
        reason,
      }),
    });

    if (res.ok) {
      const payload = await res.json();
      if (payload.ok) {
        await refreshCatalog({ keepCurrent: false, silent: true, autoPlayFallback: false });
        if (payload.message) {
          showToast(payload.message);
        }
      }
    }
  } catch (err) {
    console.error('Failed to report channel failure', err);
  }

  const nextId = nextIdHint && state.channels.some(ch => ch.id === nextIdHint) && !state.hiddenChannels.has(nextIdHint)
    ? nextIdHint
    : getNextAvailableChannelId(channel.id);

  setTimeout(() => setError(false), 1000);
  if (nextId) {
    setTimeout(() => playChannelById(nextId, { autoPlay: true }), 300);
  } else {
    resetPlayerUI();
  }
}

function skipToPrev() {
  const pool = getActivePool();
  if (!pool.length) return;
  const currentIndex = pool.findIndex(channel => channel.id === state.currentChannelId);
  const index = currentIndex <= 0 ? pool.length - 1 : currentIndex - 1;
  playChannelById(pool[index].id, { autoPlay: true });
}

function skipToNext() {
  const pool = getActivePool();
  if (!pool.length) return;
  const currentIndex = pool.findIndex(channel => channel.id === state.currentChannelId);
  const index = currentIndex === -1 || currentIndex >= pool.length - 1 ? 0 : currentIndex + 1;
  playChannelById(pool[index].id, { autoPlay: true });
}

function togglePlayPause() {
  if (video.paused) {
    video.play().then(() => setPlaying(true)).catch(err => {
      if (err && err.name === 'NotAllowedError') {
        showToast('User interaction is required to start playback.');
      }
    });
  } else {
    video.pause();
    setPlaying(false);
  }
}

function updateQualityLabel() {
  if (!state.hlsInstance || state.hlsLevel === -1) {
    resLabel.textContent = 'Auto';
    return;
  }
  const levels = state.hlsInstance.levels || [];
  const current = levels[state.hlsLevel];
  resLabel.textContent = current && current.height ? `${current.height}p` : 'Auto';
}

function buildQualityList() {
  resList.innerHTML = '';
  if (!state.hlsInstance) {
    const p = document.createElement('p');
    p.textContent = 'Quality control is available for HLS streams.';
    p.style.color = 'var(--text-dim)';
    resList.appendChild(p);
    return;
  }

  const autoBtn = document.createElement('button');
  autoBtn.className = 'res-option' + (state.hlsInstance.currentLevel === -1 ? ' active' : '');
  autoBtn.textContent = 'Auto (Adaptive)';
  autoBtn.addEventListener('click', () => {
    state.hlsInstance.currentLevel = -1;
    state.hlsLevel = -1;
    updateQualityLabel();
    resPopup.classList.add('hidden');
  });
  resList.appendChild(autoBtn);

  (state.hlsInstance.levels || []).forEach((level, index) => {
    const btn = document.createElement('button');
    btn.className = 'res-option' + (state.hlsInstance.currentLevel === index ? ' active' : '');
    btn.textContent = `${level.height || '?'}p — ${Math.round((level.bitrate || 0) / 1000)} kbps`;
    btn.addEventListener('click', () => {
      state.hlsInstance.currentLevel = index;
      state.hlsLevel = index;
      updateQualityLabel();
      resPopup.classList.add('hidden');
    });
    resList.appendChild(btn);
  });
}

function buildLangList() {
  langList.innerHTML = '';
  if (!state.hlsInstance || !(state.hlsInstance.audioTracks || []).length) {
    const p = document.createElement('p');
    p.textContent = 'No alternate audio tracks available.';
    p.style.color = 'var(--text-dim)';
    langList.appendChild(p);
    return;
  }

  state.hlsInstance.audioTracks.forEach((track, index) => {
    const btn = document.createElement('button');
    btn.className = 'lang-option' + (state.hlsInstance.audioTrack === index ? ' active' : '');
    btn.textContent = track.name || track.lang || `Track ${index + 1}`;
    btn.addEventListener('click', () => {
      state.hlsInstance.audioTrack = index;
      langPopup.classList.add('hidden');
    });
    langList.appendChild(btn);
  });
}


function updateFoldUI() {
  document.body.classList.toggle('player-folded', state.isPlayerFolded);
  overlay.classList.add('controls-visible');
  fullscreenBtn.title = state.isPlayerFolded ? 'Unfold Player' : 'Fold Player';
  fullscreenBtn.setAttribute('aria-label', fullscreenBtn.title);
  if (swipeHintText) {
    swipeHintText.textContent = state.isPlayerFolded ? 'Swipe down to unfold player' : 'Swipe up to fold player';
  }
}

function setPlayerFolded(shouldFold) {
  state.isPlayerFolded = Boolean(shouldFold);
  updateFoldUI();
  if (state.isPlayerFolded) {
    clearTimeout(state.controlsTimer);
  } else {
    showControls();
  }
}

function togglePlayerFold(forceValue) {
  const nextValue = typeof forceValue === 'boolean' ? forceValue : !state.isPlayerFolded;
  if (nextValue === state.isPlayerFolded) return;
  setPlayerFolded(nextValue);
}

function enterFullscreen() {
  const element = document.documentElement;
  const request = element.requestFullscreen || element.webkitRequestFullscreen || element.mozRequestFullScreen;
  if (request) request.call(element);
  document.body.classList.add('in-fullscreen');
  if (screen.orientation && screen.orientation.lock) {
    screen.orientation.lock('landscape').catch(() => {});
  }
}

function exitFullscreen() {
  const exit = document.exitFullscreen || document.webkitExitFullscreen || document.mozCancelFullScreen;
  if (exit) exit.call(document);
  document.body.classList.remove('in-fullscreen');
  if (screen.orientation && screen.orientation.unlock) {
    screen.orientation.unlock();
  }
}

document.addEventListener('fullscreenchange', () => {
  if (!document.fullscreenElement) {
    document.body.classList.remove('in-fullscreen');
  }
});

function applyBrightness() {
  brightnessOverlay.style.background = `rgba(0,0,0,${1 - state.brightness})`;
}

function hideIndicators() {
  volIndicator.classList.remove('visible');
  brightIndicator.classList.remove('visible');
}

function showIndicator(type, value) {
  const pct = Math.round(value * 100);
  clearTimeout(showIndicator.timer);

  if (type === 'vol') {
    volIndicator.classList.add('visible');
    brightIndicator.classList.remove('visible');
    volBarFill.style.height = `${pct}%`;
    volValue.textContent = `${pct}%`;
  } else {
    brightIndicator.classList.add('visible');
    volIndicator.classList.remove('visible');
    brightBarFill.style.height = `${pct}%`;
    brightValue.textContent = `${pct}%`;
  }

  showIndicator.timer = setTimeout(hideIndicators, 1500);
}

function toggleFitToScreen() {
  state.isFitCover = !state.isFitCover;
  video.classList.toggle('fit-cover', state.isFitCover);
  fitHintText.textContent = state.isFitCover ? 'Fill Screen' : 'Fit to Screen';
  fitHint.classList.add('visible');
  setTimeout(() => fitHint.classList.remove('visible'), 1500);
}

function bindEvents() {
  prevBtn.addEventListener('click', () => { skipToPrev(); showControls(); });
  nextBtn.addEventListener('click', () => { skipToNext(); showControls(); });
  playPauseBtn.addEventListener('click', () => { togglePlayPause(); showControls(); });
  fullscreenBtn.addEventListener('click', () => {
    togglePlayerFold();
    showControls();
  });
  minimizeBtn.addEventListener('click', () => {
    togglePlayerFold(false);
    showControls();
  });

  video.addEventListener('play', () => setPlaying(true));
  video.addEventListener('pause', () => setPlaying(false));
  video.addEventListener('waiting', () => setLoader(true, 'Buffering…'));
  video.addEventListener('playing', () => {
    clearFailureTimeout();
    setLoader(false);
    setError(false);
    reportChannelSuccess(state.currentChannelId);
  });

  filterToggle.addEventListener('click', () => filterPanel.classList.toggle('hidden'));

  typeBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      state.activeType = btn.dataset.type;
      typeBtns.forEach(item => item.classList.toggle('active', item === btn));
    });
  });

  applyFilterBtn.addEventListener('click', () => {
    applyFilters();
    filterPanel.classList.add('hidden');
  });
  clearFilterBtn.addEventListener('click', clearAllFilters);

  searchInput.addEventListener('input', () => {
    state.searchQuery = searchInput.value.trim();
    searchClear.style.display = state.searchQuery ? '' : 'none';
    applyFilters();
  });
  searchClear.addEventListener('click', () => {
    searchInput.value = '';
    state.searchQuery = '';
    searchClear.style.display = 'none';
    applyFilters();
  });

  resBtn.addEventListener('click', () => {
    buildQualityList();
    resPopup.classList.remove('hidden');
  });
  langBtn.addEventListener('click', () => {
    buildLangList();
    langPopup.classList.remove('hidden');
  });

  [langPopup, resPopup].forEach(popup => {
    popup.addEventListener('click', event => {
      if (event.target === popup) popup.classList.add('hidden');
    });
  });

  document.querySelectorAll('.see-all-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      state.activeType = btn.dataset.section;
      typeBtns.forEach(item => item.classList.toggle('active', item.dataset.type === state.activeType));
      applyFilters();
      document.getElementById('sec-all').scrollIntoView({ behavior: 'smooth' });
    });
  });

  document.addEventListener('keydown', event => {
    switch (event.key) {
      case 'ArrowLeft':
        skipToPrev();
        break;
      case 'ArrowRight':
        skipToNext();
        break;
      case ' ':
        event.preventDefault();
        togglePlayPause();
        break;
      case 'f':
      case 'F':
        togglePlayerFold();
        break;
      case 'Escape':
        if (state.isPlayerFolded) togglePlayerFold(false);
        break;
      default:
        return;
    }
    showControls();
  });

  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      state.wasPlayingBeforeHide = !video.paused;
      video.pause();
    } else if (state.wasPlayingBeforeHide) {
      video.play().catch(() => {});
    }
  });

  wrapper.addEventListener('mousemove', showControls);
  wrapper.addEventListener('mouseleave', () => {
    clearTimeout(state.controlsTimer);
    if (state.isPlaying && !state.isPlayerFolded) overlay.classList.remove('controls-visible');
  });

  let touchStartX = 0;
  let touchStartY = 0;
  let touchCount = 0;
  let touchStartTime = 0;
  let dragSide = null;
  let dragStartY = 0;
  let dragStartValue = 0;
  let dragging = false;

  wrapper.addEventListener('touchstart', event => {
    touchCount = event.touches.length;
    touchStartX = event.touches[0].clientX;
    touchStartY = event.touches[0].clientY;
    touchStartTime = Date.now();
    dragging = false;

    if (touchCount >= 2) return;
    if (document.body.classList.contains('in-fullscreen')) {
      const rect = wrapper.getBoundingClientRect();
      const x = event.touches[0].clientX - rect.left;
      dragSide = x < rect.width / 2 ? 'left' : 'right';
      dragStartY = event.touches[0].clientY;
      dragStartValue = dragSide === 'right' ? state.volume : state.brightness;
    }
  }, { passive: true });

  wrapper.addEventListener('touchmove', event => {
    if (event.touches.length >= 2) return;
    if (document.body.classList.contains('in-fullscreen') && dragSide) {
      dragging = true;
      const rect = wrapper.getBoundingClientRect();
      const deltaY = dragStartY - event.touches[0].clientY;
      const ratio = deltaY / rect.height;

      if (dragSide === 'right') {
        state.volume = Math.max(0, Math.min(1, dragStartValue + ratio));
        video.volume = state.volume;
        showIndicator('vol', state.volume);
      } else {
        state.brightness = Math.max(0, Math.min(1, dragStartValue + ratio));
        applyBrightness();
        showIndicator('bright', state.brightness);
      }
      event.preventDefault();
    }
  }, { passive: false });

  wrapper.addEventListener('touchend', event => {
    const dx = event.changedTouches[0].clientX - touchStartX;
    const dy = event.changedTouches[0].clientY - touchStartY;
    const dt = Date.now() - touchStartTime;
    const dist = Math.sqrt(dx * dx + dy * dy);

    if (touchCount >= 2) {
      if (Math.abs(dx) > 30) toggleFitToScreen();
      touchCount = 0;
      dragging = false;
      dragSide = null;
      return;
    }

    if (dragging) {
      dragging = false;
      dragSide = null;
      hideIndicators();
      return;
    }

    if (dy < -60 && Math.abs(dx) < 60 && !state.isPlayerFolded) {
      togglePlayerFold(true);
      return;
    }

    if (dy > 60 && Math.abs(dx) < 60 && state.isPlayerFolded) {
      togglePlayerFold(false);
      return;
    }

    if (dist < 15 && dt < 300) showControls();
    dragSide = null;
  }, { passive: true });
}

async function init() {
  state.userId = getUserId();
  state.hiddenChannels = loadHiddenChannels();
  state.recentChannels = loadRecentChannels();
  video.volume = state.volume;
  applyBrightness();
  updateFoldUI();
  bindEvents();

  try {
    await refreshCatalog({ keepCurrent: false, silent: false });
    if (!state.channels.length) {
      resetPlayerUI();
      showToast('No active playlists found. Add valid M3U files to the Playlist folder.');
    }
  } catch (err) {
    console.error(err);
    setLoader(false);
    resetPlayerUI();
    showToast('Unable to load the playlist library. Start the Node backend first.');
  }
}

document.addEventListener('DOMContentLoaded', init);
