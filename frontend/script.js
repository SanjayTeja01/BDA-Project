// ═══════════════════════════════════════════════════════════════════════════
// SparkInsight Analytics Platform — Shared Frontend Script
// ═══════════════════════════════════════════════════════════════════════════

// Firebase Config is dynamically loaded into the global scope
// from the <script src="firebaseConfig.js"> tag in the HTML files before this script runs.
/* global FIREBASE_CONFIG */

// ── Backend API Base URL ─────────────────────────────────────────────────────
const API_BASE = "http://localhost:8000";

// ── Firebase App State ───────────────────────────────────────────────────────
let _firebaseApp  = null;
let _firebaseAuth = null;
let _authInitPromise = null;

// ── Auth readiness tracking ───────────────────────────────────────────────────
// Wait for the first auth state event once, then rely on currentUser.
let _authReadyUser = undefined; // undefined = not yet observed

function initFirebase() {
  if (_firebaseApp) return;
  try {
    _firebaseApp = firebase.initializeApp(FIREBASE_CONFIG);
  } catch (e) {
    if (e.code !== 'app/duplicate-app') {
      console.error("Firebase init error:", e);
    }
    _firebaseApp = firebase.app();
  }
  _firebaseAuth = firebase.auth();

  // Persist auth across page loads (default is LOCAL but explicit is safer)
  _firebaseAuth.setPersistence(firebase.auth.Auth.Persistence.LOCAL).catch(() => {});

  // Track latest observed user state
  _firebaseAuth.onAuthStateChanged((user) => {
    _authReadyUser = user;
  });

  // Resolve exactly once on first emitted auth state.
  _authInitPromise = new Promise((resolve) => {
    const unsub = _firebaseAuth.onAuthStateChanged((user) => {
      _authReadyUser = user;
      unsub();
      resolve(user);
    });
  });
}

function getAuth() {
  if (!_firebaseAuth) initFirebase();
  return _firebaseAuth;
}

function isLoginPage() {
  return window.location.pathname.toLowerCase().endsWith("/login.html")
    || window.location.pathname.toLowerCase().endsWith("login.html");
}

async function waitForInitialAuthState() {
  initFirebase();
  await _authInitPromise;
  return getAuth().currentUser || _authReadyUser || null;
}

// ── Auth Guard for protected pages ──────────────────────────────────────────
async function requireAuth() {
  const user = getAuth().currentUser || await waitForInitialAuthState();
  if (!user) {
    if (!isLoginPage()) window.location.replace("login.html");
    throw new Error("Not authenticated");
  }
  return user;
}

// ── Redirect away from login if already signed in ──────────────────────────
// Use this ONLY on login.html
async function redirectIfLoggedIn(destination = "index.html", options = {}) {
  // Safety default: do not auto-redirect from login page unless explicitly forced.
  // This prevents login <-> index flip loops caused by cached HTML calling
  // redirectIfLoggedIn() on page load.
  if (!options.force) return;

  const user = getAuth().currentUser || await waitForInitialAuthState();
  if (user) window.location.replace(destination);
}

async function getIdToken() {
  const user = getAuth().currentUser || await waitForInitialAuthState();
  if (!user) throw new Error("Not authenticated");
  return user.getIdToken(false);
}

async function signOut() {
  await getAuth().signOut();
  clearCurrentJob();
  window.location.replace("login.html");
}

// ── Google Sign-In ────────────────────────────────────────────────────────────
async function signInWithGoogle() {
  try {
    const provider = new firebase.auth.GoogleAuthProvider();
    provider.addScope('email');
    provider.addScope('profile');
    await getAuth().signInWithPopup(provider);
    window.location.replace("index.html");
  } catch (err) {
    console.error("Google sign-in error:", err);
    // Surface error on login page if the function is available
    if (typeof showError === 'function') {
      showError(err.message || 'Google sign-in failed. Please try again.');
    } else {
      alert(err.message || 'Google sign-in failed.');
    }
  }
}

// ── API Client ───────────────────────────────────────────────────────────────
async function api(method, path, body = null) {
  let token;
  try {
    token = await getIdToken();
  } catch {
    window.location.replace("login.html");
    throw new Error("Session expired. Please log in again.");
  }

  const opts = {
    method,
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json",
    },
  };
  if (body) opts.body = JSON.stringify(body);

  const res = await fetch(`${API_BASE}${path}`, opts);
  if (res.status === 401) {
    try { await getAuth().signOut(); } catch (_) {}
    if (!isLoginPage()) window.location.replace("login.html");
    throw new Error("Session expired.");
  }
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

async function apiDownload(method, path, body = null) {
  const token = await getIdToken();
  const opts = {
    method,
    headers: { "Authorization": `Bearer ${token}` },
  };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${API_BASE}${path}`, opts);
  if (!res.ok) throw new Error(`Download failed: ${res.statusText}`);
  return res.blob();
}

// ── User Profile Popup ───────────────────────────────────────────────────────
function toggleUserPopup(e) {
  e.stopPropagation();
  let popup = document.getElementById('user-profile-popup');
  if (!popup) {
    popup = _createUserPopup();
    document.body.appendChild(popup);
    // Close on outside click
    document.addEventListener('click', function closePopup(ev) {
      if (!popup.contains(ev.target)) {
        popup.classList.remove('active');
      }
    });
  }
  // Position above the sidebar footer
  const footer = e.currentTarget;
  const rect   = footer.getBoundingClientRect();
  popup.style.left   = rect.left + 'px';
  popup.style.bottom = (window.innerHeight - rect.top + 8) + 'px';
  popup.style.width  = rect.width + 'px';
  popup.classList.toggle('active');
  // Refresh user info every time it opens
  _refreshPopupUser(popup);
}

function _createUserPopup() {
  const el = document.createElement('div');
  el.id = 'user-profile-popup';
  el.innerHTML = `
    <div class="user-popup-inner">
      <div class="user-popup-header">
        <div class="user-popup-avatar" id="popup-avatar"></div>
        <div>
          <div class="user-popup-name"  id="popup-name"></div>
          <div class="user-popup-email" id="popup-email"></div>
        </div>
      </div>
      <div class="user-popup-divider"></div>
      <div class="user-popup-meta">
        <div class="user-popup-meta-item">
          <span class="user-popup-meta-label">Role</span>
          <span class="user-popup-meta-val" id="popup-role">Analyst</span>
        </div>
        <div class="user-popup-meta-item">
          <span class="user-popup-meta-label">Provider</span>
          <span class="user-popup-meta-val" id="popup-provider">—</span>
        </div>
        <div class="user-popup-meta-item">
          <span class="user-popup-meta-label">User ID</span>
          <span class="user-popup-meta-val mono" id="popup-uid" style="font-size:10px;"></span>
        </div>
      </div>
      <div class="user-popup-divider"></div>
      <button class="user-popup-signout" onclick="signOut()">
        <span>⎋</span> Sign Out
      </button>
    </div>
  `;
  return el;
}

function _refreshPopupUser(popup) {
  const user = getAuth()?.currentUser;
  if (!user) return;

  const email    = user.email || '—';
  const name     = user.displayName || email.split('@')[0];
  const initials = name.charAt(0).toUpperCase();
  const provider = user.providerData?.[0]?.providerId === 'google.com' ? '🔵 Google' : '📧 Email';

  popup.querySelector('#popup-avatar').textContent  = initials;
  popup.querySelector('#popup-name').textContent    = name;
  popup.querySelector('#popup-email').textContent   = email;
  popup.querySelector('#popup-role').textContent    = 'Analyst';
  popup.querySelector('#popup-provider').textContent = provider;
  popup.querySelector('#popup-uid').textContent     = user.uid?.slice(0, 16) + '…';

  // Also update the sidebar avatar initials & name if photo URL exists (Google)
  const avatarEl = document.getElementById('user-avatar-el');
  if (avatarEl && user.photoURL) {
    avatarEl.innerHTML = `<img src="${user.photoURL}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;" />`;
  }
}

// ── Job Local Storage ─────────────────────────────────────────────────────────
function saveCurrentJob(jobId, datasetName) {
  localStorage.setItem("si_current_job", JSON.stringify({ jobId, datasetName, ts: Date.now() }));
}

function getCurrentJob() {
  try {
    const raw = localStorage.getItem("si_current_job");
    return raw ? JSON.parse(raw) : null;
  } catch { return null; }
}

function clearCurrentJob() {
  localStorage.removeItem("si_current_job");
}

// ── Job Polling ───────────────────────────────────────────────────────────────
async function pollJobStatus(jobId, onUpdate, onComplete, onFailed, intervalMs = 2500) {
  const timer = setInterval(async () => {
    try {
      const data = await api("GET", `/job/${jobId}/status`);
      onUpdate(data);
      if (data.status === "completed") {
        clearInterval(timer);
        onComplete(data);
      } else if (data.status === "failed") {
        clearInterval(timer);
        onFailed(data);
      }
    } catch (e) {
      console.warn("Polling error:", e);
    }
  }, intervalMs);
  return timer;
}

// ── Toast Notifications ───────────────────────────────────────────────────────
function toast(title, message = "", type = "info", duration = 4000) {
  const container = document.getElementById("toast-container") || createToastContainer();
  const colors = {
    success: "var(--accent-emerald)",
    error:   "var(--accent-rose)",
    warning: "var(--accent-amber)",
    info:    "var(--accent-primary)",
  };
  const icons = { success: "✅", error: "❌", warning: "⚠️", info: "ℹ️" };

  const el = document.createElement("div");
  el.className = "toast-item";
  el.innerHTML = `
    <div style="display:flex;gap:10px;align-items:flex-start;">
      <span style="font-size:16px;flex-shrink:0;">${icons[type] || "ℹ️"}</span>
      <div style="flex:1;min-width:0;">
        <div style="font-weight:600;font-size:13px;color:var(--text-primary);margin-bottom:2px;">${title}</div>
        ${message ? `<div style="font-size:12px;color:var(--text-muted);line-height:1.4;">${message}</div>` : ""}
      </div>
      <button onclick="this.closest('.toast-item').remove()" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:14px;padding:0;flex-shrink:0;">✕</button>
    </div>
    <div class="toast-bar" style="background:${colors[type]};"></div>
  `;
  container.appendChild(el);
  setTimeout(() => el.classList.add("toast-show"), 10);
  if (duration > 0) {
    setTimeout(() => {
      el.classList.remove("toast-show");
      setTimeout(() => el.remove(), 300);
    }, duration);
  }
}

function createToastContainer() {
  const el = document.createElement("div");
  el.id = "toast-container";
  el.style.cssText = "position:fixed;top:20px;right:20px;z-index:9999;display:flex;flex-direction:column;gap:8px;max-width:360px;";
  document.body.appendChild(el);
  return el;
}

// ── Sidebar Builder ───────────────────────────────────────────────────────────
function buildSidebar(active = "datasets") {
  const user     = getAuth()?.currentUser;
  const email    = user?.email || "User";
  const initials = email.charAt(0).toUpperCase();

  const navItems = [
    { key: "datasets",  href: "index.html",      icon: "🗃️",  label: "Datasets"   },
    { key: "status",    href: "job_status.html",  icon: "⚙️",  label: "Job Status" },
    { key: "dashboard", href: "dashboard.html",   icon: "📊",  label: "Dashboard"  },
    { key: "history",   href: "job_history.html", icon: "📋",  label: "History"    },
  ];

  return `
    <div class="sidebar-brand">
      <div class="brand-icon">⚡</div>
      <div>
        <div class="brand-name">SparkInsight</div>
        <div class="brand-sub">Analytics Platform</div>
      </div>
    </div>
    <nav class="sidebar-nav">
      ${navItems.map(item => `
        <a href="${item.href}" class="nav-item ${item.key === active ? "active" : ""}">
          <span class="nav-icon">${item.icon}</span>
          <span>${item.label}</span>
        </a>
      `).join("")}
    </nav>
    <div class="sidebar-footer" onclick="toggleUserPopup(event)" style="cursor:pointer;position:relative;" title="Account options">
      <div class="user-avatar" id="user-avatar-el">${initials}</div>
      <div class="user-info">
        <div class="user-email" title="${email}">${email}</div>
        <div class="user-role">Analyst</div>
      </div>
      <span style="color:var(--text-muted);font-size:12px;">⌄</span>
    </div>

    <!-- User popup (injected into body on first call) -->
  `;
}

// ── Chart Defaults ────────────────────────────────────────────────────────────
function applyChartDefaults() {
  if (typeof Chart === "undefined") return;
  Chart.defaults.color        = "#94a3b8";
  Chart.defaults.borderColor  = "rgba(148,163,184,0.08)";
  Chart.defaults.font.family  = "'DM Sans', sans-serif";
}

// ── Formatters ────────────────────────────────────────────────────────────────
function fmtInt(n) {
  if (n == null) return "—";
  return Number(n).toLocaleString();
}
function fmtFloat(n, decimals = 2) {
  if (n == null) return "—";
  return Number(n).toFixed(decimals);
}
function fmtSeconds(sec) {
  if (sec == null) return "—";
  if (sec < 60) return `${Number(sec).toFixed(1)}s`;
  const m = Math.floor(sec / 60), s = Math.round(sec % 60);
  return `${m}m ${s}s`;
}
function fmtBytes(bytes) {
  if (!bytes) return "—";
  const gb = bytes / (1024 ** 3);
  if (gb >= 1) return `${gb.toFixed(2)} GB`;
  const mb = bytes / (1024 ** 2);
  if (mb >= 1) return `${mb.toFixed(1)} MB`;
  return `${(bytes / 1024).toFixed(0)} KB`;
}
function fmtNum(n) {
  if (n == null || n === "" || isNaN(n)) return "—";
  const abs = Math.abs(Number(n));
  if (abs >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (abs >= 1e6) return (n / 1e6).toFixed(2) + "M";
  if (abs >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// ── Status Badge ──────────────────────────────────────────────────────────────
function statusBadge(status) {
  const map = {
    queued:          ["badge-warning", "⏳ Queued"],
    profiling:       ["badge-info",    "🔍 Profiling"],
    cleaning:        ["badge-info",    "🧹 Cleaning"],
    quality_scoring: ["badge-info",    "🏆 Quality Check"],
    analytics:       ["badge-primary", "⚡ Analytics"],
    formatting:      ["badge-info",    "📦 Formatting"],
    completed:       ["badge-success", "✅ Completed"],
    failed:          ["badge-error",   "❌ Failed"],
  };
  const [cls, label] = map[status] || ["badge-warning", status];
  return `<span class="badge ${cls}">${label}</span>`;
}

// ── Quality Score Color ───────────────────────────────────────────────────────
function qualityColor(score) {
  if (score >= 80) return "var(--accent-emerald)";
  if (score >= 60) return "var(--accent-amber)";
  return "var(--accent-rose)";
}

// ── Dropdown Toggle ───────────────────────────────────────────────────────────
function toggleDropdown() {
  const menu = document.getElementById("dl-menu");
  if (menu) menu.classList.toggle("hidden");
}
document.addEventListener("click", (e) => {
  const menu = document.getElementById("dl-menu");
  if (menu && !e.target.closest(".dropdown")) menu.classList.add("hidden");
});

// ── Chart Color Palette ───────────────────────────────────────────────────────
const CHART_COLORS = [
  "#6366f1","#22d3ee","#10b981","#f59e0b","#f43f5e","#8b5cf6",
  "#3b82f6","#ec4899","#14b8a6","#f97316","#a855f7","#84cc16",
];

// ── Chart Gradient Helper ─────────────────────────────────────────────────────
function chartGradient(ctx, color1, color2) {
  const gradient = ctx.createLinearGradient(0, 0, 0, 300);
  gradient.addColorStop(0, color1 + "55");
  gradient.addColorStop(1, color2 + "05");
  return gradient;
}

// ── Correlation Cell Coloring ─────────────────────────────────────────────────
function corrBg(v) {
  if (v == null) return "transparent";
  const abs = Math.abs(v);
  if (abs >= 0.8) return v > 0 ? "rgba(16,185,129,0.25)" : "rgba(244,63,94,0.25)";
  if (abs >= 0.5) return v > 0 ? "rgba(16,185,129,0.12)" : "rgba(244,63,94,0.12)";
  if (abs >= 0.2) return "rgba(148,163,184,0.07)";
  return "transparent";
}
function corrColor(v) {
  if (v == null) return "var(--text-muted)";
  const abs = Math.abs(v);
  if (abs >= 0.8) return v > 0 ? "var(--accent-emerald)" : "var(--accent-rose)";
  if (abs >= 0.5) return v > 0 ? "#6ee7b7" : "#fda4af";
  return "var(--text-secondary)";
}