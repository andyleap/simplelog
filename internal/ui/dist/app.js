// simplelog web UI — dependency-free ES module driving the manager's HTTP API.

const $ = (id) => document.getElementById(id);
const MAX_ROWS = 5000;

const DEFAULT_COLUMNS = ["_ts", "_namespace", "_pod", "_container", "level", "_message"];
const SAVED_KEY = "simplelog.saved";
const COLS_KEY = "simplelog.columns";

let columns = loadColumns();
let knownColumns = new Set([...DEFAULT_COLUMNS]);
let abort = null; // AbortController for an in-flight stream
let rowCount = 0;

// ---- time range ----------------------------------------------------------

function rangeParams() {
  const sel = $("range").value;
  if (sel === "custom") {
    const s = $("start").value ? Math.floor(new Date($("start").value).getTime() / 1000) : 0;
    const e = $("end").value ? Math.floor(new Date($("end").value).getTime() / 1000) : 0;
    return { start: s || "", end: e || "" };
  }
  if (sel === "0") return { start: "", end: "" }; // all time
  const now = Math.floor(Date.now() / 1000);
  return { start: now - parseInt(sel, 10), end: "" };
}

$("range").addEventListener("change", () => {
  const custom = $("range").value === "custom";
  $("start").classList.toggle("hidden", !custom);
  $("end").classList.toggle("hidden", !custom);
});

// ---- query URL building ---------------------------------------------------

function queryURL(follow) {
  const p = new URLSearchParams();
  p.set("expr", $("expr").value.trim());
  const { start, end } = rangeParams();
  if (start) p.set("start", start);
  if (end) p.set("end", end);
  if (follow) {
    p.set("follow", "true");
  } else {
    p.set("direction", $("direction").value);
    const lim = $("limit").value.trim();
    if (lim && lim !== "0") p.set("limit", lim);
  }
  return "/v1/query?" + p.toString();
}

// ---- NDJSON streaming -----------------------------------------------------

async function streamNDJSON(url, onRecord, signal) {
  const resp = await fetch(url, { signal });
  if (!resp.ok) throw new Error((await resp.text()) || resp.statusText);
  const reader = resp.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.trim()) onRecord(JSON.parse(line));
    }
  }
}

// ---- run / live -----------------------------------------------------------

async function run({ follow }) {
  stopStream();
  abort = new AbortController();
  resetTable();
  setRunning(true, follow);
  setStatus(follow ? "Live tailing…" : "Running query…");

  if (!follow) drawHistogram().catch(() => {});

  try {
    await streamNDJSON(queryURL(follow), appendRecord, abort.signal);
    if (!follow) setStatus(`${rowCount} record(s).`);
  } catch (e) {
    if (e.name !== "AbortError") setStatus("Error: " + e.message, true);
  } finally {
    if (!follow || (abort && abort.signal.aborted)) setRunning(false, follow);
  }
}

function stopStream() {
  if (abort) {
    abort.abort();
    abort = null;
  }
}

function setRunning(on, follow) {
  $("stop").disabled = !on;
  $("follow").classList.toggle("active", on && follow);
}

$("queryForm").addEventListener("submit", (e) => {
  e.preventDefault();
  run({ follow: false });
});
$("follow").addEventListener("click", () => run({ follow: true }));
$("stop").addEventListener("click", () => {
  stopStream();
  setRunning(false, true);
  setStatus(`Stopped. ${rowCount} record(s).`);
});

// ---- table ----------------------------------------------------------------

function resetTable() {
  $("rows").innerHTML = "";
  rowCount = 0;
  renderHead();
}

function renderHead() {
  $("head").innerHTML = columns.map((c) => `<th>${escapeHTML(c)}</th>`).join("");
}

function appendRecord(rec) {
  // Track any new body fields for the column picker.
  if (rec.body) {
    for (const k of Object.keys(rec.body)) {
      if (!knownColumns.has(k)) {
        knownColumns.add(k);
        renderColumnPicker();
      }
    }
  }
  const tr = document.createElement("tr");
  for (const col of columns) {
    const td = document.createElement("td");
    const val = cellValue(rec, col);
    if (col === "_ts") {
      td.className = "ts";
      td.textContent = fmtTime(val);
    } else {
      td.textContent = val;
      if (col === "_message" && rec._stream === "stderr") td.className = "stderr";
      if (col === "level") td.className = "lvl-" + String(val).toLowerCase();
    }
    tr.appendChild(td);
  }
  const body = $("rows");
  body.appendChild(tr);
  rowCount++;
  while (body.children.length > MAX_ROWS) body.removeChild(body.firstChild);

  // Auto-scroll when near the bottom (live tail).
  const wrap = body.closest(".tableWrap");
  if (wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight < 80) {
    wrap.scrollTop = wrap.scrollHeight;
  }
}

function cellValue(rec, col) {
  if (col.startsWith("_")) return rec[col] ?? "";
  if (col.startsWith("label.")) return (rec._labels || {})[col.slice(6)] ?? "";
  let v = rec.body;
  for (const part of col.split(".")) {
    if (v == null) return "";
    v = v[part];
  }
  if (v == null) return "";
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}

function fmtTime(s) {
  if (!s) return "";
  const d = new Date(s);
  if (isNaN(d)) return s;
  return d.toISOString().replace("T", " ").replace("Z", "");
}

// ---- columns --------------------------------------------------------------

function loadColumns() {
  try {
    const c = JSON.parse(localStorage.getItem(COLS_KEY));
    if (Array.isArray(c) && c.length) return c;
  } catch {}
  return [...DEFAULT_COLUMNS];
}

function saveColumns() {
  localStorage.setItem(COLS_KEY, JSON.stringify(columns));
}

function renderColumnPicker() {
  const all = [...new Set([...knownColumns, ...columns])].sort(cmpColumns);
  $("columns").innerHTML = "";
  for (const c of all) {
    const id = "col-" + c;
    const label = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = columns.includes(c);
    cb.addEventListener("change", () => toggleColumn(c, cb.checked));
    label.appendChild(cb);
    label.appendChild(document.createTextNode(" " + c));
    $("columns").appendChild(label);
  }
}

function cmpColumns(a, b) {
  const order = (x) => (x === "_ts" ? 0 : x.startsWith("_") ? 1 : x.startsWith("label.") ? 2 : 3);
  return order(a) - order(b) || a.localeCompare(b);
}

function toggleColumn(col, on) {
  if (on && !columns.includes(col)) {
    columns.push(col);
    columns.sort(cmpColumns);
  } else if (!on) {
    columns = columns.filter((c) => c !== col);
  }
  saveColumns();
  renderHead();
}

// ---- fields helper --------------------------------------------------------

async function loadFields() {
  let data;
  try {
    data = await (await fetch("/v1/fields")).json();
  } catch {
    return;
  }
  (data.envelope || []).forEach((f) => knownColumns.add(f));
  (data.fields || []).forEach((f) => knownColumns.add(f));
  (data.labels || []).forEach((l) => knownColumns.add("label." + l));
  renderColumnPicker();

  const groups = [
    ["envelope", data.envelope],
    ["namespaces", data.namespaces],
    ["pods", data.pods],
    ["nodes", data.nodes],
    ["containers", data.containers],
    ["labels", (data.labels || []).map((l) => "label." + l)],
    ["fields", data.fields],
  ];
  const box = $("fields");
  box.innerHTML = "";
  for (const [name, vals] of groups) {
    if (!vals || !vals.length) continue;
    const g = document.createElement("div");
    g.className = "group";
    g.innerHTML = `<div class="glabel">${name}</div>`;
    for (const v of vals) {
      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = v;
      chip.addEventListener("click", () => insertField(name, v));
      g.appendChild(chip);
    }
    box.appendChild(g);
  }
}

// insertField inserts a field reference (or a field=value snippet) into the query.
function insertField(group, value) {
  const ta = $("expr");
  let snippet;
  if (group === "namespaces") snippet = `_namespace="${value}"`;
  else if (group === "pods") snippet = `_pod="${value}"`;
  else if (group === "nodes") snippet = `_node="${value}"`;
  else if (group === "containers") snippet = `_container="${value}"`;
  else if (group === "fields") snippet = `${value}=`;
  else snippet = value + (group === "labels" ? "=" : "");
  const sep = ta.value && !ta.value.endsWith(" ") ? " " : "";
  ta.value += sep + snippet;
  ta.focus();
}

// ---- saved queries --------------------------------------------------------

function loadSaved() {
  try {
    return JSON.parse(localStorage.getItem(SAVED_KEY)) || {};
  } catch {
    return {};
  }
}

function renderSaved() {
  const saved = loadSaved();
  const ul = $("saved");
  ul.innerHTML = "";
  for (const name of Object.keys(saved).sort()) {
    const li = document.createElement("li");
    const span = document.createElement("span");
    span.className = "name";
    span.textContent = name;
    span.title = saved[name];
    span.addEventListener("click", () => {
      $("expr").value = saved[name];
      $("expr").focus();
    });
    const del = document.createElement("button");
    del.className = "del";
    del.textContent = "✕";
    del.addEventListener("click", () => {
      const s = loadSaved();
      delete s[name];
      localStorage.setItem(SAVED_KEY, JSON.stringify(s));
      renderSaved();
    });
    li.appendChild(span);
    li.appendChild(del);
    ul.appendChild(li);
  }
}

$("saveBtn").addEventListener("click", () => {
  const name = $("saveName").value.trim();
  if (!name) return;
  const s = loadSaved();
  s[name] = $("expr").value.trim();
  localStorage.setItem(SAVED_KEY, JSON.stringify(s));
  $("saveName").value = "";
  renderSaved();
});

// ---- histogram ------------------------------------------------------------

async function drawHistogram() {
  const p = new URLSearchParams();
  p.set("expr", $("expr").value.trim());
  const { start, end } = rangeParams();
  if (start) p.set("start", start);
  if (end) p.set("end", end);
  const data = await (await fetch("/v1/histogram?" + p.toString())).json();
  const counts = data.counts || [];
  const canvas = $("histogram");
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = 80;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext("2d");
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  const max = Math.max(1, ...counts);
  const bw = w / counts.length;
  ctx.fillStyle = "#4aa3ff";
  counts.forEach((c, i) => {
    const bh = (c / max) * (h - 4);
    ctx.fillRect(i * bw, h - bh, Math.max(1, bw - 1), bh);
  });
}

// ---- misc -----------------------------------------------------------------

function setStatus(msg, isErr) {
  const el = $("status");
  el.textContent = msg;
  el.classList.toggle("error", !!isErr);
}

function escapeHTML(s) {
  return s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// Ctrl/Cmd+Enter runs the query.
$("expr").addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
    e.preventDefault();
    run({ follow: false });
  }
});

// ---- init -----------------------------------------------------------------

renderColumnPicker();
renderSaved();
renderHead();
loadFields();
