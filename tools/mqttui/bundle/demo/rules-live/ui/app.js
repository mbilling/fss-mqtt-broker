// The live rules demo's page (demo/rules-live/README.md). server.py serves it, passes
// /api/* on to the broker's admin API, and streams MQTT to it as server-sent events.
//
// Anyone on the broker can publish what this page shows, so every string from MQTT or the
// admin API reaches the DOM as text (textContent, or a text node), never as HTML.
"use strict";

const RULE_ID = /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/;
const DEVICE_ROOTS = ["plant", "home", "vehicle"];
const KEEP_DATA = 500; // messages kept for the filter
const SHOW_DATA = 200; // messages shown
const KEEP_TRACE = 20; // trace records kept per rule

const NEW_RULE = {
  id: "demo_grid_deviation",
  description: "Grid frequency more than 20 mHz off 50 Hz",
  enable: true,
  sql: [
    "SELECT",
    "  topic(2) AS site,",
    "  payload.Hz AS hz,",
    "  round((payload.Hz - 50) * 1000) AS deviation_mhz",
    'FROM "plant/+/poc/grid"',
    "WHERE is_num(payload.Hz) AND (payload.Hz < 49.98 OR payload.Hz > 50.02)",
    "",
  ].join("\n"),
  actions: [
    { function: "republish", args: { topic: "kpi/demo/${site}/frequency", qos: 0, payload: "${.}" } },
  ],
};

const state = {
  list: null, // the last GET /api/rules answer
  stats: new Map(), // rule id -> its latest $SYS record
  traces: new Map(), // rule id -> its latest trace records, newest first
  summary: null, // the latest $SYS summary
  summaryAt: 0, // when it arrived (ms)
  interval: 2, // seconds between summaries
  mqttUpSince: 0, // when the server's MQTT connection came up, or 0
  selected: null, // the rule in the editor; null for a new one
  awaiting: null, // {digest, out}: an Apply waiting to see its digest on $SYS
  fileDigest: null, // the digest the whole-file editor was loaded at
  data: [], // recent device and derived messages, oldest first
  es: null,
  banner: null, // why the banner shows: "predates", "quiet", "admin"
  refresh: 0,
};

const $ = (id) => document.getElementById(id);

function el(tag, text, className) {
  const e = document.createElement(tag);
  if (text !== undefined && text !== null) e.textContent = String(text);
  if (className) e.className = className;
  return e;
}

function short(digest) {
  return typeof digest === "string" ? digest.slice(0, 12) : "-";
}

function clock(ms) {
  return new Date(ms).toLocaleTimeString([], { hour12: false });
}

function ago(when) {
  const ms = typeof when === "string" ? Date.parse(when) : when;
  if (!ms) return "-";
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  return clock(ms);
}

function count(n) {
  return typeof n === "number" ? n.toLocaleString() : "-";
}

function mqttMatch(filter, topic) {
  if (topic.startsWith("$") && (filter.startsWith("+") || filter.startsWith("#"))) return false;
  const f = filter.split("/");
  const t = topic.split("/");
  for (let i = 0; i < f.length; i++) {
    if (f[i] === "#") return true;
    if (i >= t.length || (f[i] !== "+" && f[i] !== t[i])) return false;
  }
  return f.length === t.length;
}

// ---- the admin API, through server.py ------------------------------------------------

async function api(method, path, body) {
  const init = { method, headers: {} };
  if (method !== "GET") {
    // server.py refuses a write without these: a cross-site page cannot send them.
    init.headers["Content-Type"] = "application/json";
    init.headers["X-Rules-UI"] = "1";
    if (body !== undefined) init.body = JSON.stringify(body);
  }
  let res;
  try {
    res = await fetch(path, init);
  } catch (e) {
    return { status: 0, body: { error: { code: "network", message: String(e) } } };
  }
  let json;
  try {
    json = await res.json();
  } catch {
    json = { error: { code: "bad-answer", message: `HTTP ${res.status} without a JSON body` } };
  }
  return { status: res.status, body: json };
}

function setBanner(reason, text) {
  const b = $("banner");
  state.banner = text ? reason : null;
  b.textContent = text || "";
  b.hidden = !text;
}

async function loadRules() {
  const r = await api("GET", "/api/rules");
  if (r.status === 404) {
    setBanner("predates", "This broker predates ADR 0084: its admin API has no rules endpoints. " +
      "Rebuild it with demo/rules-live/up.sh, which always builds from this checkout.");
    return;
  }
  if (r.status !== 200) {
    setBanner("admin", `The admin API did not answer the rules list: ${errorText(r)}`);
    return;
  }
  if (state.banner === "admin" || state.banner === "predates") setBanner(null, "");
  state.list = r.body;
  renderHeader();
  renderRules();
}

function scheduleRefresh() {
  clearTimeout(state.refresh);
  state.refresh = setTimeout(loadRules, 300);
}

function errorText(r) {
  const e = r.body && r.body.error;
  return e ? `${e.code}: ${e.message}` : `HTTP ${r.status}`;
}

// A field an error answer carries beside its code, or inside the error object.
function errorField(r, name) {
  const b = r.body || {};
  return b[name] !== undefined ? b[name] : (b.error || {})[name];
}

// ---- header and rules table ------------------------------------------------------------

function renderHeader() {
  const s = state.summary;
  const l = state.list;
  $("s-node").textContent = (s && s.node) || (l && l.node) || "-";
  const running = (s && s.digest) || (l && l.digest);
  $("s-digest").textContent = short(running);
  if (l) {
    const same = l.file_digest === running;
    $("s-file").textContent = `${short(l.file_digest)} ${same ? "(running)" : "(differs from the running rules)"}`;
  }
  if (s) {
    $("s-count").textContent = `${s.enabled} of ${s.rules} enabled`;
    $("s-trace").textContent = s.trace
      ? `on, ${s.trace_rate}/s per rule${s.trace_dropped ? `, ${count(s.trace_dropped)} dropped` : ""}`
      : "off";
  } else if (l && Array.isArray(l.rules)) {
    $("s-count").textContent = `${l.rules.filter((r) => r.enabled).length} of ${l.rules.length} enabled`;
  }
  const reload = (s && s.reload) || (l && l.reload);
  if (reload) {
    const outcome = reload.applied ? "applied" : `rejected (${reload.error_kind || "error"})`;
    const repeats = reload.repeats ? `, ${reload.repeats + 1} times` : "";
    $("s-reload").textContent = `${reload.trigger}: ${outcome}${repeats}, ${ago(reload.at)}`;
    $("s-reload").title = reload.error || "";
  }
}

function renderRules() {
  const rules = (state.list && state.list.rules) || [];
  $("rules").tBodies[0].replaceChildren(...rules.map(ruleRow));
}

function ruleRow(rule) {
  const tr = el("tr");
  tr.dataset.id = rule.id;
  const th = el("th");
  th.scope = "row";
  const pick = el("button", rule.id, "link");
  pick.type = "button";
  pick.addEventListener("click", () => selectRule(rule.id));
  if (rule.id === state.selected) {
    tr.classList.add("selected");
    pick.setAttribute("aria-current", "true");
  }
  th.append(pick);
  tr.append(th);
  const from = [].concat(rule.from || [], rule.events || []).join(", ");
  for (const [key, text] of [["on", ""], ["from", from], ["matched"], ["passed"], ["no_result"],
    ["failed"], ["actions_failed"], ["rate"], ["active"], ["error"]]) {
    const td = el("td", text);
    td.dataset.key = key;
    if (!["on", "from", "active", "error"].includes(key)) td.className = "num";
    tr.append(td);
  }
  fillRow(tr, rule, state.stats.get(rule.id));
  return tr;
}

// Live $SYS figures win over the list's, which are as old as the last GET.
function fillRow(tr, rule, live) {
  const src = live || rule;
  const counts = src.counts || {};
  const cell = (key) => tr.querySelector(`td[data-key="${key}"]`);
  cell("on").textContent = src.enabled ? "yes" : "no";
  for (const k of ["matched", "passed", "no_result", "failed", "actions_failed"]) {
    cell(k).textContent = count(counts[k]);
  }
  tr.classList.toggle("failing", (counts.failed || 0) + (counts.actions_failed || 0) > 0);
  const rate = live && live.rates ? live.rates.matched : undefined;
  cell("rate").textContent = typeof rate === "number" ? rate.toFixed(1) : "-";
  cell("active").textContent = src.last_active_at ? ago(src.last_active_at) : "never";
  // $SYS leaves the message out while the trace is off; the list has it for an operator.
  let err = src.last_error;
  if (err && !err.message && rule.last_error && rule.last_error.at === err.at) err = rule.last_error;
  const errCell = cell("error");
  errCell.textContent = err ? `${err.kind}${err.message ? `: ${err.message}` : ""} (${ago(err.at)})` : "";
  errCell.title = err && err.message ? err.message : "";
}

function updateRow(id) {
  const rule = state.list && state.list.rules.find((r) => r.id === id);
  const tr = rule && $("rules").tBodies[0].querySelector(`tr[data-id="${CSS.escape(id)}"]`);
  if (tr) fillRow(tr, rule, state.stats.get(id));
}

// ---- the rule editor -------------------------------------------------------------------

function fillEditor(rule) {
  $("f-id").value = rule.id;
  $("f-description").value = rule.description || "";
  $("f-enable").checked = Boolean(rule.enable);
  $("f-sql").value = rule.sql || "";
  $("f-actions").value = rule.actions ? JSON.stringify(rule.actions, null, 2) : "";
}

function selectRule(id) {
  const rule = state.list && state.list.rules.find((r) => r.id === id);
  if (!rule) return;
  state.selected = id;
  $("editing").textContent = id;
  $("f-id").readOnly = true;
  $("b-delete").disabled = false;
  fillEditor({ id, description: rule.description, enable: rule.enabled, sql: rule.sql, actions: rule.actions_spec });
  $("r-out").replaceChildren();
  if (rule.redacted) say($("r-out"), "error", "The admin API answered as to a viewer: no SQL or actions.");
  renderRules();
  renderTrace();
}

function newRule() {
  state.selected = null;
  $("editing").textContent = "(new)";
  $("f-id").readOnly = false;
  $("b-delete").disabled = true;
  fillEditor(NEW_RULE);
  $("r-out").replaceChildren();
  renderRules();
  renderTrace();
  $("f-id").focus();
}

function ruleFromEditor() {
  const id = $("f-id").value.trim();
  if (!RULE_ID.test(id)) {
    throw new Error("A rule id is a letter or _ followed by up to 63 letters, digits, _ or -.");
  }
  let actions;
  try {
    actions = JSON.parse($("f-actions").value);
  } catch (e) {
    throw new Error(`The actions are not JSON: ${e.message}`);
  }
  if (!Array.isArray(actions)) throw new Error("The actions must be a JSON array, as in the rules file.");
  return {
    id,
    fields: { sql: $("f-sql").value, actions, description: $("f-description").value, enable: $("f-enable").checked },
  };
}

// The topic filters (and $events) after FROM: quoted strings, comma-separated.
function fromFilters(sql) {
  const m = /\bFROM\s+((?:"[^"]*"\s*,\s*)*"[^"]*")/i.exec(sql);
  return m ? Array.from(m[1].matchAll(/"([^"]*)"/g), (x) => x[1]) : [];
}

async function testInput(rule) {
  if ($("t-custom").checked) {
    return { body: { topic: $("t-topic").value, payload: $("t-payload").value, payload_encoding: "utf8" } };
  }
  const filters = fromFilters(rule.fields.sql);
  const topics = filters.filter((f) => !f.startsWith("$events/"));
  if (topics.length) {
    const q = new URLSearchParams();
    for (const f of topics) q.append("topic_filter", f);
    const r = await api("GET", `/api/latest?${q}`);
    if (r.status !== 200) return { error: errorText(r) };
    const m = r.body;
    // Shown in the custom fields too, so it can be edited and tested again.
    $("t-topic").value = m.topic;
    if (m.payload_encoding === "utf8") $("t-payload").value = m.payload;
    return {
      body: { topic: m.topic, payload: m.payload, payload_encoding: m.payload_encoding },
      note: `the latest ${m.topic} (${count(m.bytes)} bytes, received ${clock(m.at)})`,
    };
  }
  if (filters.length) {
    // An event rule: the last event its trace saw, else a sample of its first event.
    const seen = (state.traces.get(rule.id) || []).find((t) => t.trigger && t.trigger.type === "event");
    const t = seen ? seen.trigger : {};
    const event = t.event || filters[0].slice("$events/".length).replace("/", ".");
    const body = { topic: filters[0], payload: "", event, clientid: t.clientid || "rules-ui-test" };
    if (t.username) body.username = t.username;
    return { body, note: seen ? `the last ${event} event in its trace (${t.clientid})` : `a sample ${event} event` };
  }
  return { error: "No FROM topic in the SQL to take the latest message from: test with a message of your own." };
}

async function checkRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  say(out, "busy", "Checking…");
  showCheck(out, await api("POST", "/api/check", { rule: { id: rule.id, ...rule.fields } }), $("f-sql"));
}

async function testRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  say(out, "busy", "Testing…");
  const input = await testInput(rule);
  if (input.error) {
    say(out, "error", input.error);
    return;
  }
  const r = await api("POST", "/api/test", { rule: { id: rule.id, ...rule.fields }, ...input.body });
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, $("f-sql"));
    return;
  }
  out.append(el("p", `Tested against ${input.note || `${input.body.topic}`}, without publishing anything:`));
  for (const res of r.body.results || []) {
    const box = el("div", undefined, "result");
    const head = el("p");
    head.append(el("strong", res.rule), " ", el("span", res.result, `badge ${res.result}`));
    if (res.enabled === false) head.append(" (tested as if enabled: it is disabled)");
    box.append(head);
    if (res.reason) box.append(el("p", res.reason));
    if (res.error) box.append(el("p", res.error, "error"));
    box.append(outputList(res.outputs));
    out.append(box);
  }
}

async function applyRule() {
  const out = $("r-out");
  let rule;
  try {
    rule = ruleFromEditor();
  } catch (e) {
    say(out, "error", e.message);
    return;
  }
  const q = new URLSearchParams({ id: rule.id });
  if (state.list && state.list.file_digest) q.set("if_match", state.list.file_digest);
  say(out, "busy", "Applying…");
  const r = await api("PUT", `/api/rule?${q}`, rule.fields);
  showWrite(out, r, $("f-sql"));
  if (r.status === 200) {
    state.selected = rule.id;
    $("editing").textContent = rule.id;
    $("f-id").readOnly = true;
    $("b-delete").disabled = false;
  }
  await loadRules();
}

async function deleteRule() {
  const id = state.selected;
  if (!id || !confirm(`Delete ${id} from the rules file? Its statistics go with it.`)) return;
  const out = $("r-out");
  const q = new URLSearchParams({ id });
  if (state.list && state.list.file_digest) q.set("if_match", state.list.file_digest);
  say(out, "busy", "Deleting…");
  const r = await api("DELETE", `/api/rule?${q}`);
  showWrite(out, r, null);
  if (r.status === 200) {
    state.selected = null;
    $("editing").textContent = `(${id} deleted)`;
    $("f-id").readOnly = false;
    $("b-delete").disabled = true;
  }
  await loadRules();
}

// ---- the whole file ----------------------------------------------------------------------

async function loadSource() {
  const out = $("f-out");
  const r = await api("GET", "/api/source");
  if (r.status !== 200) {
    showError(out, r, null);
    return;
  }
  $("f-source").value = r.body.source;
  state.fileDigest = r.body.digest;
  say(out, "ok", `Loaded ${count(r.body.bytes)} bytes, digest ${short(r.body.digest)}: ` +
    (r.body.in_sync ? "this is what runs." : `not what runs (${short(r.body.running_digest)}).`));
}

async function checkFile() {
  const out = $("f-out");
  say(out, "busy", "Checking…");
  showCheck(out, await api("POST", "/api/check", { source: $("f-source").value }), $("f-source"));
}

async function applyFile() {
  const out = $("f-out");
  if (!state.fileDigest) {
    say(out, "error", "Load the file first: Apply replaces it only if it has not changed since.");
    return;
  }
  const q = new URLSearchParams({ if_match: state.fileDigest });
  say(out, "busy", "Applying…");
  const r = await api("PUT", `/api/rules?${q}`, { source: $("f-source").value });
  showWrite(out, r, $("f-source"));
  if (r.status === 200) state.fileDigest = r.body.digest;
  await loadRules();
}

async function resetFile() {
  if (!confirm("Replace the rules file with the shipped demo/rules/rules.toml? Every edit is lost.")) return;
  const out = $("f-out");
  say(out, "busy", "Resetting…");
  const r = await api("PUT", "/api/reset");
  showWrite(out, r, null);
  if (r.status === 200) {
    $("f-source").value = "";
    state.fileDigest = null;
  }
  await loadRules();
}

// ---- answers -------------------------------------------------------------------------------

function say(out, kind, text) {
  out.replaceChildren(el("p", text, kind));
}

function showCheck(out, r, textarea) {
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, textarea);
    return;
  }
  out.append(el("p", `Valid: the file would hold ${r.body.rules} rules, ${r.body.enabled} enabled ` +
    `(digest ${short(r.body.digest)}). Nothing was written.`, "ok"));
  warnings(out, r.body.warnings);
}

function showWrite(out, r, textarea) {
  out.replaceChildren();
  if (r.status !== 200) {
    showError(out, r, textarea);
    if (r.status === 412) {
      out.append(el("p", "The rules list is reloaded now. Your edit is still here: Apply again to " +
        "write it over the newer file, or load the newer version first."));
    }
    return;
  }
  const b = r.body;
  out.append(el("p", b.applied
    ? `Written and running: ${b.rules} rules, ${b.enabled} enabled. ` +
      `On disk ${short(b.digest)}, running ${short(b.running_digest)}.`
    : `Written (${short(b.digest)}), but the running rules are ${short(b.running_digest)}: ` +
      "the reload did not apply it.", b.applied ? "ok" : "error"));
  warnings(out, b.warnings);
  const wait = el("p", `Waiting for $SYS/brokers/+/rules to report ${short(b.running_digest)}…`, "hint");
  out.append(wait);
  state.awaiting = { digest: b.running_digest, out: wait };
  if (state.summary && state.summary.digest === b.running_digest) sawDigest(state.summary);
}

function sawDigest(summary) {
  const { out } = state.awaiting;
  out.textContent = `$SYS/brokers/${summary.node}/rules reports ${short(summary.digest)} at ` +
    `${clock(Date.parse(summary.at) || Date.now())}: every rule's statistics now count from it.`;
  out.className = "ok";
  state.awaiting = null;
}

function showError(out, r, textarea) {
  out.append(el("p", errorText(r), "error"));
  const d = errorField(r, "details");
  if (d && (d.line || d.rule)) {
    const where = [d.scope === "sql" ? "in the SQL" : "in the file", d.rule && `of ${d.rule}`,
      d.line && `at line ${d.line}`, d.column && `column ${d.column}`].filter(Boolean).join(" ");
    out.append(el("p", `Where: ${where}.`));
    // SQL positions count from the statement, TOML positions from the file.
    const sqlHere = textarea === $("f-sql") && d.scope === "sql";
    const tomlHere = textarea === $("f-source") && d.scope === "toml";
    if (d.line && (sqlHere || tomlHere)) selectAt(textarea, d.line, d.column);
  }
  const running = errorField(r, "running_digest");
  if (errorField(r, "written")) {
    out.append(el("p", `The file was written (${short(errorField(r, "digest"))}), but the reload ` +
      `rejected it: the broker still runs ${short(running)}.`));
  } else if (errorField(r, "file_digest")) {
    out.append(el("p", `On disk now: ${short(errorField(r, "file_digest"))}; running: ${short(running)}.`));
  }
}

function warnings(out, list) {
  if (!Array.isArray(list) || !list.length) return;
  const ul = el("ul", undefined, "warnings");
  for (const w of list) ul.append(el("li", w));
  out.append(el("p", "Warnings:"), ul);
}

function selectAt(textarea, line, column) {
  const lines = textarea.value.split("\n");
  let at = 0;
  for (let i = 0; i < line - 1 && i < lines.length; i++) at += lines[i].length + 1;
  at = Math.min(at + Math.max(0, (column || 1) - 1), textarea.value.length);
  textarea.focus();
  textarea.setSelectionRange(at, Math.min(at + 1, textarea.value.length));
}

function payloadText(p, encoding) {
  return encoding === "base64" ? `(base64) ${p}` : p;
}

function outputList(outputs) {
  const ul = el("ul", undefined, "outputs");
  for (const o of outputs || []) {
    const li = el("li");
    if (o.error !== undefined) {
      li.append(el("span", `action ${o.action_index + 1} failed: ${o.error}`, "error"));
    } else if (o.action === "republish") {
      li.append(el("span", "→ ", "arrow"), el("span", o.topic, "topic"),
        el("span", ` qos ${o.qos}${o.retain ? ", retained" : ""}`, "hint"));
      li.append(el("pre", payloadText(o.payload, o.payload_encoding), "payload"));
      if (o.truncated) li.append(el("span", `first part of ${count(o.payload_bytes)} bytes`, "hint"));
    } else {
      li.append(el("span", `${o.action || "output"}: `, "arrow"),
        el("pre", JSON.stringify(o.output === undefined ? o : o.output), "payload"));
    }
    ul.append(li);
  }
  if (!ul.childElementCount) ul.append(el("li", "no output", "hint"));
  return ul;
}

// ---- trace -----------------------------------------------------------------------------------

function traceItem(rec) {
  const li = el("li", undefined, "record");
  const t = rec.trigger || {};
  const meta = el("div", undefined, "meta");
  meta.append(el("time", clock(Date.parse(rec.at) || Date.now())), " ",
    el("span", rec.result, `badge ${rec.result}`), " ");
  const about = [t.type === "will" && "a Will", t.qos !== undefined && `qos ${t.qos}`,
    t.retain && "retained", t.clientid && `client ${t.clientid}`, t.username && `user ${t.username}`];
  meta.append(el("span", t.type === "event" ? t.event : t.topic, "topic"),
    el("span", ` ${about.filter(Boolean).join(", ")}`, "hint"));
  li.append(meta);
  if (t.payload !== undefined) {
    li.append(el("pre", payloadText(t.payload, t.payload_encoding), "payload"));
    if (t.truncated) li.append(el("span", `first part of ${count(t.payload_bytes)} bytes`, "hint"));
  }
  if (rec.error) li.append(el("p", rec.error, "error"));
  li.append(outputList(rec.outputs));
  if (rec.outputs_omitted) li.append(el("span", `and ${rec.outputs_omitted} more outputs`, "hint"));
  return li;
}

function renderTrace() {
  const id = state.selected;
  $("trace-rule").textContent = id ? `of ${id}` : "(choose a rule)";
  $("trace").replaceChildren(...(id ? state.traces.get(id) || [] : []).map(traceItem));
}

// ---- live data -----------------------------------------------------------------------------

function filterMatches(topic) {
  const f = $("d-filter").value.trim();
  if (!f) return true;
  if (f.includes("+") || f.includes("#")) return mqttMatch(f, topic);
  return topic.toLowerCase().includes(f.toLowerCase());
}

function dataItem(m) {
  const root = m.topic.split("/", 1)[0];
  const kind = DEVICE_ROOTS.includes(root) ? "device" : "derived";
  const li = el("li", undefined, `msg ${kind} ${root}`);
  const meta = el("div", undefined, "meta");
  meta.append(el("time", clock(m.at)), " ", el("span", kind, `badge ${kind}`), " ", el("span", m.topic, "topic"));
  if (m.retain) meta.append(" ", el("span", "retained", "badge"));
  li.append(meta, el("pre", payloadText(m.payload, m.encoding), "payload"));
  if (m.truncated) li.append(el("span", `first part of ${count(m.bytes)} bytes`, "hint"));
  return li;
}

function addData(m) {
  state.data.push(m);
  if (state.data.length > KEEP_DATA) state.data.shift();
  if ($("d-pause").checked || !filterMatches(m.topic)) return;
  const list = $("data");
  list.prepend(dataItem(m));
  while (list.childElementCount > SHOW_DATA) list.lastElementChild.remove();
}

function renderData() {
  const shown = state.data.filter((m) => filterMatches(m.topic)).slice(-SHOW_DATA).reverse();
  $("data").replaceChildren(...shown.map(dataItem));
}

// ---- the live feed -------------------------------------------------------------------------

function record(m) {
  if (m.truncated || m.encoding !== "utf8") return null;
  try {
    const v = JSON.parse(m.payload);
    return v && typeof v === "object" ? v : null;
  } catch {
    return null;
  }
}

function onMessage(m) {
  const p = m.topic.split("/");
  if (p[0] !== "$SYS") {
    addData(m);
    return;
  }
  const rec = record(m);
  if (!rec || p[1] !== "brokers") return;
  if (p.length === 4 && p[3] === "rules") onSummary(rec);
  else if (p.length === 5 && p[3] === "rules") onRuleStats(p[4], rec);
  else if (p.length === 6 && p[3] === "trace" && p[4] === "rules") onTrace(p[5], rec);
}

function onSummary(s) {
  state.summary = s;
  state.summaryAt = Date.now();
  if (s.interval_secs) state.interval = s.interval_secs;
  if (state.banner === "quiet") setBanner(null, "");
  if (state.list && s.digest !== state.list.digest) scheduleRefresh();
  if (state.awaiting && s.digest === state.awaiting.digest) sawDigest(s);
  renderHeader();
}

function onRuleStats(id, rec) {
  state.stats.set(id, rec);
  updateRow(id);
}

function onTrace(id, rec) {
  const list = state.traces.get(id) || [];
  list.unshift(rec);
  if (list.length > KEEP_TRACE) list.length = KEEP_TRACE;
  state.traces.set(id, list);
  if (id !== state.selected) return;
  const ol = $("trace");
  ol.prepend(traceItem(rec));
  while (ol.childElementCount > KEEP_TRACE) ol.lastElementChild.remove();
}

function setFeed(mqtt) {
  if (mqtt.state === "connected") {
    if (!state.mqttUpSince) state.mqttUpSince = Date.now();
    $("s-feed").textContent = "live";
  } else {
    state.mqttUpSince = 0;
    $("s-feed").textContent = `MQTT ${mqtt.state}: ${mqtt.detail}`;
  }
}

function openStream() {
  if (state.es) return;
  const es = new EventSource("/api/events");
  state.es = es;
  es.addEventListener("hello", (e) => {
    const h = JSON.parse(e.data);
    state.interval = h.sys_interval_secs || state.interval;
    setFeed(h.mqtt);
  });
  es.addEventListener("mqtt", (e) => setFeed(JSON.parse(e.data)));
  es.addEventListener("msg", (e) => onMessage(JSON.parse(e.data)));
  es.addEventListener("dropped", (e) => {
    $("s-feed").textContent = `live (this page fell behind: ${JSON.parse(e.data).n} messages skipped)`;
  });
  es.addEventListener("error", () => {
    state.mqttUpSince = 0;
    if (es.readyState !== EventSource.CLOSED) {
      $("s-feed").textContent = "reconnecting";
      return;
    }
    // Refused (server.py serves at most 8 open pages) or gone: try again in a while.
    state.es = null;
    $("s-feed").textContent = "refused: too many open pages? Retrying.";
    setTimeout(() => {
      if (!document.hidden) openStream();
    }, 5000);
  });
}

function closeStream() {
  if (state.es) state.es.close();
  state.es = null;
  state.mqttUpSince = 0;
  $("s-feed").textContent = "paused while the tab is hidden";
}

// No summary for three intervals while MQTT is up: the broker publishes no statistics.
function watchdog() {
  if (!state.mqttUpSince || state.banner === "predates") return;
  const since = Math.max(state.mqttUpSince, state.summaryAt);
  const quiet = (Date.now() - since) / 1000;
  if (quiet > 3 * state.interval + 1) {
    setBanner("quiet", `No rule statistics on $SYS/brokers/+/rules for ${Math.round(quiet)} s: ` +
      "this broker predates ADR 0084, or its statistics are off. Rebuild it with demo/rules-live/up.sh.");
  }
  if (state.list && state.summary) renderHeader(); // the "… ago" texts
}

function init() {
  $("new-rule").addEventListener("click", newRule);
  $("b-check").addEventListener("click", checkRule);
  $("b-test").addEventListener("click", testRule);
  $("b-apply").addEventListener("click", applyRule);
  $("b-delete").addEventListener("click", deleteRule);
  $("b-load").addEventListener("click", loadSource);
  $("b-fcheck").addEventListener("click", checkFile);
  $("b-fapply").addEventListener("click", applyFile);
  $("b-reset").addEventListener("click", resetFile);
  $("d-filter").addEventListener("input", renderData);
  $("d-pause").addEventListener("change", renderData);
  $("t-topic").addEventListener("input", () => { $("t-custom").checked = true; });
  $("t-payload").addEventListener("input", () => { $("t-custom").checked = true; });
  $("rule-form").addEventListener("submit", (e) => e.preventDefault());
  // A hidden tab keeps no stream open: browsers allow only a few connections per host.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      closeStream();
    } else {
      openStream();
      loadRules();
    }
  });
  $("b-delete").disabled = true;
  renderTrace();
  loadRules();
  if (!document.hidden) openStream();
  setInterval(watchdog, 1000);
}

init();
