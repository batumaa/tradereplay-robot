// Every route, as a pure function of the request and the robot, so tests reach them without a Durable Object.
import { validateSignal } from "./signal.ts";
import type { Limits, Robot } from "./robot.ts";

export interface Env { ROBOT_TOKEN?: string; SITE_ORIGINS?: string }

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

const bearer = (request: Request) => {
  const h = request.headers.get("authorization") ?? "";
  return h.startsWith("Bearer ") ? h.slice(7) : "";
};

/** The token the hub and engine use: from the Worker's env, or made at pairing. */
async function authed(request: Request, env: Env, robot: Robot): Promise<boolean> {
  const given = bearer(request);
  if (!given) return false;
  if (env.ROBOT_TOKEN && given === env.ROBOT_TOKEN) return true;
  const held = await robot.pairedToken();
  return !!held && given === held;
}

/** The owner: the token, or a session bought with a code. Never reaches /signal or /strategies. */
async function owner(request: Request, env: Env, robot: Robot): Promise<boolean> {
  if (await authed(request, env, robot)) return true;
  const given = bearer(request);
  return !!given && robot.sessionOk(given);
}

async function body(request: Request): Promise<any> {
  try { return await request.json(); } catch { return null; }
}

function siteHeaders(request: Request, env: Env): Record<string, string> | null {
  const origin = request.headers.get("origin");
  if (!origin) return {};
  const allowed = (env.SITE_ORIGINS ?? "").split(",").map((o) => o.trim()).filter(Boolean);
  return allowed.includes(origin) ? { "access-control-allow-origin": origin, vary: "Origin" } : null;
}

export async function handle(request: Request, env: Env, robot: Robot): Promise<Response> {
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === "/ping" || path === "/pair") {
    const cors = siteHeaders(request, env);
    if (cors === null) return json({ error: "forbidden" }, 403);
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: { ...cors, "access-control-allow-methods": "GET, POST", "access-control-allow-headers": "content-type" } });
    }
    if (path === "/ping" && request.method === "GET") {
      return json({ robot: true, paired: !!env.ROBOT_TOKEN || !!(await robot.pairedToken()) }, 200, cors);
    }
    if (path === "/pair" && request.method === "POST") {
      const b = await body(request);
      if (typeof b?.code !== "string" || !b.code) return json({ error: "A code is required." }, 400, cors);
      const r = await robot.pair(url.origin, b.code);
      return r.ok ? json({ paired: true }, 200, cors) : json({ error: r.error }, r.status, cors);
    }
    return json({ error: "not found" }, 404, cors);
  }
  if (path === "/broker" && request.method === "GET") {
    const page = env.ROBOT_TOKEN ? tokenPage(await robot.connected()) : ownerPage();
    return new Response(page, { headers: { "content-type": "text/html; charset=utf-8" } });
  }
  if (path === "/session" && request.method === "POST") {
    const b = await body(request);
    if (typeof b?.code !== "string" || !b.code) return json({ error: "A code is required." }, 400);
    const r = await robot.openSession(b.code);
    return r.ok ? json({ session: r.session }) : json({ error: r.error }, r.status);
  }

  // The owner's routes, and the hub's.
  if (path === "/broker" && request.method === "POST") {
    if (!(await owner(request, env, robot))) return json({ error: "unauthorized" }, 401);
    const b = await body(request);
    if (!b || typeof b.keyId !== "string" || typeof b.secret !== "string" || !b.keyId || !b.secret) return json({ error: "Key id and secret are required." }, 400);
    const r = await robot.saveBroker(b.keyId, b.secret);
    if (!r.ok) return json({ error: r.reason }, 400);
    await robot.reportBroker();
    return json({ connected: true, paper: r.paper });
  }
  if (path === "/status" && request.method === "GET") {
    if (!(await owner(request, env, robot))) return json({ error: "unauthorized" }, 401);
    return json(await robot.status());
  }
  if (path === "/kill" && request.method === "POST") {
    if (!(await owner(request, env, robot))) return json({ error: "unauthorized" }, 401);
    const b = await body(request);
    return json(await robot.kill(b?.flatten === true));
  }
  if (path === "/resume" && request.method === "POST") {
    if (!(await owner(request, env, robot))) return json({ error: "unauthorized" }, 401);
    await robot.resume();
    return json({ resumed: true });
  }
  if (path === "/limits" && request.method === "POST") {
    if (!(await owner(request, env, robot))) return json({ error: "unauthorized" }, 401);
    const b = await body(request);
    return json(await robot.setLimits((b ?? {}) as Partial<Limits>));
  }

  if (!(await authed(request, env, robot))) return json({ error: "unauthorized" }, 401);
  if (path === "/signal" && request.method === "POST") {
    const v = validateSignal(await body(request));
    if (!v.ok) return json({ status: "rejected", reason: v.reason });
    return json(await robot.onSignal(v.signal));
  }
  const m = /^\/strategies\/([^/]+)$/.exec(path);
  if (m && request.method === "POST") {
    const b = await body(request);
    if (typeof b?.capital !== "number" || !(b.capital > 0)) return json({ error: "capital must be a positive number" }, 400);
    await robot.setStrategy(decodeURIComponent(m[1]), b.capital);
    return json({ saved: true });
  }
  return json({ error: "not found" }, 404);
}

const PAGE_STYLE = `*{box-sizing:border-box}body{margin:0;background:#F5F3EE;color:#111111;font-family:Georgia,serif;padding:28px 16px}
main{max-width:520px;margin:0 auto;display:flex;flex-direction:column;gap:18px}
h1{margin:0;font-size:clamp(34px,8vw,52px);line-height:1.02;font-weight:900;border-bottom:3px solid #111111;padding-bottom:14px}
h2{margin:12px 0 0;font-size:24px;font-weight:900;border-bottom:1px solid #111111;padding-bottom:8px}
#state,#run{font-style:italic;font-size:18px;color:#444444}
label{display:flex;flex-direction:column;gap:8px;font:800 11px system-ui,sans-serif;letter-spacing:.16em;text-transform:uppercase}
input{height:44px;background:transparent;border:0;border-bottom:2px solid #111111;font:500 16px ui-monospace,monospace;color:#111111;outline:none;width:100%}
input:focus-visible,button:focus-visible{outline:2px solid #111111;outline-offset:2px}
button{height:44px;border:2px solid #111111;background:#111111;color:#F5F3EE;font:800 12px system-ui,sans-serif;letter-spacing:.16em;text-transform:uppercase;cursor:pointer;align-self:flex-start;padding:0 24px}
button.plain{background:transparent;color:#111111}button:disabled{opacity:.4;cursor:default}
.row{display:flex;justify-content:space-between;gap:12px;font:500 15px ui-monospace,monospace;border-bottom:1px solid #CFCBC2;padding:8px 0}
.acts{display:flex;flex-wrap:wrap;gap:12px}
#err,#err2{min-height:20px;font-size:15px}`;

/** Paired robot: the owner arrives from TradeReplay with a code in the address's fragment, which never reaches a log. */
function ownerPage(): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Your robot</title>
<style>${PAGE_STYLE}</style></head><body><main>
<h1>Your robot</h1>
<div id="none" hidden>Open this page from TradeReplay.</div>
<div id="owner" hidden style="display:flex;flex-direction:column;gap:18px">
<div id="run"></div>
<div id="positions"></div>
<form id="lim" style="display:flex;flex-direction:column;gap:18px" autocomplete="off">
<label>Max per trade<input id="maxPerTrade" inputmode="numeric" required></label>
<button type="submit" class="plain">Save</button>
</form>
<div class="acts"><button type="button" class="plain" id="resume" disabled>Resume</button><button type="button" class="plain" id="kill">Kill</button><button type="button" id="killYes" hidden>Close everything now</button></div>
<div id="err2" role="alert"></div>
<h2>TradeZero</h2>
<div id="state"></div>
<form id="f" style="display:flex;flex-direction:column;gap:18px" autocomplete="off">
<label>API key id<input id="keyId" type="password" required></label>
<label>API secret<input id="secret" type="password" required></label>
<button type="submit">Save</button>
<div id="err" role="alert"></div>
</form>
</div>
</main><script>
const $ = (id) => document.getElementById(id);
const KEY = "robot_session";
const get = () => { try { return sessionStorage.getItem(KEY); } catch { return null; } };
const put = (v) => { try { v ? sessionStorage.setItem(KEY, v) : sessionStorage.removeItem(KEY); } catch {} };
let session = get();
const lockOut = () => { put(null); session = null; $("owner").hidden = true; $("owner").style.display = "none"; $("none").hidden = false; };
async function call(path, method, payload) {
  const r = await fetch(path, { method, headers: { "content-type": "application/json", authorization: "Bearer " + session }, body: payload === undefined ? undefined : JSON.stringify(payload) });
  if (r.status === 401) { lockOut(); throw new Error("Open this page again from TradeReplay."); }
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || "Could not save.");
  return j;
}
async function draw() {
  const s = await call("/status", "GET");
  $("state").textContent = !s.broker ? "Not connected" : s.paper ? "Connected" : (s.why || "Not a paper account");
  const stop = s.paused ? (s.paused.reason || "paused") : "";
  $("run").textContent = stop ? "Paused: " + stop + (s.drift ? " · " + JSON.stringify(s.drift) : "") : "Running";
  $("resume").disabled = !s.paused;
  const held = Object.entries(s.holdings || {}).filter(([, q]) => q !== 0);
  $("positions").replaceChildren(...(held.length ? [] : [Object.assign(document.createElement("div"), { className: "row", textContent: "No open positions" })]), ...held.map(([k, q]) => { const d = document.createElement("div"); d.className = "row"; const [a, b] = k.split("|"); d.append(Object.assign(document.createElement("span"), { textContent: b + " · " + a }), Object.assign(document.createElement("span"), { textContent: String(q) })); return d; }));
  if (document.activeElement !== $("maxPerTrade")) $("maxPerTrade").value = "$" + s.limits.maxPerTrade;
}
const show = (fn, box) => async (e) => { e?.preventDefault(); $(box).textContent = ""; try { await fn(); await draw(); } catch (err) { if (session) $(box).textContent = err.message === "Failed to fetch" ? "Could not reach the robot." : err.message; } };
$("f").addEventListener("submit", show(async () => {
  await call("/broker", "POST", { keyId: $("keyId").value.trim(), secret: $("secret").value.trim() });
  $("keyId").value = ""; $("secret").value = "";
}, "err"));
$("lim").addEventListener("submit", show(async () => {
  const n = Number($("maxPerTrade").value.replace(/[$,\\s]/g, ""));
  if (!Number.isFinite(n) || n <= 0) throw new Error("Max per trade must be a positive number.");
  await call("/limits", "POST", { maxPerTrade: n });
  document.activeElement?.blur?.();
}, "err2"));
$("resume").addEventListener("click", show(() => call("/resume", "POST", {}), "err2"));
$("kill").addEventListener("click", () => { $("killYes").hidden = false; });
$("killYes").addEventListener("click", show(async () => { $("killYes").hidden = true; await call("/kill", "POST", { flatten: true }); }, "err2"));
(async () => {
  const m = /^#code=(.+)$/.exec(location.hash);
  if (m) {
    history.replaceState(null, "", location.pathname);
    try {
      const r = await fetch("/session", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ code: decodeURIComponent(m[1]) }) });
      const j = await r.json();
      if (r.ok) { session = j.session; put(session); }
      else if (!session) { $("none").textContent = j.error || $("none").textContent; }
    } catch { if (!session) $("none").textContent = "Could not reach the robot."; }
  }
  if (!session) { $("none").hidden = false; return; }
  $("owner").hidden = false;
  $("owner").style.display = "flex";
  await show(async () => {}, "err2")();
})();
</script></body></html>`;
}

function tokenPage(connected: boolean): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Your robot</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#F5F3EE;color:#111111;font-family:Georgia,serif;padding:28px 16px}
main{max-width:520px;margin:0 auto;display:flex;flex-direction:column;gap:18px}
h1{margin:0;font-size:clamp(34px,8vw,52px);line-height:1.02;font-weight:900;border-bottom:3px solid #111111;padding-bottom:14px}
#state{font-style:italic;font-size:18px;color:#444444}
label{display:flex;flex-direction:column;gap:8px;font:800 11px system-ui,sans-serif;letter-spacing:.16em;text-transform:uppercase}
input{height:44px;background:transparent;border:0;border-bottom:2px solid #111111;font:500 16px ui-monospace,monospace;color:#111111;outline:none;width:100%}
button{height:44px;border:2px solid #111111;background:#111111;color:#F5F3EE;font:800 12px system-ui,sans-serif;letter-spacing:.16em;text-transform:uppercase;cursor:pointer;align-self:flex-start;padding:0 24px}
#err{min-height:20px;font-size:15px}
</style></head><body><main>
<h1>TradeZero</h1>
<div id="state">${connected ? "Connected" : "Not connected"}</div>
<form id="f" style="display:flex;flex-direction:column;gap:18px" autocomplete="off">
<label>Robot token<input id="token" type="password" required></label>
<label>API key id<input id="keyId" type="password" required></label>
<label>API secret<input id="secret" type="password" required></label>
<button type="submit">Save</button>
<div id="err" role="alert"></div>
</form>
</main><script>
document.getElementById("f").addEventListener("submit", async (e) => {
  e.preventDefault();
  const v = (id) => document.getElementById(id).value.trim();
  const err = document.getElementById("err");
  err.textContent = "";
  try {
    const r = await fetch("/broker", { method: "POST", headers: { "content-type": "application/json", authorization: "Bearer " + v("token") }, body: JSON.stringify({ keyId: v("keyId"), secret: v("secret") }) });
    const j = await r.json();
    if (!r.ok) { err.textContent = j.error === "unauthorized" ? "Wrong robot token." : j.error || "Could not save."; return; }
    document.getElementById("state").textContent = "Connected";
    document.getElementById("keyId").value = "";
    document.getElementById("secret").value = "";
  } catch { err.textContent = "Could not reach the robot."; }
});
</script></body></html>`;
}
