//! Minimal server-rendered HTML: a landing page with sign-in options and a
//! dashboard for managing keys and stations. No framework, no build step -
//! inline strings and a sprinkle of vanilla `fetch` against the `/v1` API.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};

use crate::AppState;

// Same dark theme as openseafeed.com and the live map.
const STYLE: &str = r#"
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { background: #0b1420; color: #cfe3f5; font: 16px/1.6 system-ui, sans-serif; margin: 0; }
  header.site { display: flex; gap: 1.5rem; align-items: baseline; padding: 1rem 1.5rem;
                border-bottom: 1px solid #1d3242; }
  header.site h1 { font-size: 1.05rem; font-weight: 600; letter-spacing: .04em;
                   color: #7fd4a8; margin: 0; }
  header.site nav { margin-left: auto; display: flex; gap: 1.25rem; font-size: .95rem; }
  header.site nav a { text-decoration: none; color: #9fb8cd; }
  header.site nav a:hover { color: #7fd4a8; }
  main { max-width: 52rem; margin: 0 auto; padding: 2.5rem 1.5rem 4rem; }
  h2 { color: #fff; font-size: 1.5rem; margin: 0 0 .5rem; }
  h3 { color: #7fd4a8; font-size: 1.05rem; letter-spacing: .03em; margin: 2.2rem 0 .6rem; }
  p { margin: .5rem 0; }
  .muted { color: #9fb8cd; }
  a { color: #7fd4a8; }
  code { background: #0e1b2a; border: 1px solid #1d3242; border-radius: 4px;
         padding: .1rem .4rem; color: #d6ffe4; font-size: .88em;
         font-family: ui-monospace, monospace; overflow-wrap: anywhere; }
  .card { background: #0e1b2a; border: 1px solid #1d3242; border-radius: 8px;
          padding: .9rem 1.1rem; margin: .75rem 0; }
  .card b { color: #fff; }
  .btn { display: inline-block; padding: .5rem 1.1rem; border-radius: 6px;
         border: 1px solid #1f7a44; background: #14532d; color: #d6ffe4;
         font: inherit; font-weight: 600; cursor: pointer; text-decoration: none; }
  .btn:hover { background: #1a6b3a; }
  .btn.ghost { background: transparent; border-color: #24455f; color: #cfe3f5;
               font-weight: 400; }
  .btn.ghost:hover { border-color: #7fd4a8; }
  input, select { background: #0e1b2a; color: #cfe3f5; border: 1px solid #24455f;
                  border-radius: 6px; padding: .5rem .6rem; font: inherit; }
  table { border-collapse: collapse; width: 100%; margin-top: .5rem; }
  th, td { text-align: left; padding: .45rem .6rem; border-bottom: 1px solid #16283a;
           font-size: .9rem; }
  th { color: #7f9ab0; font-weight: 600; }
  form.inline { margin: .75rem 0; display: flex; gap: .5rem; flex-wrap: wrap;
                align-items: center; }
</style>
"#;

/// Shared top bar linking the rest of the product.
const NAV: &str = r#"<header class="site"><h1>OPENSEAFEED</h1><nav>
  <a href="https://stream.openseafeed.com/">Live map</a>
  <a href="https://openseafeed.com/docs.html">API docs</a>
  <a href="https://openseafeed.com/account.html">Account</a>
  <a href="https://openseafeed.com/">About</a>
</nav></header>"#;

/// `GET /` - landing page. Shows only the sign-in methods this server has
/// configured, plus the always-available magic-link form.
pub async fn landing(State(state): State<AppState>) -> Html<String> {
    let mut providers = String::new();
    if state.cfg.github.is_some() {
        providers.push_str(r#"<a class="btn" href="/auth/github">Sign in with GitHub</a> "#);
    }
    if state.cfg.google.is_some() {
        providers.push_str(r#"<a class="btn" href="/auth/google">Sign in with Google</a> "#);
    }
    let email_intro = if providers.is_empty() {
        "Enter your email and we send a one-time sign-in link. No password, no separate \
         registration step - your first sign-in creates the account."
    } else {
        "Or use your email: we send a one-time sign-in link. Your first sign-in creates \
         the account."
    };

    let body = format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenSeaFeed - sign in</title>{STYLE}</head>
<body>
{NAV}
<main>
  <h2>Sign in</h2>
  <p class="muted">OpenSeaFeed is a free, community-owned AIS network. The
     <a href="https://stream.openseafeed.com/">live map</a> and most of the
     <a href="https://openseafeed.com/docs.html">API</a> work without any account.
     Sign in when you want a key of your own:</p>

  <div class="card"><b>Use the data</b> -
    <span class="muted">a <code>live</code> key lifts the anonymous limits: bigger map areas
    on the stream and deeper history queries.</span></div>
  <div class="card"><b>Contribute data</b> -
    <span class="muted">a <code>feed</code> key (or a registered receiver station) lets you push
    AIS into the network. Active contributors get the highest tier: unlimited streaming and
    the freshest data.</span></div>

  <h3>Sign in</h3>
  <p>{providers}</p>
  <p class="muted">{email_intro}</p>
  <form class="inline" id="magic">
    <input type="email" name="email" placeholder="you@example.com" required>
    <button class="btn" type="submit">Email me a sign-in link</button>
  </form>
  <p class="muted" id="magic-msg"></p>
</main>

<script>
document.getElementById('magic').addEventListener('submit', async (e) => {{
  e.preventDefault();
  const email = e.target.email.value;
  const r = await fetch('/auth/magic', {{
    method: 'POST', headers: {{ 'content-type': 'application/json' }},
    body: JSON.stringify({{ email }})
  }});
  const j = await r.json();
  document.getElementById('magic-msg').textContent = j.message || j.error || 'Done.';
}});
</script>
</body></html>"#
    );
    Html(body)
}

/// `GET /dashboard` - requires a session. Renders the user's keys and stations
/// and small forms that call the `/v1` API and reload.
pub async fn dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let user = match state.current_user(&headers).await {
        Some(u) => u,
        None => return Redirect::to("/").into_response(),
    };

    let (keys, stations, tier) = {
        let conn = state.db.lock().await;
        let keys = crate::db::keys_for_user(&conn, &user.id).unwrap_or_default();
        let stations = crate::db::stations_for_user(&conn, &user.id).unwrap_or_default();
        let tier = crate::db::tier_for_user(&conn, &user.id).unwrap_or("free");
        (keys, stations, tier)
    };

    let who = user
        .display_name
        .clone()
        .or_else(|| user.email.clone())
        .unwrap_or_else(|| user.id.clone());

    let mut key_rows = String::new();
    for k in keys.iter().filter(|k| k.revoked_at.is_none()) {
        key_rows.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&k.key),
            html_escape(&k.kind),
            html_escape(k.label.as_deref().unwrap_or("")),
            revoke_button(&k.kind, &k.key),
        ));
    }
    if key_rows.is_empty() {
        key_rows.push_str(r#"<tr><td colspan="4" class="muted">No keys yet.</td></tr>"#);
    }

    let mut station_rows = String::new();
    for s in &stations {
        station_rows.push_str(&format!(
            "<tr><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
            html_escape(&s.name),
            html_escape(&s.key),
            s.msgs_total,
            html_escape(s.last_seen_at.as_deref().unwrap_or("never")),
        ));
    }
    if station_rows.is_empty() {
        station_rows.push_str(r#"<tr><td colspan="4" class="muted">No stations yet.</td></tr>"#);
    }

    let body = format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenSeaFeed - dashboard</title>{STYLE}</head>
<body>
{NAV}
<main>
  <h2>Dashboard</h2>
  <p class="muted">Signed in as <strong>{who}</strong> - tier: <strong>{tier}</strong>
    <span class="muted">(contributor = you actively feed the network; it unlocks unlimited
    streaming and the freshest data)</span>
    &nbsp; <form class="inline" style="display:inline" method="post" action="/auth/logout">
    <button class="btn ghost" type="submit">Log out</button></form></p>

  <h3>API keys</h3>
  <p class="muted"><code>live</code> keys read the API (stream, snapshot, history);
    <code>feed</code> keys authenticate data you push to
    <code>ingest.openseafeed.com</code>. Treat them like passwords - anyone holding a key
    can use it. See the <a href="https://openseafeed.com/docs.html">API docs</a> for how
    to use each kind.</p>
  <table><thead><tr><th>Key</th><th>Kind</th><th>Label</th><th></th></tr></thead>
    <tbody>{key_rows}</tbody></table>
  <form class="inline" id="newkey">
    <select name="kind"><option value="live">live (use the data)</option><option value="feed">feed (contribute data)</option></select>
    <input type="text" name="label" placeholder="label, e.g. my-app (optional)">
    <button class="btn" type="submit">Create key</button>
  </form>

  <h3>Receiver stations</h3>
  <p class="muted">A station is your own AIS receiver (RTL-SDR, AIS-catcher, dAISy, ...).
    Registering one gives it its own key to feed with; a station seen in the last 7 days
    makes you a contributor. Position is optional and only used for coverage maps.</p>
  <table><thead><tr><th>Name</th><th>Station key</th><th>Msgs</th><th>Last seen</th></tr></thead>
    <tbody>{station_rows}</tbody></table>
  <form class="inline" id="newstation">
    <input type="text" name="name" placeholder="station name, e.g. rooftop-enschede" required>
    <input type="number" step="any" name="lat" placeholder="lat (optional)">
    <input type="number" step="any" name="lon" placeholder="lon (optional)">
    <button class="btn" type="submit">Register station</button>
  </form>
</main>

<script>
async function post(url, body) {{
  const r = await fetch(url, {{ method: 'POST',
    headers: {{ 'content-type': 'application/json' }}, body: JSON.stringify(body) }});
  if (!r.ok) {{ alert((await r.json()).error || 'Request failed'); return false; }}
  return true;
}}
document.getElementById('newkey').addEventListener('submit', async (e) => {{
  e.preventDefault();
  if (await post('/v1/keys', {{ kind: e.target.kind.value, label: e.target.label.value || null }}))
    location.reload();
}});
document.getElementById('newstation').addEventListener('submit', async (e) => {{
  e.preventDefault();
  const lat = e.target.lat.value ? parseFloat(e.target.lat.value) : null;
  const lon = e.target.lon.value ? parseFloat(e.target.lon.value) : null;
  if (await post('/v1/stations', {{ name: e.target.name.value, lat, lon }}))
    location.reload();
}});
async function revoke(key) {{
  if (!confirm('Revoke this key?')) return;
  const r = await fetch('/v1/keys/' + encodeURIComponent(key), {{ method: 'DELETE' }});
  if (r.ok) location.reload(); else alert('Revoke failed');
}}
</script>
</body></html>"#
    );
    (StatusCode::OK, Html(body)).into_response()
}

/// Station keys are revoked with their station in this MVP, so only user-held
/// key kinds get a revoke button.
fn revoke_button(kind: &str, key: &str) -> String {
    if kind == "station" {
        String::new()
    } else {
        format!(
            r#"<button class="btn" onclick="revoke('{}')">revoke</button>"#,
            html_escape(key)
        )
    }
}

/// Minimal HTML entity escaping for interpolated values.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
