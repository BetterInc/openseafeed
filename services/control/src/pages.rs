//! Minimal server-rendered HTML: a landing page with sign-in options and a
//! dashboard for managing keys and stations. No framework, no build step —
//! inline strings and a sprinkle of vanilla `fetch` against the `/v1` API.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};

use crate::AppState;

const STYLE: &str = r#"
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, sans-serif; max-width: 52rem; margin: 3rem auto;
         padding: 0 1rem; line-height: 1.5; }
  h1 { font-size: 1.6rem; } h2 { margin-top: 2rem; font-size: 1.2rem; }
  .muted { opacity: 0.7; } code { font-family: ui-monospace, monospace; }
  .btn { display: inline-block; padding: 0.5rem 0.9rem; margin: 0.25rem 0.25rem 0.25rem 0;
         border: 1px solid currentColor; border-radius: 0.4rem; text-decoration: none;
         background: transparent; color: inherit; cursor: pointer; font-size: 1rem; }
  input { padding: 0.5rem; font-size: 1rem; border-radius: 0.4rem;
          border: 1px solid #8888; }
  table { border-collapse: collapse; width: 100%; margin-top: 0.5rem; }
  th, td { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 1px solid #8884;
           font-size: 0.9rem; }
  form.inline { margin: 0.5rem 0; }
</style>
"#;

/// `GET /` — landing page. Shows only the sign-in methods this server has
/// configured, plus the always-available magic-link form.
pub async fn landing(State(state): State<AppState>) -> Html<String> {
    let mut providers = String::new();
    if state.cfg.github.is_some() {
        providers.push_str(r#"<a class="btn" href="/auth/github">Sign in with GitHub</a>"#);
    }
    if state.cfg.google.is_some() {
        providers.push_str(r#"<a class="btn" href="/auth/google">Sign in with Google</a>"#);
    }
    if providers.is_empty() {
        providers.push_str(
            r#"<p class="muted">No OAuth providers are configured. Use the email link below.</p>"#,
        );
    }

    let body = format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenSeaFeed</title>{STYLE}</head>
<body>
  <h1>OpenSeaFeed</h1>
  <p class="muted">An open community AIS network. Sign in to manage API keys and register stations.</p>

  <h2>Sign in</h2>
  {providers}

  <h2>Email sign-in link</h2>
  <form class="inline" id="magic">
    <input type="email" name="email" placeholder="you@example.com" required>
    <button class="btn" type="submit">Email me a link</button>
  </form>
  <p class="muted" id="magic-msg"></p>

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

/// `GET /dashboard` — requires a session. Renders the user's keys and stations
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
<title>OpenSeaFeed — Dashboard</title>{STYLE}</head>
<body>
  <h1>Dashboard</h1>
  <p class="muted">Signed in as <strong>{who}</strong> — tier: <strong>{tier}</strong>
    &nbsp; <form class="inline" style="display:inline" method="post" action="/auth/logout">
    <button class="btn" type="submit">Log out</button></form></p>

  <h2>API keys</h2>
  <table><thead><tr><th>Key</th><th>Kind</th><th>Label</th><th></th></tr></thead>
    <tbody>{key_rows}</tbody></table>
  <form class="inline" id="newkey">
    <select name="kind"><option value="live">live</option><option value="feed">feed</option></select>
    <input type="text" name="label" placeholder="label (optional)">
    <button class="btn" type="submit">Create key</button>
  </form>

  <h2>Stations</h2>
  <table><thead><tr><th>Name</th><th>Station key</th><th>Msgs</th><th>Last seen</th></tr></thead>
    <tbody>{station_rows}</tbody></table>
  <form class="inline" id="newstation">
    <input type="text" name="name" placeholder="station name" required>
    <input type="number" step="any" name="lat" placeholder="lat">
    <input type="number" step="any" name="lon" placeholder="lon">
    <button class="btn" type="submit">Register station</button>
  </form>

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
