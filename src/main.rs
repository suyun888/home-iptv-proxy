use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    extract::{Form, Path, Query, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, Uri},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::TryStreamExt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::{net::TcpListener, sync::RwLock, time};
use tracing::{info, warn};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct AppState {
    client: Client,
    config_path: Arc<String>,
    config: Arc<RwLock<Config>>,
    runtime: Arc<RwLock<RuntimeState>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    bind: Option<String>,
    public_base_url: Option<String>,
    refresh_minutes: Option<u64>,
    user_agent: Option<String>,
    signing_secret: String,
    sources: Vec<SourceConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceConfig {
    name: String,
    url: String,
    enabled: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
struct Channel {
    id: String,
    name: String,
    group: String,
    tvg_id: Option<String>,
    tvg_logo: Option<String>,
    source_name: String,
    upstream_url: String,
}

#[derive(Default)]
struct RuntimeState {
    channels: Vec<Channel>,
    by_id: HashMap<String, Channel>,
    source_errors: BTreeMap<String, String>,
    last_refresh_ok: bool,
}

#[derive(Debug, Deserialize)]
struct ProxyQuery {
    u: String,
    s: String,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    channels: usize,
    last_refresh_ok: bool,
    source_errors: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct AdminForm {
    public_base_url: Option<String>,
    refresh_minutes: Option<String>,
    user_agent: Option<String>,
    signing_secret: Option<String>,
    source_name: Option<Vec<String>>,
    source_url: Option<Vec<String>>,
    source_enabled: Option<Vec<String>>,
}

#[derive(Serialize)]
struct AdminPageData {
    public_base_url: String,
    refresh_minutes: String,
    user_agent: String,
    signing_secret: String,
    sources: Vec<AdminSourceView>,
    status_message: String,
    status_class: String,
}

#[derive(Serialize)]
struct AdminSourceView {
    name: String,
    url: String,
    enabled: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = env::var("IPTV_CONFIG").unwrap_or_else(|_| "config/sources.yaml".to_string());
    let config_text = tokio::fs::read_to_string(&config_path).await?;
    let config: Config = serde_yaml::from_str(&config_text)?;

    let user_agent = config
        .user_agent
        .clone()
        .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string());

    let client = Client::builder()
        .user_agent(user_agent)
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let state = AppState {
        client,
        config_path: Arc::new(config_path),
        config: Arc::new(RwLock::new(config)),
        runtime: Arc::new(RwLock::new(RuntimeState::default())),
    };

    refresh_channels(&state).await;
    spawn_refresh_loop(state.clone());

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/channels", get(channels))
        .route("/list.m3u", get(list_m3u))
        .route("/live/{id}", get(live))
        .route("/proxy/{id}", get(proxy))
        .route("/admin", get(admin_page))
        .route("/admin/save", post(save_admin))
        .with_state(state.clone());

    let bind = {
        let config = state.config.read().await;
        config
            .bind
            .clone()
            .unwrap_or_else(|| "0.0.0.0:8787".to_string())
    };
    let addr: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    let filter =
        env::var("RUST_LOG").unwrap_or_else(|_| "info,reqwest=warn,hyper=warn".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn spawn_refresh_loop(state: AppState) {
    tokio::spawn(async move {
        loop {
            let minutes = {
                let config = state.config.read().await;
                config.refresh_minutes.unwrap_or(30).max(1)
            };
            time::sleep(Duration::from_secs(minutes * 60)).await;
            refresh_channels(&state).await;
        }
    });
}

async fn refresh_channels(state: &AppState) {
    let config = state.config.read().await.clone();

    let mut channels = Vec::new();
    let mut by_id = HashMap::new();
    let mut errors = BTreeMap::new();
    let mut seen_keys = HashSet::new();

    for source in &config.sources {
        if source.enabled == Some(false) {
            continue;
        }

        match fetch_source_channels(&state.client, source).await {
            Ok(found) => {
                for channel in found {
                    let dedupe_key = format!("{}|{}", channel.name, channel.upstream_url);
                    if seen_keys.insert(dedupe_key) {
                        by_id.insert(channel.id.clone(), channel.clone());
                        channels.push(channel);
                    }
                }
            }
            Err(err) => {
                warn!("source {} refresh failed: {}", source.name, err);
                errors.insert(source.name.clone(), err.to_string());
            }
        }
    }

    channels.sort_by(|a, b| a.group.cmp(&b.group).then(a.name.cmp(&b.name)));
    let last_refresh_ok = !channels.is_empty() || errors.is_empty();

    let mut runtime = state.runtime.write().await;
    runtime.channels = channels;
    runtime.by_id = by_id;
    runtime.source_errors = errors;
    runtime.last_refresh_ok = last_refresh_ok;
}

async fn fetch_source_channels(
    client: &Client,
    source: &SourceConfig,
) -> anyhow::Result<Vec<Channel>> {
    let text = client
        .get(&source.url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut channels = Vec::new();
    let mut pending_meta: Option<M3uMeta> = None;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with("#EXTINF:") {
            pending_meta = Some(parse_extinf(line));
            continue;
        }

        if line.starts_with('#') {
            continue;
        }

        let meta = pending_meta.take().unwrap_or_else(|| M3uMeta {
            name: line.to_string(),
            group: "Ungrouped".to_string(),
            tvg_id: None,
            tvg_logo: None,
        });

        let id = build_channel_id(&source.name, &meta.name, line);
        channels.push(Channel {
            id,
            name: meta.name,
            group: meta.group,
            tvg_id: meta.tvg_id,
            tvg_logo: meta.tvg_logo,
            source_name: source.name.clone(),
            upstream_url: line.to_string(),
        });
    }

    Ok(channels)
}

#[derive(Default)]
struct M3uMeta {
    name: String,
    group: String,
    tvg_id: Option<String>,
    tvg_logo: Option<String>,
}

fn parse_extinf(line: &str) -> M3uMeta {
    let mut meta = M3uMeta {
        group: "Ungrouped".to_string(),
        ..M3uMeta::default()
    };

    if let Some(name) = line.split_once(',').map(|(_, name)| name.trim()) {
        if !name.is_empty() {
            meta.name = name.to_string();
        }
    }

    for key in ["group-title", "tvg-id", "tvg-logo"] {
        if let Some(value) = extract_attr(line, key) {
            match key {
                "group-title" => meta.group = value,
                "tvg-id" => meta.tvg_id = Some(value),
                "tvg-logo" => meta.tvg_logo = Some(value),
                _ => {}
            }
        }
    }

    if meta.name.is_empty() {
        meta.name = "Unnamed Channel".to_string();
    }
    meta
}

fn extract_attr(line: &str, key: &str) -> Option<String> {
    let needle = format!(r#"{key}=""#);
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn build_channel_id(source_name: &str, channel_name: &str, upstream_url: &str) -> String {
    let seed = format!("{source_name}:{channel_name}:{upstream_url}");
    let mut mac = HmacSha256::new_from_slice(seed.as_bytes()).expect("valid hmac key");
    mac.update(upstream_url.as_bytes());
    let digest = mac.finalize().into_bytes();
    let short = URL_SAFE_NO_PAD.encode(digest);
    let slug = channel_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .take(6)
        .collect::<Vec<_>>()
        .join("-");
    format!("{}-{}", slug, &short[..10])
}

async fn root() -> impl IntoResponse {
    Redirect::to("/admin")
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    Json(HealthResponse {
        ok: runtime.last_refresh_ok,
        channels: runtime.channels.len(),
        last_refresh_ok: runtime.last_refresh_ok,
        source_errors: runtime.source_errors.clone(),
    })
}

async fn channels(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    Json(runtime.channels.clone())
}

async fn list_m3u(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    let base = public_base_url(&state, &headers, &uri).await;

    let mut body = String::from("#EXTM3U\n");
    for channel in &runtime.channels {
        body.push_str("#EXTINF:-1");
        if let Some(tvg_id) = &channel.tvg_id {
            body.push_str(&format!(r#" tvg-id="{}""#, tvg_id));
        }
        if let Some(tvg_logo) = &channel.tvg_logo {
            body.push_str(&format!(r#" tvg-logo="{}""#, tvg_logo));
        }
        body.push_str(&format!(
            r#" group-title="{}" source-name="{}""#,
            channel.group, channel.source_name
        ));
        body.push_str(&format!(",{}\n", channel.name));
        body.push_str(&format!("{}/live/{}\n", base, channel.id));
    }

    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-mpegURL; charset=utf-8"),
    );
    response
}

async fn live(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response<Body>, AppError> {
    let channel = {
        let runtime = state.runtime.read().await;
        runtime.by_id.get(&id).cloned()
    }
    .ok_or_else(|| AppError::not_found("channel not found"))?;

    proxy_upstream(&state, &id, &channel.upstream_url, &headers, &uri).await
}

async fn proxy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ProxyQuery>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response<Body>, AppError> {
    let config = state.config.read().await.clone();
    verify_signature(&config.signing_secret, &query.u, &query.s)?;
    proxy_upstream(&state, &id, &query.u, &headers, &uri).await
}

async fn proxy_upstream(
    state: &AppState,
    channel_id: &str,
    target_url: &str,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<Response<Body>, AppError> {
    let upstream = state
        .client
        .get(target_url)
        .send()
        .await
        .map_err(AppError::bad_gateway)?;
    let upstream = upstream.error_for_status().map_err(AppError::bad_gateway)?;

    let final_url = upstream.url().clone();
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if looks_like_m3u(content_type, final_url.path()) {
        let text = upstream.text().await.map_err(AppError::bad_gateway)?;
        let config = state.config.read().await.clone();
        let rewritten = rewrite_playlist(
            &config,
            channel_id,
            &public_base_url(state, headers, uri).await,
            &final_url,
            &text,
        )?;
        let mut response = Response::new(Body::from(rewritten));
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-mpegURL; charset=utf-8"),
        );
        return Ok(response);
    }

    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .cloned();
    let mut response = Response::new(Body::from_stream(
        upstream
            .bytes_stream()
            .map_err(|err| std::io::Error::other(err.to_string())),
    ));
    if let Some(value) = content_type {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_TYPE, value);
    }
    Ok(response)
}

async fn admin_page(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Html<String>, AppError> {
    let config = state.config.read().await.clone();
    let status = query.get("status").map(String::as_str).unwrap_or("");
    let (status_message, status_class) = match status {
        "saved" => ("保存成功，已自动刷新频道列表。", "ok"),
        "error" => ("保存失败，请检查输入内容后重试。", "error"),
        _ => ("", ""),
    };

    let data = AdminPageData {
        public_base_url: config.public_base_url.unwrap_or_default(),
        refresh_minutes: config
            .refresh_minutes
            .map(|v| v.to_string())
            .unwrap_or_else(|| "30".to_string()),
        user_agent: config
            .user_agent
            .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string()),
        signing_secret: config.signing_secret,
        sources: if config.sources.is_empty() {
            vec![AdminSourceView {
                name: "source-1".to_string(),
                url: String::new(),
                enabled: true,
            }]
        } else {
            config
                .sources
                .into_iter()
                .map(|source| AdminSourceView {
                    name: source.name,
                    url: source.url,
                    enabled: source.enabled != Some(false),
                })
                .collect()
        },
        status_message: status_message.to_string(),
        status_class: status_class.to_string(),
    };

    let html = render_admin_page(data)?;
    Ok(Html(html))
}

async fn save_admin(
    State(state): State<AppState>,
    Form(form): Form<AdminForm>,
) -> Result<Redirect, AppError> {
    let source_names = form.source_name.unwrap_or_default();
    let source_urls = form.source_url.unwrap_or_default();
    let enabled_flags = form.source_enabled.unwrap_or_default();

    let mut sources = Vec::new();
    let row_count = source_names.len().max(source_urls.len());
    for idx in 0..row_count {
        let name = source_names
            .get(idx)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let url = source_urls
            .get(idx)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();

        if name.is_empty() && url.is_empty() {
            continue;
        }
        if url.is_empty() {
            return Err(AppError::bad_request("有订阅源缺少地址"));
        }

        let enabled = enabled_flags.iter().any(|value| value == &idx.to_string());
        sources.push(SourceConfig {
            name: if name.is_empty() {
                format!("source-{}", idx + 1)
            } else {
                name
            },
            url,
            enabled: Some(enabled),
        });
    }

    let refresh_minutes = form
        .refresh_minutes
        .as_deref()
        .unwrap_or("30")
        .trim()
        .parse::<u64>()
        .map_err(|_| AppError::bad_request("刷新间隔必须是数字"))?;

    let signing_secret = form.signing_secret.unwrap_or_default().trim().to_string();
    if signing_secret.is_empty() {
        return Err(AppError::bad_request("签名密钥不能为空"));
    }

    let new_config = Config {
        bind: Some("0.0.0.0:8787".to_string()),
        public_base_url: clean_optional(form.public_base_url),
        refresh_minutes: Some(refresh_minutes.max(1)),
        user_agent: clean_optional(form.user_agent),
        signing_secret,
        sources,
    };

    let yaml = serde_yaml::to_string(&new_config).map_err(AppError::internal)?;
    tokio::fs::write(&*state.config_path, yaml)
        .await
        .map_err(AppError::internal)?;

    {
        let mut config = state.config.write().await;
        *config = new_config;
    }

    refresh_channels(&state).await;
    Ok(Redirect::to("/admin?status=saved"))
}

fn clean_optional(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn looks_like_m3u(content_type: &str, path: &str) -> bool {
    content_type.contains("mpegurl")
        || content_type.contains("vnd.apple.mpegurl")
        || path.ends_with(".m3u8")
}

fn rewrite_playlist(
    config: &Config,
    channel_id: &str,
    base: &str,
    origin_url: &Url,
    body: &str,
) -> Result<String, AppError> {
    let mut output = String::new();

    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.starts_with("#EXT-X-KEY:")
            || line.starts_with("#EXT-X-MEDIA:")
            || line.starts_with("#EXT-X-STREAM-INF:")
        {
            output.push_str(&rewrite_tag_uris(
                config, channel_id, base, origin_url, raw_line,
            )?);
            output.push('\n');
            continue;
        }

        if line.is_empty() || line.starts_with('#') {
            output.push_str(raw_line);
            output.push('\n');
            continue;
        }

        let absolute = origin_url.join(line).map_err(AppError::bad_gateway)?;
        output.push_str(&signed_proxy_url(
            config,
            channel_id,
            base,
            absolute.as_str(),
        ));
        output.push('\n');
    }

    Ok(output)
}

fn rewrite_tag_uris(
    config: &Config,
    channel_id: &str,
    base: &str,
    origin_url: &Url,
    line: &str,
) -> Result<String, AppError> {
    if let Some(start) = line.find("URI=\"") {
        let value_start = start + 5;
        if let Some(end_rel) = line[value_start..].find('"') {
            let value_end = value_start + end_rel;
            let old = &line[value_start..value_end];
            let absolute = origin_url.join(old).map_err(AppError::bad_gateway)?;
            let replacement = signed_proxy_url(config, channel_id, base, absolute.as_str());
            let mut new_line = String::new();
            new_line.push_str(&line[..value_start]);
            new_line.push_str(&replacement);
            new_line.push_str(&line[value_end..]);
            return Ok(new_line);
        }
    }
    Ok(line.to_string())
}

fn signed_proxy_url(config: &Config, channel_id: &str, base: &str, target: &str) -> String {
    let signature = sign_target(&config.signing_secret, target);
    format!(
        "{}/proxy/{}?u={}&s={}",
        base,
        channel_id,
        urlencoding::encode(target),
        signature
    )
}

fn sign_target(secret: &str, target: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("valid hmac key");
    mac.update(target.as_bytes());
    let digest = mac.finalize().into_bytes();
    URL_SAFE_NO_PAD.encode(digest)
}

fn verify_signature(secret: &str, target: &str, signature: &str) -> Result<(), AppError> {
    let expected = sign_target(secret, target);
    if expected == signature {
        Ok(())
    } else {
        Err(AppError::forbidden("bad signature"))
    }
}

async fn public_base_url(state: &AppState, headers: &HeaderMap, uri: &Uri) -> String {
    let config = state.config.read().await;
    if let Some(base) = &config.public_base_url {
        return base.trim_end_matches('/').to_string();
    }

    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|v| v.to_str().ok())
        .unwrap_or("127.0.0.1:8787");
    let proto = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_else(|| {
            if uri.scheme_str() == Some("https") {
                "https"
            } else {
                "http"
            }
        });
    format!("{proto}://{}", host.trim_end_matches('/'))
}

fn render_admin_page(data: AdminPageData) -> Result<String, AppError> {
    let sources_json = serde_json::to_string(&data.sources).map_err(AppError::internal)?;
    let status_block = if data.status_message.is_empty() {
        String::new()
    } else {
        format!(
            r#"<div class="notice {class}">{message}</div>"#,
            class = escape_html(&data.status_class),
            message = escape_html(&data.status_message)
        )
    };

    Ok(format!(
        r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>home-iptv-proxy 后台</title>
  <style>
    :root {{
      --bg: #f5efe4;
      --panel: #fffdf8;
      --line: #d9ccb7;
      --ink: #2b241c;
      --muted: #746759;
      --accent: #c76b32;
      --accent-dark: #9d4f20;
      --ok: #e6f5ea;
      --ok-line: #94c7a2;
      --error: #fdebea;
      --error-line: #dd8f89;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "PingFang SC", "Noto Sans SC", "Microsoft YaHei", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #fff7ed 0, transparent 35%),
        linear-gradient(180deg, #f9f3e8 0%, var(--bg) 100%);
    }}
    .shell {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 18px 56px;
    }}
    .hero {{
      display: grid;
      gap: 10px;
      margin-bottom: 22px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(28px, 5vw, 42px);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }}
    .sub {{
      color: var(--muted);
      font-size: 15px;
    }}
    .panel {{
      background: rgba(255, 253, 248, 0.92);
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: 0 18px 60px rgba(79, 51, 24, 0.09);
      overflow: hidden;
    }}
    .panel-head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 20px 22px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(135deg, rgba(199, 107, 50, 0.12), rgba(255,255,255,0));
    }}
    .panel-title {{
      font-size: 18px;
      font-weight: 700;
    }}
    .panel-body {{
      padding: 22px;
    }}
    .grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-bottom: 18px;
    }}
    .field {{
      display: grid;
      gap: 8px;
    }}
    .field.full {{
      grid-column: 1 / -1;
    }}
    label {{
      font-size: 13px;
      color: var(--muted);
      font-weight: 700;
      letter-spacing: 0.02em;
    }}
    input {{
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 14px;
      padding: 12px 14px;
      font-size: 15px;
      color: var(--ink);
      outline: none;
    }}
    input:focus {{
      border-color: var(--accent);
      box-shadow: 0 0 0 4px rgba(199, 107, 50, 0.12);
    }}
    .sources {{
      display: grid;
      gap: 14px;
      margin-top: 10px;
    }}
    .source-card {{
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 18px;
      padding: 16px;
      display: grid;
      gap: 12px;
    }}
    .source-top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }}
    .source-badge {{
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .source-grid {{
      display: grid;
      gap: 12px;
      grid-template-columns: 220px 1fr;
    }}
    .toggle {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 14px;
      color: var(--ink);
    }}
    .toggle input {{
      width: 18px;
      height: 18px;
      margin: 0;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 22px;
    }}
    button {{
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: transform .15s ease, opacity .15s ease, background .15s ease;
    }}
    button:hover {{ transform: translateY(-1px); }}
    .primary {{
      background: var(--accent);
      color: #fff;
    }}
    .primary:hover {{
      background: var(--accent-dark);
    }}
    .ghost {{
      background: #efe3d1;
      color: var(--ink);
    }}
    .danger {{
      background: #f7dfdc;
      color: #7e3029;
    }}
    .notice {{
      margin-bottom: 18px;
      padding: 14px 16px;
      border-radius: 16px;
      font-size: 14px;
      font-weight: 600;
    }}
    .notice.ok {{
      background: var(--ok);
      border: 1px solid var(--ok-line);
    }}
    .notice.error {{
      background: var(--error);
      border: 1px solid var(--error-line);
    }}
    .footer-note {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 760px) {{
      .grid, .source-grid {{
        grid-template-columns: 1fr;
      }}
      .panel-head {{
        align-items: flex-start;
        flex-direction: column;
      }}
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="hero">
      <h1>订阅中转后台</h1>
      <div class="sub">在这里维护上游 m3u 地址，保存后会自动刷新，本地订阅地址保持不变。</div>
    </div>
    <div class="panel">
      <div class="panel-head">
        <div>
          <div class="panel-title">源地址管理</div>
          <div class="sub">本地播放地址：<code>/list.m3u</code></div>
        </div>
      </div>
      <div class="panel-body">
        {status_block}
        <form method="post" action="/admin/save">
          <div class="grid">
            <div class="field">
              <label for="public_base_url">外部访问地址（可选）</label>
              <input id="public_base_url" name="public_base_url" value="{public_base_url}" placeholder="例如 https://tv.example.com">
            </div>
            <div class="field">
              <label for="refresh_minutes">刷新间隔（分钟）</label>
              <input id="refresh_minutes" name="refresh_minutes" value="{refresh_minutes}" inputmode="numeric">
            </div>
            <div class="field">
              <label for="user_agent">请求标识</label>
              <input id="user_agent" name="user_agent" value="{user_agent}">
            </div>
            <div class="field">
              <label for="signing_secret">签名密钥</label>
              <input id="signing_secret" name="signing_secret" value="{signing_secret}">
            </div>
          </div>

          <div class="panel-title">M3U 源列表</div>
          <div class="sources" id="sources"></div>

          <div class="actions">
            <button class="ghost" type="button" id="add-source">新增一条源地址</button>
            <button class="primary" type="submit">保存并刷新</button>
          </div>
        </form>
        <div class="footer-note">保存后会直接写入服务器配置文件，并重新抓取频道列表。</div>
      </div>
    </div>
  </div>

  <template id="source-template">
    <div class="source-card">
      <div class="source-top">
        <div class="source-badge">Source</div>
        <button class="danger remove-source" type="button">删除</button>
      </div>
      <div class="source-grid">
        <div class="field">
          <label>名称</label>
          <input data-role="name" name="source_name" placeholder="例如 客厅主源">
        </div>
        <div class="field">
          <label>m3u 地址</label>
          <input data-role="url" name="source_url" placeholder="https://example.com/live.m3u">
        </div>
      </div>
      <label class="toggle">
        <input data-role="enabled" type="checkbox" name="source_enabled">
        启用这条源
      </label>
    </div>
  </template>

  <script>
    const initialSources = {sources_json};
    const container = document.getElementById("sources");
    const template = document.getElementById("source-template");
    const addButton = document.getElementById("add-source");

    function refreshIndexes() {{
      [...container.querySelectorAll(".source-card")].forEach((card, index) => {{
        card.querySelector(".source-badge").textContent = "Source " + (index + 1);
        card.querySelector('[data-role="enabled"]').value = String(index);
      }});
    }}

    function addSourceRow(source = {{ name: "", url: "", enabled: true }}) {{
      const node = template.content.firstElementChild.cloneNode(true);
      node.querySelector('[data-role="name"]').value = source.name || "";
      node.querySelector('[data-role="url"]').value = source.url || "";
      node.querySelector('[data-role="enabled"]').checked = source.enabled !== false;
      node.querySelector(".remove-source").addEventListener("click", () => {{
        node.remove();
        if (!container.children.length) {{
          addSourceRow();
        }}
        refreshIndexes();
      }});
      container.appendChild(node);
      refreshIndexes();
    }}

    if (initialSources.length) {{
      initialSources.forEach(addSourceRow);
    }} else {{
      addSourceRow();
    }}

    addButton.addEventListener("click", () => addSourceRow());
  </script>
</body>
</html>"#,
        status_block = status_block,
        public_base_url = escape_html(&data.public_base_url),
        refresh_minutes = escape_html(&data.refresh_minutes),
        user_agent = escape_html(&data.user_agent),
        signing_secret = escape_html(&data.signing_secret),
        sources_json = sources_json
    ))
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.to_string(),
        }
    }

    fn forbidden(message: &str) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.to_string(),
        }
    }

    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.to_string(),
        }
    }

    fn bad_gateway(err: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: err.to_string(),
        }
    }

    fn internal(err: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}
