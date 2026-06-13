use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
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
    config: Arc<Config>,
    runtime: Arc<RwLock<RuntimeState>>,
}

#[derive(Debug, Deserialize)]
struct Config {
    bind: Option<String>,
    public_base_url: Option<String>,
    refresh_minutes: Option<u64>,
    user_agent: Option<String>,
    signing_secret: String,
    sources: Vec<SourceConfig>,
}

#[derive(Debug, Deserialize)]
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
        config: Arc::new(config),
        runtime: Arc::new(RwLock::new(RuntimeState::default())),
    };

    refresh_channels(&state).await;
    spawn_refresh_loop(state.clone());

    let app = Router::new()
        .route("/health", get(health))
        .route("/channels", get(channels))
        .route("/list.m3u", get(list_m3u))
        .route("/live/{id}", get(live))
        .route("/proxy/{id}", get(proxy))
        .with_state(state.clone());

    let bind = state
        .config
        .bind
        .clone()
        .unwrap_or_else(|| "0.0.0.0:8787".to_string());
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
    let minutes = state.config.refresh_minutes.unwrap_or(30).max(1);
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(minutes * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            refresh_channels(&state).await;
        }
    });
}

async fn refresh_channels(state: &AppState) {
    let mut channels = Vec::new();
    let mut by_id = HashMap::new();
    let mut errors = BTreeMap::new();
    let mut seen_keys = HashSet::new();

    for source in &state.config.sources {
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
    let base = public_base_url(&state.config, &headers, &uri);

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
    verify_signature(&state.config.signing_secret, &query.u, &query.s)?;
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
        let rewritten = rewrite_playlist(
            &state.config,
            channel_id,
            &public_base_url(&state.config, headers, uri),
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

fn public_base_url(config: &Config, headers: &HeaderMap, uri: &Uri) -> String {
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

    fn bad_gateway(err: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.message).into_response()
    }
}
