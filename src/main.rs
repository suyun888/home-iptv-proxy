use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Form, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, header},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use chrono::{Local, TimeZone, Utc};
use futures_util::TryStreamExt;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
    time,
};
use url::Url;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
struct AppState {
    path: Arc<PathBuf>,
    password: Arc<String>,
    config: Arc<RwLock<Config>>,
    runtime: Arc<RwLock<Runtime>>,
    refresh_lock: Arc<Mutex<()>>,
    client: Client,
}

#[derive(Clone, Serialize, Deserialize)]
struct Config {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default)]
    public_base_url: Option<String>,
    #[serde(default = "default_refresh")]
    refresh_minutes: u64,
    signing_secret: String,
    #[serde(default)]
    sources: Vec<Source>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Source {
    id: String,
    name: String,
    url: String,
    mode: Mode,
    enabled: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Direct,
    Proxy,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Direct => "直连",
            Self::Proxy => "中转",
        }
    }
}

#[derive(Clone)]
struct Channel {
    id: String,
    source_id: String,
    source_name: String,
    mode: Mode,
    extinf: String,
    url: String,
    catchup: Option<String>,
}

#[derive(Default)]
struct Runtime {
    channels: Vec<Channel>,
    by_id: HashMap<String, Channel>,
    source_status: HashMap<String, SourceStatus>,
    epg_by_source: HashMap<String, Vec<String>>,
    epg_urls: Vec<String>,
}

#[derive(Clone, Default)]
struct SourceStatus {
    count: usize,
    error: bool,
}

#[derive(Deserialize)]
struct SourceForm {
    id: Option<String>,
    name: String,
    url: String,
    mode: Mode,
    #[serde(default)]
    enabled: bool,
}

#[derive(Deserialize)]
struct IdForm {
    id: String,
}
#[derive(Deserialize)]
struct ProxyQuery {
    u: String,
    s: String,
}
#[derive(Deserialize)]
struct ReplayQuery {
    start: i64,
    end: i64,
}

#[derive(Debug)]
struct AppError(StatusCode, &'static str);
impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let mut response = (self.0, self.1).into_response();
        if self.0 == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"Source Management\""),
            );
        }
        response
    }
}

fn default_bind() -> String {
    "0.0.0.0:28788".into()
}
fn default_refresh() -> u64 {
    30
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path =
        PathBuf::from(env::var("IPTV_CONFIG").unwrap_or_else(|_| "config/sources.yaml".into()));
    let password = env::var("IPTV_ADMIN_PASSWORD")
        .map_err(|_| anyhow::anyhow!("IPTV_ADMIN_PASSWORD is required"))?;
    anyhow::ensure!(
        !password.is_empty(),
        "IPTV_ADMIN_PASSWORD must not be empty"
    );
    let config: Config = serde_yaml::from_str(&tokio::fs::read_to_string(&path).await?)?;
    anyhow::ensure!(
        config.signing_secret.len() >= 32,
        "signing_secret must have at least 32 characters"
    );
    let bind: SocketAddr = config.bind.parse()?;
    let state = AppState {
        path: Arc::new(path),
        password: Arc::new(password),
        config: Arc::new(RwLock::new(config)),
        runtime: Arc::new(RwLock::new(Runtime::default())),
        refresh_lock: Arc::new(Mutex::new(())),
        client: Client::builder()
            .connect_timeout(Duration::from_secs(8))
            .redirect(reqwest::redirect::Policy::limited(8))
            .user_agent("home-iptv-proxy/1.0")
            .build()?,
    };
    refresh(&state).await;
    tokio::spawn(refresh_loop(state.clone()));
    let app = Router::new()
        .route("/", get(|| async { Redirect::to("/admin") }))
        .route("/health", get(health))
        .route("/list.m3u", get(list_m3u))
        .route("/live/{id}", get(live))
        .route("/catchup/{id}", get(catchup))
        .route("/proxy/{id}", get(proxy))
        .route("/admin", get(admin))
        .route("/admin/sources", post(save_source))
        .route("/admin/sources/delete", post(delete_source))
        .route("/admin/refresh", post(refresh_now))
        .with_state(state);
    axum::serve(TcpListener::bind(bind).await?, app).await?;
    Ok(())
}

async fn refresh_loop(state: AppState) {
    loop {
        let minutes = state.config.read().await.refresh_minutes.max(1);
        time::sleep(Duration::from_secs(minutes * 60)).await;
        refresh(&state).await;
    }
}

async fn refresh(state: &AppState) {
    let _guard = state.refresh_lock.lock().await;
    let sources = state.config.read().await.sources.clone();
    let previous = state.runtime.read().await;
    let previous_channels = previous.channels.clone();
    let previous_epg = previous.epg_by_source.clone();
    drop(previous);
    let mut runtime = Runtime::default();
    for source in sources.into_iter().filter(|source| source.enabled) {
        match fetch_source(state, &source).await {
            Ok((channels, epg_urls)) => {
                runtime.epg_by_source.insert(source.id.clone(), epg_urls);
                runtime.source_status.insert(
                    source.id,
                    SourceStatus {
                        count: channels.len(),
                        error: false,
                    },
                );
                runtime.channels.extend(channels);
            }
            Err(_) => {
                let stale: Vec<_> = previous_channels
                    .iter()
                    .filter(|channel| channel.source_id == source.id)
                    .cloned()
                    .collect();
                if let Some(epg) = previous_epg.get(&source.id) {
                    runtime.epg_by_source.insert(source.id.clone(), epg.clone());
                }
                runtime.source_status.insert(
                    source.id,
                    SourceStatus {
                        count: stale.len(),
                        error: true,
                    },
                );
                runtime.channels.extend(stale);
            }
        }
    }
    runtime.by_id = runtime
        .channels
        .iter()
        .map(|channel| (channel.id.clone(), channel.clone()))
        .collect();
    runtime.epg_urls = runtime.epg_by_source.values().flatten().cloned().collect();
    runtime.epg_urls.sort();
    runtime.epg_urls.dedup();
    *state.runtime.write().await = runtime;
}

async fn fetch_source(
    state: &AppState,
    source: &Source,
) -> anyhow::Result<(Vec<Channel>, Vec<String>)> {
    let base = Url::parse(&source.url)?;
    anyhow::ensure!(
        matches!(base.scheme(), "http" | "https"),
        "invalid source URL"
    );
    let response = state
        .client
        .get(base.clone())
        .timeout(Duration::from_secs(25))
        .send()
        .await?
        .error_for_status()?;
    anyhow::ensure!(
        response.content_length().unwrap_or(0) <= 10_000_000,
        "playlist too large"
    );
    let body = response.bytes().await?;
    anyhow::ensure!(body.len() <= 10_000_000, "playlist too large");
    let text = String::from_utf8(body.to_vec())?;
    Ok(parse_source(&text, &base, source))
}

fn parse_source(text: &str, base: &Url, source: &Source) -> (Vec<Channel>, Vec<String>) {
    let mut channels = Vec::new();
    let mut epg_urls = Vec::new();
    let mut info: Option<String> = None;
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with("#EXTM3U") {
            for key in ["x-tvg-url", "url-tvg"] {
                if let Some(value) = attribute(line, key) {
                    epg_urls.extend(
                        value
                            .split(',')
                            .filter(|part| !part.is_empty())
                            .map(str::to_string),
                    );
                }
            }
        } else if line.starts_with("#EXTINF:") {
            info = Some(line.to_string());
        } else if !line.is_empty() && !line.starts_with('#') {
            let Some(extinf) = info.take() else { continue };
            let Ok(url) = base.join(line) else { continue };
            if !matches!(url.scheme(), "http" | "https") {
                continue;
            }
            let id = hex_id(&format!("{}\n{}\n{}", source.id, extinf, url));
            channels.push(Channel {
                id,
                source_id: source.id.clone(),
                source_name: source.name.clone(),
                mode: source.mode,
                catchup: attribute(&extinf, "catchup-source"),
                extinf,
                url: url.to_string(),
            });
        }
    }
    (channels, epg_urls)
}

fn hex_id(input: &str) -> String {
    Sha256::digest(input.as_bytes())[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn attribute(line: &str, name: &str) -> Option<String> {
    let prefix = format!("{name}=\"");
    let start = line.find(&prefix)? + prefix.len();
    let end = line[start..].find('"')? + start;
    Some(line[start..end].to_string())
}

fn set_attribute(line: &str, name: &str, value: &str) -> String {
    let Some((before, channel_name)) = line.split_once(',') else {
        return line.into();
    };
    let prefix = format!("{name}=\"");
    if let Some(start) = before.find(&prefix) {
        let value_start = start + prefix.len();
        if let Some(end) = before[value_start..].find('"') {
            let value_end = value_start + end;
            return format!(
                "{}{}{},{}",
                &before[..value_start],
                value,
                &before[value_end..],
                channel_name
            );
        }
    }
    format!("{before} {name}=\"{value}\",{channel_name}")
}

async fn base_url(state: &AppState, headers: &HeaderMap) -> String {
    if let Some(base) = &state.config.read().await.public_base_url {
        return base.trim_end_matches('/').to_string();
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("127.0.0.1:8787");
    format!("http://{host}")
}

async fn list_m3u(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    let base = base_url(&state, &headers).await;
    let runtime = state.runtime.read().await;
    let mut body = String::from("#EXTM3U");
    if !runtime.epg_urls.is_empty() {
        body.push_str(&format!(" x-tvg-url=\"{}\"", runtime.epg_urls.join(",")));
    }
    body.push('\n');
    for channel in &runtime.channels {
        let mut info = channel.extinf.clone();
        if channel.mode == Mode::Proxy && channel.catchup.is_some() {
            let replay_url = format!(
                "{base}/catchup/{}?start=${{(b)timestamp}}&end=${{(e)timestamp}}",
                channel.id
            );
            info = set_attribute(&info, "catchup", "default");
            info = set_attribute(&info, "catchup-source", &replay_url);
        }
        info = set_attribute(&info, "source-name", &channel.source_name);
        info = set_attribute(
            &info,
            "access-mode",
            if channel.mode == Mode::Direct {
                "direct"
            } else {
                "proxy"
            },
        );
        body.push_str(&info);
        body.push('\n');
        if channel.mode == Mode::Direct {
            body.push_str(&channel.url);
        } else {
            body.push_str(&format!("{base}/live/{}", channel.id));
        }
        body.push('\n');
    }
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-mpegURL; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    axum::Json(serde_json::json!({
        "ok": true,
        "channels": runtime.channels.len(),
        "sources": runtime.source_status.len()
    }))
}

async fn lookup(state: &AppState, id: &str) -> Result<Channel, AppError> {
    let channel = state
        .runtime
        .read()
        .await
        .by_id
        .get(id)
        .cloned()
        .ok_or(AppError(StatusCode::NOT_FOUND, "channel unavailable"))?;
    if channel.mode != Mode::Proxy {
        return Err(AppError(StatusCode::NOT_FOUND, "channel unavailable"));
    }
    Ok(channel)
}

async fn live(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    let channel = lookup(&state, &id).await?;
    relay(&state, &channel, &channel.url, &headers).await
}

async fn catchup(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ReplayQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    if query.start <= 0 || query.end <= query.start {
        return Err(AppError(StatusCode::BAD_REQUEST, "invalid replay window"));
    }
    let channel = lookup(&state, &id).await?;
    let template = channel
        .catchup
        .as_deref()
        .ok_or(AppError(StatusCode::NOT_FOUND, "replay unavailable"))?;
    let target = expand_catchup(template, query.start, query.end)?;
    relay(&state, &channel, &target, &headers).await
}

fn expand_catchup(template: &str, start: i64, end: i64) -> Result<String, AppError> {
    let mut result = template.to_string();
    for (marker, timestamp) in [("(b)", start), ("(e)", end)] {
        let utc = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .ok_or(AppError(StatusCode::BAD_REQUEST, "invalid replay time"))?;
        let local = utc.with_timezone(&Local);
        result = result
            .replace(&format!("${{{marker}timestamp}}"), &timestamp.to_string())
            .replace(
                &format!("${{{marker}yyyyMMddHHmmss:utc}}"),
                &utc.format("%Y%m%d%H%M%S").to_string(),
            )
            .replace(
                &format!("${{{marker}yyyyMMddHHmmss}}"),
                &local.format("%Y%m%d%H%M%S").to_string(),
            );
    }
    let start_utc = Utc
        .timestamp_opt(start, 0)
        .single()
        .ok_or(AppError(StatusCode::BAD_REQUEST, "invalid replay time"))?;
    let end_utc = Utc
        .timestamp_opt(end, 0)
        .single()
        .ok_or(AppError(StatusCode::BAD_REQUEST, "invalid replay time"))?;
    result = result
        .replace(
            "{utc:YmdHMS}",
            &start_utc.format("%Y%m%d%H%M%S").to_string(),
        )
        .replace(
            "{utcend:YmdHMS}",
            &end_utc.format("%Y%m%d%H%M%S").to_string(),
        );
    if result.contains("${") || result.contains("{utc:") || result.contains("{utcend:") {
        return Err(AppError(
            StatusCode::BAD_REQUEST,
            "unsupported replay template",
        ));
    }
    let url =
        Url::parse(&result).map_err(|_| AppError(StatusCode::BAD_REQUEST, "invalid replay URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError(StatusCode::BAD_REQUEST, "invalid replay URL"));
    }
    Ok(result)
}

async fn proxy(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ProxyQuery>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    let channel = lookup(&state, &id).await?;
    let secret = state.config.read().await.signing_secret.clone();
    if sign(&secret, &id, &query.u) != query.s {
        return Err(AppError(StatusCode::FORBIDDEN, "invalid signature"));
    }
    relay(&state, &channel, &query.u, &headers).await
}

async fn relay(
    state: &AppState,
    channel: &Channel,
    target: &str,
    headers: &HeaderMap,
) -> Result<Response<Body>, AppError> {
    let url =
        Url::parse(target).map_err(|_| AppError(StatusCode::BAD_REQUEST, "invalid media URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError(StatusCode::BAD_REQUEST, "invalid media URL"));
    }
    let mut request = state.client.get(url);
    for name in [
        header::RANGE,
        header::IF_RANGE,
        header::ACCEPT,
        header::REFERER,
    ] {
        if let Some(value) = headers.get(&name) {
            request = request.header(name, value);
        }
    }
    let upstream = request
        .send()
        .await
        .map_err(|_| AppError(StatusCode::BAD_GATEWAY, "upstream unavailable"))?;
    let status = upstream.status();
    if !status.is_success() {
        return Err(AppError(
            StatusCode::BAD_GATEWAY,
            "upstream rejected request",
        ));
    }
    let final_url = upstream.url().clone();
    let content_type = upstream
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if content_type.contains("mpegurl") || final_url.path().to_ascii_lowercase().ends_with(".m3u8")
    {
        let text = upstream
            .text()
            .await
            .map_err(|_| AppError(StatusCode::BAD_GATEWAY, "invalid upstream playlist"))?;
        let base = base_url(state, headers).await;
        let secret = state.config.read().await.signing_secret.clone();
        let rewritten = rewrite_hls(&text, &final_url, &base, &secret, &channel.id)?;
        let mut response = Response::new(Body::from(rewritten));
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.apple.mpegurl"),
        );
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        return Ok(response);
    }
    let upstream_headers = upstream.headers().clone();
    let mut response = Response::new(Body::from_stream(
        upstream
            .bytes_stream()
            .map_err(|_| std::io::Error::other("upstream stream failed")),
    ));
    *response.status_mut() = status;
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_LENGTH,
        header::CONTENT_RANGE,
        header::ACCEPT_RANGES,
        header::CACHE_CONTROL,
    ] {
        if let Some(value) = upstream_headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    Ok(response)
}

fn rewrite_hls(
    text: &str,
    origin: &Url,
    base: &str,
    secret: &str,
    id: &str,
) -> Result<String, AppError> {
    let mut output = String::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.starts_with('#') {
            let mut rewritten = raw.to_string();
            if let Some(start) = raw.find("URI=\"") {
                let value_start = start + 5;
                if let Some(end) = raw[value_start..].find('"') {
                    let value_end = value_start + end;
                    let absolute = origin.join(&raw[value_start..value_end]).map_err(|_| {
                        AppError(StatusCode::BAD_GATEWAY, "invalid upstream playlist")
                    })?;
                    rewritten = format!(
                        "{}{}{}",
                        &raw[..value_start],
                        proxy_url(base, secret, id, absolute.as_str()),
                        &raw[value_end..]
                    );
                }
            }
            output.push_str(&rewritten);
        } else if !line.is_empty() {
            let absolute = origin
                .join(line)
                .map_err(|_| AppError(StatusCode::BAD_GATEWAY, "invalid upstream playlist"))?;
            output.push_str(&proxy_url(base, secret, id, absolute.as_str()));
        }
        output.push('\n');
    }
    Ok(output)
}

fn proxy_url(base: &str, secret: &str, id: &str, target: &str) -> String {
    format!(
        "{base}/proxy/{id}?u={}&s={}",
        urlencoding::encode(target),
        sign(secret, id, target)
    )
}

fn sign(secret: &str, id: &str, target: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("valid HMAC key");
    mac.update(id.as_bytes());
    mac.update(b"\n");
    mac.update(target.as_bytes());
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), AppError> {
    let valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Basic "))
        .and_then(|value| STANDARD.decode(value).ok())
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .is_some_and(|pair| pair == format!("admin:{}", state.password));
    if valid {
        return Ok(());
    }
    Err(AppError(
        StatusCode::UNAUTHORIZED,
        "Authentication required",
    ))
}

async fn admin(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, AppError> {
    require_admin(&state, &headers)?;
    let sources = state.config.read().await.sources.clone();
    let statuses = state.runtime.read().await.source_status.clone();
    let mut rows = String::new();
    for source in &sources {
        let status = statuses.get(&source.id).cloned().unwrap_or_default();
        rows.push_str(&format!(
            "<article><form action=\"/admin/sources\" method=\"post\"><input type=\"hidden\" name=\"id\" value=\"{}\"><label>名称<input name=\"name\" required value=\"{}\"></label><label>订阅 URL<input name=\"url\" type=\"url\" required value=\"{}\"></label><label>播放方式<select name=\"mode\"><option value=\"direct\" {}>直连</option><option value=\"proxy\" {}>中转</option></select></label><label class=\"check\"><input type=\"checkbox\" name=\"enabled\" value=\"true\" {}>启用</label><button type=\"submit\">保存</button></form><form action=\"/admin/sources/delete\" method=\"post\" onsubmit=\"return confirm('删除这条源？')\"><input type=\"hidden\" name=\"id\" value=\"{}\"><button class=\"secondary\" type=\"submit\">删除</button></form><p class=\"status\">{} · {} 个频道{} {}</p></article>",
            escape(&source.id), escape(&source.name), escape(&source.url),
            if source.mode == Mode::Direct { "selected" } else { "" },
            if source.mode == Mode::Proxy { "selected" } else { "" },
            if source.enabled { "checked" } else { "" },
            escape(&source.id), source.mode.label(), status.count,
            if source.enabled { "" } else { " · 已停用" },
            if status.error { " · 刷新失败" } else { "" }
        ));
    }
    if rows.is_empty() {
        rows.push_str("<p class=\"empty\">暂无源</p>");
    }
    Ok(Html(
        include_str!("admin.html").replace("{{SOURCE_ROWS}}", &rows),
    ))
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

async fn save_source(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<SourceForm>,
) -> Result<Redirect, AppError> {
    require_admin(&state, &headers)?;
    let name = form.name.trim().to_string();
    let url = form.url.trim().to_string();
    if name.is_empty() || !valid_url(&url) {
        return Err(AppError(StatusCode::BAD_REQUEST, "Invalid source"));
    }
    let id = form.id.filter(|id| !id.is_empty()).unwrap_or_else(|| {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        hex_id(&format!("{name}:{url}:{nonce}"))
    });
    let source = Source {
        id: id.clone(),
        name,
        url,
        mode: form.mode,
        enabled: form.enabled,
    };
    {
        let mut current = state.config.write().await;
        let mut updated = current.clone();
        if let Some(existing) = updated.sources.iter_mut().find(|row| row.id == id) {
            *existing = source;
        } else {
            updated.sources.push(source);
        }
        write_config(&state.path, &updated).await?;
        *current = updated;
    }
    refresh(&state).await;
    Ok(Redirect::to("/admin"))
}

fn valid_url(value: &str) -> bool {
    Url::parse(value).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

async fn delete_source(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<IdForm>,
) -> Result<Redirect, AppError> {
    require_admin(&state, &headers)?;
    {
        let mut current = state.config.write().await;
        let mut updated = current.clone();
        updated.sources.retain(|source| source.id != form.id);
        write_config(&state.path, &updated).await?;
        *current = updated;
    }
    refresh(&state).await;
    Ok(Redirect::to("/admin"))
}

async fn refresh_now(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Redirect, AppError> {
    require_admin(&state, &headers)?;
    refresh(&state).await;
    Ok(Redirect::to("/admin"))
}

async fn write_config(path: &PathBuf, config: &Config) -> Result<(), AppError> {
    let body = serde_yaml::to_string(config)
        .map_err(|_| AppError(StatusCode::INTERNAL_SERVER_ERROR, "Save failed"))?;
    let temporary = path.with_extension("yaml.tmp");
    tokio::fs::write(&temporary, body)
        .await
        .map_err(|_| AppError(StatusCode::INTERNAL_SERVER_ERROR, "Save failed"))?;
    tokio::fs::rename(&temporary, path)
        .await
        .map_err(|_| AppError(StatusCode::INTERNAL_SERVER_ERROR, "Save failed"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aptv_replay_template() {
        let url = "http://gitv/catchup/CCTV1?start=${(b)timestamp}&end=${(e)timestamp}";
        assert_eq!(
            expand_catchup(url, 1700000000, 1700003600).unwrap(),
            "http://gitv/catchup/CCTV1?start=1700000000&end=1700003600"
        );
    }

    #[test]
    fn hls_rewrites_keys_segments_and_variants() {
        let origin = Url::parse("http://host/master/index.m3u8").unwrap();
        let text = "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXT-X-STREAM-INF:BANDWIDTH=1000\nlow/index.m3u8\nseg.ts\n";
        let result = rewrite_hls(text, &origin, "http://local", "secret", "channel").unwrap();
        assert!(result.contains("u=http%3A%2F%2Fhost%2Fmaster%2Fkey.bin"));
        assert!(result.contains("u=http%3A%2F%2Fhost%2Fmaster%2Flow%2Findex.m3u8"));
        assert!(result.contains("u=http%3A%2F%2Fhost%2Fmaster%2Fseg.ts"));
    }

    #[test]
    fn direct_keeps_replay_metadata() {
        let info = "#EXTINF:-1 catchup=\"default\" catchup-source=\"http://origin/replay\",CCTV1";
        assert!(
            set_attribute(info, "access-mode", "direct")
                .contains("catchup-source=\"http://origin/replay\"")
        );
    }
}
