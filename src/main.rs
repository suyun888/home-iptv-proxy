use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Multipart, Path, Query, RawForm, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode, Uri},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, FixedOffset, Local};
use flate2::{Compression, write::GzEncoder};
use futures_util::TryStreamExt;
use hmac::{Hmac, Mac};
use quick_xml::{Reader, events::Event};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::{
    io::AsyncWriteExt,
    net::TcpListener,
    process::Command,
    sync::{Mutex, RwLock},
    time,
};
use tokio_util::io::ReaderStream;
use tracing::{info, warn};
use url::Url;

type HmacSha256 = Hmac<Sha256>;
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const ADMIN_CHANNEL_OPTION_LIMIT: usize = 500;
const ADMIN_CHANNEL_DIAGNOSTIC_LIMIT: usize = 300;

#[derive(Clone)]
struct AppState {
    client: Client,
    config_path: Arc<String>,
    recordings_path: Arc<PathBuf>,
    xhs_env_path: Arc<PathBuf>,
    config: Arc<RwLock<Config>>,
    recordings: Arc<RwLock<Vec<RecordingTask>>>,
    runtime: Arc<RwLock<RuntimeState>>,
    refresh_lock: Arc<Mutex<()>>,
    epg_cache_lock: Arc<Mutex<()>>,
    active_recordings: Arc<Mutex<HashSet<String>>>,
    app_version: Arc<String>,
    app_image: Arc<String>,
    version_check_url: Arc<Option<String>>,
    auto_update_enabled: bool,
    manual_update_command: Arc<Option<String>>,
    xhs_apply_command: Arc<Option<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    bind: Option<String>,
    public_base_url: Option<String>,
    refresh_minutes: Option<u64>,
    user_agent: Option<String>,
    epg_source_url: Option<String>,
    epg_proxy_url: Option<String>,
    epg_cache_minutes: Option<u64>,
    epg_cache_dir: Option<String>,
    recordings_dir: Option<String>,
    xhs_a1: Option<String>,
    xhs_web_session: Option<String>,
    signing_secret: String,
    sources: Vec<SourceConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceConfig {
    name: String,
    url: String,
    proxy_url: Option<String>,
    output_mode: Option<String>,
    enabled: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
struct Channel {
    id: String,
    name: String,
    normalized_name: String,
    group: String,
    tvg_id: Option<String>,
    tvg_logo: Option<String>,
    source_name: String,
    source_slug: String,
    source_proxy_url: Option<String>,
    source_output_mode: String,
    upstream_url: String,
}

#[derive(Default)]
struct RuntimeState {
    channels: Vec<Channel>,
    by_id: HashMap<String, Channel>,
    source_errors: BTreeMap<String, String>,
    source_stats: Vec<SourceRuntimeStat>,
    last_refresh_ok: bool,
}

#[derive(Clone, Debug, Serialize)]
struct SourceRuntimeStat {
    name: String,
    url: String,
    enabled: bool,
    channel_count: usize,
    refresh_latency_ms: Option<u64>,
    last_status: String,
    last_error: String,
    last_refresh_at: String,
    access_mode: String,
    proxy_target: String,
    proxy_region: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RecordingStatus {
    Scheduled,
    Running,
    Completed,
    Failed,
    Missed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecordingTask {
    id: String,
    channel_id: String,
    channel_name: String,
    program_title: String,
    start_at: String,
    end_at: String,
    pre_minutes: i64,
    post_minutes: i64,
    output_name: String,
    enabled: bool,
    status: RecordingStatus,
    output_file: Option<String>,
    last_error: Option<String>,
}

#[derive(Serialize)]
struct RecordingTaskView {
    id: String,
    channel_name: String,
    program_title: String,
    start_at: String,
    end_at: String,
    pre_minutes: i64,
    post_minutes: i64,
    output_name: String,
    enabled: bool,
    status: String,
    output_file: String,
    last_error: String,
}

#[derive(Serialize)]
struct ChannelOptionView {
    id: String,
    name: String,
    label: String,
    tvg_id: String,
    source_name: String,
}

#[derive(Serialize)]
struct EpgProgrammeView {
    title: String,
    start_at: String,
    end_at: String,
}

#[derive(Debug, Deserialize)]
struct ProxyQuery {
    u: String,
    s: String,
}

#[derive(Debug, Deserialize)]
struct EpgProgrammesQuery {
    channel_id: String,
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
    channels: usize,
    last_refresh_ok: bool,
    source_errors: BTreeMap<String, String>,
    epg_cache_ready: bool,
    version: String,
    latest_version: Option<String>,
    update_available: bool,
    auto_update_enabled: bool,
    manual_update_enabled: bool,
}

#[derive(Serialize)]
struct AdminPageData {
    app_version: String,
    latest_version: String,
    update_available: bool,
    update_status: String,
    app_image: String,
    auto_update_status: String,
    manual_update_label: String,
    manual_update_hint: String,
    manual_update_enabled: bool,
    xhs_a1: String,
    xhs_web_session: String,
    xhs_apply_enabled: bool,
    xhs_apply_hint: String,
    public_base_url: String,
    refresh_minutes: String,
    user_agent: String,
    epg_source_url: String,
    epg_proxy_url: String,
    epg_cache_minutes: String,
    epg_cache_dir: String,
    recordings_dir: String,
    signing_secret: String,
    sources: Vec<AdminSourceView>,
    source_statuses_json: String,
    channel_statuses_json: String,
    channels_json: String,
    recordings_json: String,
    status_message: String,
    status_class: String,
}

struct EpgCachePaths {
    raw_path: PathBuf,
    gzip_path: PathBuf,
}

#[derive(Serialize)]
struct AdminSourceView {
    name: String,
    source_kind: String,
    url: String,
    proxy_url: String,
    output_mode: String,
    enabled: bool,
}

#[derive(Serialize)]
struct AdminChannelStatusView {
    id: String,
    name: String,
    normalized_name: String,
    group: String,
    source_name: String,
    source_slug: String,
    upstream_host: String,
    access_mode: String,
    proxy_region: String,
    refresh_latency_ms: u64,
    last_status: String,
}

#[derive(Deserialize)]
struct ToggleSourceForm {
    index: usize,
    enabled: bool,
}

#[derive(Serialize)]
struct ToggleSourceResponse {
    ok: bool,
    index: usize,
    enabled: bool,
    message: String,
}

#[derive(Serialize, Deserialize)]
struct GeoLookupResponse {
    success: bool,
    country: Option<String>,
    region: Option<String>,
    city: Option<String>,
}

struct FetchSourceResult {
    channels: Vec<Channel>,
    latency_ms: u64,
}

#[derive(Deserialize)]
struct CreateRecordingForm {
    channel_id: String,
    channel_name: String,
    program_title: String,
    start_at: String,
    end_at: String,
    pre_minutes: i64,
    post_minutes: i64,
}

#[derive(Deserialize)]
struct DeleteRecordingForm {
    id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = env::var("IPTV_CONFIG").unwrap_or_else(|_| "config/sources.yaml".to_string());
    let app_image = env::var("IPTV_IMAGE_NAME")
        .or_else(|_| env::var("APP_IMAGE_NAME"))
        .unwrap_or_else(|_| "home-iptv-proxy:local".to_string());
    let auto_update_enabled = env_flag("IPTV_AUTO_UPDATE_ENABLED");
    let version_check_url = env::var("IPTV_VERSION_CHECK_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| default_version_check_url(&app_image));
    let manual_update_command = env::var("IPTV_UPDATE_COMMAND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let xhs_apply_command = env::var("IPTV_XHS_APPLY_COMMAND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let config_text = tokio::fs::read_to_string(&config_path).await?;
    let config: Config = serde_yaml::from_str(&config_text)?;
    let recordings_path = recordings_path_from_config(&config_path);
    let xhs_env_path = xhs_env_path_from_config(&config_path);
    let recordings = load_recordings(&recordings_path).await.unwrap_or_default();

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
        recordings_path: Arc::new(recordings_path),
        xhs_env_path: Arc::new(xhs_env_path),
        config: Arc::new(RwLock::new(config)),
        recordings: Arc::new(RwLock::new(normalize_recordings(recordings))),
        runtime: Arc::new(RwLock::new(RuntimeState::default())),
        refresh_lock: Arc::new(Mutex::new(())),
        epg_cache_lock: Arc::new(Mutex::new(())),
        active_recordings: Arc::new(Mutex::new(HashSet::new())),
        app_version: Arc::new(APP_VERSION.to_string()),
        app_image: Arc::new(app_image),
        version_check_url: Arc::new(version_check_url),
        auto_update_enabled,
        manual_update_command: Arc::new(manual_update_command),
        xhs_apply_command: Arc::new(xhs_apply_command),
    };

    sync_xhs_env_file(&state)
        .await
        .map_err(|err| anyhow::anyhow!(err.message.clone()))?;

    spawn_initial_refresh(state.clone());
    spawn_refresh_loop(state.clone());
    spawn_recording_loop(state.clone());

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/channels", get(channels))
        .route("/m3u", get(list_m3u))
        .route("/list.m3u", get(list_m3u))
        .route("/playlist.m3u", get(list_m3u))
        .route("/txt", get(list_txt))
        .route("/epg.xml", get(epg_xml))
        .route("/epg.xml.gz", get(epg_xml_gz))
        .route("/live/{source}/{id}", get(live_with_source))
        .route("/live/{id}", get(live))
        .route("/proxy/{id}", get(proxy))
        .route("/admin/epg/programmes", get(admin_epg_programmes))
        .route("/admin/recordings/create", post(create_recording))
        .route("/admin/recordings/delete", post(delete_recording))
        .route("/admin/xhs/apply", post(apply_xhs_credentials))
        .route("/admin", get(admin_page))
        .route("/admin/save", post(save_admin))
        .route("/admin/sources/toggle", post(toggle_source))
        .route("/admin/refresh", post(refresh_now))
        .route("/admin/upload-source-file", post(upload_source_file))
        .route("/admin/update", post(run_manual_update))
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

fn env_flag(name: &str) -> bool {
    matches!(
        env::var(name)
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

fn recordings_path_from_config(config_path: &str) -> PathBuf {
    let base = PathBuf::from(config_path);
    let dir = base.parent().unwrap_or_else(|| FsPath::new("."));
    dir.join("recordings.json")
}

fn xhs_env_path_from_config(config_path: &str) -> PathBuf {
    let base = PathBuf::from(config_path);
    let dir = base.parent().unwrap_or_else(|| FsPath::new("."));
    dir.join("xhsuhd.env")
}

fn format_now_local() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn parse_proxy_target(proxy_url: Option<&str>) -> String {
    let Some(value) = proxy_url.filter(|value| !value.trim().is_empty()) else {
        return "-".to_string();
    };
    Url::parse(value)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| value.to_string())
}

fn is_local_host(host: &str) -> bool {
    let lower = host.trim().to_ascii_lowercase();
    if matches!(lower.as_str(), "localhost" | "127.0.0.1" | "::1") {
        return true;
    }
    if let Ok(ip) = lower.parse::<std::net::IpAddr>() {
        return match ip {
            std::net::IpAddr::V4(ipv4) => {
                ipv4.is_private() || ipv4.is_loopback() || ipv4.is_link_local()
            }
            std::net::IpAddr::V6(ipv6) => {
                ipv6.is_loopback() || ipv6.is_unique_local() || ipv6.is_unicast_link_local()
            }
        };
    }
    false
}

async fn resolve_proxy_region(client: &Client, proxy_url: Option<&str>) -> String {
    let target = parse_proxy_target(proxy_url);
    if target == "-" {
        return "本地出口".to_string();
    }
    if is_local_host(&target) {
        return "局域网".to_string();
    }

    let lookup_url = format!("https://ipwho.is/{}", target);
    let request = async {
        let response = client
            .get(lookup_url)
            .send()
            .await
            .map_err(AppError::bad_gateway)?
            .error_for_status()
            .map_err(AppError::bad_gateway)?;
        response
            .json::<GeoLookupResponse>()
            .await
            .map_err(AppError::bad_gateway)
    };

    let body = match time::timeout(Duration::from_secs(2), request).await {
        Ok(result) => match result {
            Ok(payload) => payload,
            Err(_) => return "未知".to_string(),
        },
        Err(_) => return "未知".to_string(),
    };

    if !body.success {
        return "未知".to_string();
    }

    let mut parts = Vec::new();
    if let Some(country) = body.country.filter(|value| !value.trim().is_empty()) {
        parts.push(country);
    }
    if let Some(region) = body.region.filter(|value| !value.trim().is_empty()) {
        parts.push(region);
    }
    if let Some(city) = body.city.filter(|value| !value.trim().is_empty()) {
        parts.push(city);
    }
    if parts.is_empty() {
        "未知".to_string()
    } else {
        parts.join(" / ")
    }
}

async fn describe_access_mode(
    client: &Client,
    proxy_url: Option<&str>,
) -> (String, String, String) {
    let target = parse_proxy_target(proxy_url);
    if proxy_url.is_some() && target != "-" {
        let region = resolve_proxy_region(client, proxy_url).await;
        ("代理".to_string(), target, region)
    } else {
        ("直连".to_string(), "-".to_string(), "本地出口".to_string())
    }
}

fn normalize_output_mode(value: Option<&str>) -> &'static str {
    match value.unwrap_or_default().trim() {
        "direct" => "direct",
        _ => "proxy",
    }
}

fn output_mode_label(value: Option<&str>) -> &'static str {
    match normalize_output_mode(value) {
        "direct" => "直连",
        _ => "代理",
    }
}

fn channel_output_url(base: &str, channel: &Channel) -> String {
    if channel.source_output_mode == "direct" {
        channel.upstream_url.clone()
    } else {
        format!("{}/live/{}/{}", base, channel.source_slug, channel.id)
    }
}

fn default_version_check_url(app_image: &str) -> Option<String> {
    let trimmed = app_image.trim();
    let image = trimmed.strip_prefix("ghcr.io/")?;
    let image = image.split('@').next().unwrap_or(image);
    let image = image.split(':').next().unwrap_or(image);
    let (owner, repo) = image.split_once('/')?;
    Some(format!(
        "https://raw.githubusercontent.com/{owner}/{repo}/main/Cargo.toml"
    ))
}

fn source_fetch_candidates(source_url: &str) -> Vec<String> {
    let mut candidates = vec![source_url.to_string()];
    let Ok(url) = Url::parse(source_url) else {
        return candidates;
    };
    if url.host_str() != Some("raw.githubusercontent.com") {
        return candidates;
    }
    let segments = url
        .path_segments()
        .map(|parts| parts.map(str::to_string).collect::<Vec<_>>())
        .unwrap_or_default();
    if segments.len() < 4 {
        return candidates;
    }
    let owner = &segments[0];
    let repo = &segments[1];
    let branch = &segments[2];
    let path = segments[3..].join("/");
    candidates.push(format!(
        "https://cdn.jsdelivr.net/gh/{owner}/{repo}@{branch}/{path}"
    ));
    candidates
}

fn rewrite_loopback_stream_url(source_url: &str, stream_url: &str) -> String {
    let Ok(source) = Url::parse(source_url) else {
        return stream_url.to_string();
    };
    let Ok(mut stream) = Url::parse(stream_url) else {
        return stream_url.to_string();
    };

    let Some(host) = stream.host_str() else {
        return stream_url.to_string();
    };
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return stream_url.to_string();
    }

    if stream.set_scheme(source.scheme()).is_err() {
        return stream_url.to_string();
    }
    if stream.set_host(source.host_str()).is_err() {
        return stream_url.to_string();
    }
    if stream.set_port(source.port()).is_err() {
        return stream_url.to_string();
    }
    stream.to_string()
}

fn parse_version_from_cargo_toml(body: &str) -> Option<String> {
    for line in body.lines() {
        let trimmed = line.trim();
        if let Some(value) = trimmed.strip_prefix("version = \"") {
            return value.strip_suffix('"').map(str::to_string);
        }
    }
    None
}

async fn fetch_latest_version(state: &AppState) -> Result<Option<String>, AppError> {
    let Some(url) = state.version_check_url.as_ref().clone() else {
        return Ok(None);
    };

    let request = async {
        let response = state
            .client
            .get(url)
            .send()
            .await
            .map_err(AppError::bad_gateway)?
            .error_for_status()
            .map_err(AppError::bad_gateway)?;
        response.text().await.map_err(AppError::bad_gateway)
    };
    let body = match time::timeout(Duration::from_secs(2), request).await {
        Ok(result) => result?,
        Err(_) => return Ok(None),
    };

    Ok(parse_version_from_cargo_toml(&body))
}

async fn load_recordings(path: &PathBuf) -> Result<Vec<RecordingTask>, AppError> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(AppError::internal(err)),
    };
    serde_json::from_str(&text).map_err(AppError::internal)
}

fn normalize_recordings(tasks: Vec<RecordingTask>) -> Vec<RecordingTask> {
    tasks
        .into_iter()
        .map(|mut task| {
            if task.status == RecordingStatus::Running {
                task.status = RecordingStatus::Scheduled;
            }
            task
        })
        .collect()
}

async fn save_recordings(state: &AppState) -> Result<(), AppError> {
    let tasks = state.recordings.read().await.clone();
    let text = serde_json::to_string_pretty(&tasks).map_err(AppError::internal)?;
    tokio::fs::write(&*state.recordings_path, text)
        .await
        .map_err(AppError::internal)
}

async fn persist_config(state: &AppState, new_config: Config) -> Result<(), AppError> {
    let yaml = serde_yaml::to_string(&new_config).map_err(AppError::internal)?;
    tokio::fs::write(&*state.config_path, yaml)
        .await
        .map_err(AppError::internal)?;

    let mut config = state.config.write().await;
    *config = new_config;
    Ok(())
}

async fn prune_runtime_source(state: &AppState, source_name: &str) {
    let mut runtime = state.runtime.write().await;
    runtime
        .channels
        .retain(|channel| channel.source_name != source_name);
    runtime
        .by_id
        .retain(|_, channel| channel.source_name != source_name);
    if let Some(stat) = runtime
        .source_stats
        .iter_mut()
        .find(|item| item.name == source_name)
    {
        stat.enabled = false;
        stat.channel_count = 0;
        stat.last_status = "已禁用".to_string();
        stat.last_error.clear();
        stat.last_refresh_at = format_now_local();
    }
    runtime.source_errors.remove(source_name);
    runtime.last_refresh_ok = !runtime.channels.is_empty() || runtime.source_errors.is_empty();
}

fn spawn_channel_refresh(state: AppState) {
    tokio::spawn(async move {
        refresh_channels_serialized(&state).await;
    });
}

fn spawn_refresh_loop(state: AppState) {
    tokio::spawn(async move {
        loop {
            let minutes = {
                let config = state.config.read().await;
                config.refresh_minutes.unwrap_or(30).max(1)
            };
            time::sleep(Duration::from_secs(minutes * 60)).await;
            refresh_channels_serialized(&state).await;
        }
    });
}

fn spawn_initial_refresh(state: AppState) {
    tokio::spawn(async move {
        refresh_channels_serialized(&state).await;
    });
}

fn spawn_recording_loop(state: AppState) {
    tokio::spawn(async move {
        loop {
            if let Err(err) = tick_recordings(state.clone()).await {
                warn!("recording scheduler failed: {}", err.message);
            }
            time::sleep(Duration::from_secs(30)).await;
        }
    });
}

async fn refresh_channels_serialized(state: &AppState) {
    let Ok(_guard) = state.refresh_lock.try_lock() else {
        info!("channel refresh already running; skipping duplicate request");
        return;
    };
    refresh_channels(state).await;
}

async fn refresh_channels(state: &AppState) {
    let config = state.config.read().await.clone();

    let mut channels = Vec::new();
    let mut by_id = HashMap::new();
    let mut errors = BTreeMap::new();
    let mut source_stats = Vec::new();
    let mut seen_keys = HashSet::new();
    let user_agent = config
        .user_agent
        .clone()
        .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string());

    for source in &config.sources {
        let enabled = source.enabled != Some(false);
        let (access_mode, proxy_target, proxy_region) =
            describe_access_mode(&state.client, source.proxy_url.as_deref()).await;
        if !enabled {
            source_stats.push(SourceRuntimeStat {
                name: source.name.clone(),
                url: source.url.clone(),
                enabled: false,
                channel_count: 0,
                refresh_latency_ms: None,
                last_status: "已禁用".to_string(),
                last_error: String::new(),
                last_refresh_at: format_now_local(),
                access_mode,
                proxy_target,
                proxy_region,
            });
            continue;
        }

        match fetch_source_channels(&state.client, source, &user_agent).await {
            Ok(result) => {
                let mut unique_count = 0usize;
                for channel in result.channels {
                    let dedupe_key = format!(
                        "{}|{}|{}",
                        channel.source_slug, channel.normalized_name, channel.upstream_url
                    );
                    if seen_keys.insert(dedupe_key) {
                        by_id.insert(channel.id.clone(), channel.clone());
                        channels.push(channel);
                        unique_count += 1;
                    }
                }
                source_stats.push(SourceRuntimeStat {
                    name: source.name.clone(),
                    url: source.url.clone(),
                    enabled: true,
                    channel_count: unique_count,
                    refresh_latency_ms: Some(result.latency_ms),
                    last_status: if unique_count > 0 {
                        "正常".to_string()
                    } else {
                        "已抓取但无频道".to_string()
                    },
                    last_error: String::new(),
                    last_refresh_at: format_now_local(),
                    access_mode,
                    proxy_target,
                    proxy_region,
                });
            }
            Err(err) => {
                warn!("source {} refresh failed: {}", source.name, err);
                errors.insert(source.name.clone(), err.to_string());
                source_stats.push(SourceRuntimeStat {
                    name: source.name.clone(),
                    url: source.url.clone(),
                    enabled: true,
                    channel_count: 0,
                    refresh_latency_ms: None,
                    last_status: "抓取失败".to_string(),
                    last_error: err.to_string(),
                    last_refresh_at: format_now_local(),
                    access_mode,
                    proxy_target,
                    proxy_region,
                });
            }
        }
    }

    channels.sort_by(|a, b| {
        a.group
            .cmp(&b.group)
            .then(a.normalized_name.cmp(&b.normalized_name))
            .then(a.source_slug.cmp(&b.source_slug))
            .then(a.name.cmp(&b.name))
    });
    let last_refresh_ok = !channels.is_empty() || errors.is_empty();

    let mut runtime = state.runtime.write().await;
    runtime.channels = channels;
    runtime.by_id = by_id;
    runtime.source_errors = errors;
    runtime.source_stats = source_stats;
    runtime.last_refresh_ok = last_refresh_ok;
}

async fn fetch_source_channels(
    client: &Client,
    source: &SourceConfig,
    user_agent: &str,
) -> anyhow::Result<FetchSourceResult> {
    let mut latency_ms = 0u64;
    let text = if is_remote_url(&source.url) {
        let client = match proxied_client(client, source.proxy_url.as_deref(), user_agent) {
            Ok(client) => client,
            Err(err) => return Err(anyhow::anyhow!(err.message)),
        };
        let mut last_error: Option<anyhow::Error> = None;
        let mut body = String::new();
        let candidates = source_fetch_candidates(&source.url);
        'fetch: for candidate in candidates {
            for attempt in 0..3 {
                let start = std::time::Instant::now();
                match client
                    .get(&candidate)
                    .timeout(Duration::from_secs(35))
                    .send()
                    .await
                {
                    Ok(response) => match response.error_for_status() {
                        Ok(response) => match response.text().await {
                            Ok(text) => {
                                latency_ms = start.elapsed().as_millis() as u64;
                                body = text;
                                last_error = None;
                                break 'fetch;
                            }
                            Err(err) => last_error = Some(anyhow::anyhow!(err)),
                        },
                        Err(err) => last_error = Some(anyhow::anyhow!(err)),
                    },
                    Err(err) => last_error = Some(anyhow::anyhow!(err)),
                }

                if attempt < 2 {
                    time::sleep(Duration::from_millis(600 * (attempt as u64 + 1))).await;
                }
            }
        }
        if let Some(err) = last_error
            && body.is_empty()
        {
            return Err(err);
        }
        body
    } else {
        let start = std::time::Instant::now();
        let body = tokio::fs::read_to_string(&source.url).await?;
        latency_ms = start.elapsed().as_millis() as u64;
        body
    };
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

        let upstream_url = rewrite_loopback_stream_url(&source.url, line);
        let normalized_name = normalize_channel_name(&meta.name);
        let source_slug = slugify_source_name(&source.name);
        let id = build_channel_id(&source_slug, &normalized_name, &upstream_url);
        channels.push(Channel {
            id,
            name: meta.name,
            normalized_name,
            group: meta.group,
            tvg_id: meta.tvg_id,
            tvg_logo: meta.tvg_logo,
            source_name: source.name.clone(),
            source_slug,
            source_proxy_url: source.proxy_url.clone(),
            source_output_mode: normalize_output_mode(source.output_mode.as_deref()).to_string(),
            upstream_url,
        });
    }

    Ok(FetchSourceResult {
        channels,
        latency_ms,
    })
}

fn is_remote_url(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
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

    if let Some(name) = line.split_once(',').map(|(_, name)| name.trim())
        && !name.is_empty()
    {
        meta.name = name.to_string();
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

fn normalize_channel_name(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(|c| c.to_lowercase())
        .collect::<String>()
}

fn slugify_source_name(source_name: &str) -> String {
    let slug = source_name
        .trim()
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

    if slug.is_empty() {
        "source".to_string()
    } else {
        slug
    }
}

fn build_channel_id(source_slug: &str, normalized_name: &str, upstream_url: &str) -> String {
    let seed = format!("{source_slug}:{normalized_name}:{upstream_url}");
    let mut mac = HmacSha256::new_from_slice(seed.as_bytes()).expect("valid hmac key");
    mac.update(upstream_url.as_bytes());
    let digest = mac.finalize().into_bytes();
    let short = URL_SAFE_NO_PAD.encode(digest);
    let channel_slug = normalized_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .take(6)
        .collect::<Vec<_>>()
        .join("-");
    let channel_slug = if channel_slug.is_empty() {
        "channel"
    } else {
        &channel_slug
    };
    format!("{channel_slug}-{}", &short[..10])
}

async fn root() -> impl IntoResponse {
    Redirect::to("/admin")
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let (last_refresh_ok, channels_len, source_errors) = {
        let runtime = state.runtime.read().await;
        (
            runtime.last_refresh_ok,
            runtime.channels.len(),
            runtime.source_errors.clone(),
        )
    };
    let config = state.config.read().await.clone();
    let latest_version = fetch_latest_version(&state).await.ok().flatten();
    let current_version = state.app_version.as_ref().clone();
    let update_available = latest_version
        .as_ref()
        .map(|latest| latest != &current_version)
        .unwrap_or(false);
    let epg_cache_ready = match epg_cache_paths(&config) {
        Ok(paths) => paths.raw_path.exists(),
        Err(_) => false,
    };
    Json(HealthResponse {
        ok: last_refresh_ok,
        channels: channels_len,
        last_refresh_ok,
        source_errors,
        epg_cache_ready,
        version: current_version,
        latest_version,
        update_available,
        auto_update_enabled: state.auto_update_enabled,
        manual_update_enabled: state.manual_update_command.is_some(),
    })
}

async fn channels(State(state): State<AppState>) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    Json(runtime.channels.clone())
}

async fn epg_xml(State(state): State<AppState>) -> Result<Response<Body>, AppError> {
    let paths = ensure_epg_cache(&state).await?;
    stream_file_response(
        paths.raw_path,
        "application/xml; charset=utf-8",
        Some("public, max-age=300"),
    )
    .await
}

async fn epg_xml_gz(State(state): State<AppState>) -> Result<Response<Body>, AppError> {
    let paths = ensure_epg_cache(&state).await?;
    stream_file_response(
        paths.gzip_path,
        "application/gzip",
        Some("public, max-age=300"),
    )
    .await
}

fn local_epg_source_path(source: &str) -> Option<String> {
    if source.starts_with("http://") || source.starts_with("https://") {
        return None;
    }

    let trimmed = source.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(
        trimmed
            .strip_prefix("file://")
            .unwrap_or(trimmed)
            .to_string(),
    )
}

fn resolve_local_epg_path(source: &str) -> Result<PathBuf, AppError> {
    let path = PathBuf::from(source);
    if !source.contains('*') {
        return Ok(path);
    }

    let parent = path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or_else(|| FsPath::new("."));
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| AppError::not_found("invalid epg file pattern"))?;
    let (prefix, suffix) = file_name
        .split_once('*')
        .ok_or_else(|| AppError::not_found("invalid epg file pattern"))?;

    let mut matched: Option<(std::time::SystemTime, PathBuf)> = None;
    let entries = std::fs::read_dir(parent)
        .map_err(|err| AppError::not_found(&format!("epg directory not available: {err}")))?;

    for entry in entries {
        let entry = entry.map_err(|err| AppError::not_found(&format!("epg file error: {err}")))?;
        let entry_path = entry.path();
        let Some(name) = entry_path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if !name.starts_with(prefix) || !name.ends_with(suffix) {
            continue;
        }

        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);

        match &matched {
            Some((current_modified, _)) if modified <= *current_modified => {}
            _ => matched = Some((modified, entry_path)),
        }
    }

    matched
        .map(|(_, path)| path)
        .ok_or_else(|| AppError::not_found("no matching epg file found"))
}

async fn ensure_epg_cache(state: &AppState) -> Result<EpgCachePaths, AppError> {
    let _guard = state.epg_cache_lock.lock().await;
    let config = state.config.read().await.clone();
    let epg_source_url = config
        .epg_source_url
        .clone()
        .ok_or_else(|| AppError::not_found("epg not configured"))?;
    let paths = epg_cache_paths(&config)?;
    tokio::fs::create_dir_all(
        paths
            .raw_path
            .parent()
            .ok_or_else(|| AppError::internal("invalid epg cache path"))?,
    )
    .await
    .map_err(AppError::internal)?;

    let cache_ttl = Duration::from_secs(config.epg_cache_minutes.unwrap_or(720).max(1) * 60);
    let local_source = local_epg_source_path(&epg_source_url);

    let should_refresh_raw = if let Some(local_path) = local_source.as_ref() {
        let source_path = resolve_local_epg_path(local_path)?;
        cache_is_stale_for_file(&paths.raw_path, &source_path).await?
    } else {
        cache_is_stale_by_age(&paths.raw_path, cache_ttl).await?
    };

    if should_refresh_raw {
        if let Some(local_path) = local_source {
            let source_path = resolve_local_epg_path(&local_path)?;
            copy_epg_file(&source_path, &paths.raw_path).await?;
        } else {
            fetch_epg_to_file(state, &config, &epg_source_url, &paths.raw_path).await?;
        }
    }

    let should_refresh_gzip = cache_is_stale_for_file(&paths.gzip_path, &paths.raw_path).await?;
    if should_refresh_gzip {
        gzip_file(&paths.raw_path, &paths.gzip_path).await?;
    }

    Ok(paths)
}

fn epg_cache_paths(config: &Config) -> Result<EpgCachePaths, AppError> {
    let cache_dir = config
        .epg_cache_dir
        .clone()
        .unwrap_or_else(|| "/app/config/cache".to_string());
    let cache_dir = PathBuf::from(cache_dir);
    if cache_dir.as_os_str().is_empty() {
        return Err(AppError::bad_request("节目单缓存目录不能为空"));
    }
    Ok(EpgCachePaths {
        raw_path: cache_dir.join("epg.xml"),
        gzip_path: cache_dir.join("epg.xml.gz"),
    })
}

async fn cache_is_stale_by_age(path: &PathBuf, ttl: Duration) -> Result<bool, AppError> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(AppError::internal(err)),
    };
    let modified = metadata.modified().map_err(AppError::internal)?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age >= ttl)
}

async fn cache_is_stale_for_file(path: &PathBuf, source_path: &PathBuf) -> Result<bool, AppError> {
    let cache_metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => return Err(AppError::internal(err)),
    };
    let source_metadata = tokio::fs::metadata(source_path)
        .await
        .map_err(|err| AppError::not_found(&format!("epg source file not available: {err}")))?;
    let cache_mtime = cache_metadata.modified().map_err(AppError::internal)?;
    let source_mtime = source_metadata.modified().map_err(AppError::internal)?;
    Ok(cache_mtime < source_mtime)
}

async fn copy_epg_file(source_path: &PathBuf, target_path: &PathBuf) -> Result<(), AppError> {
    let tmp_path = temp_path_for(target_path, "copy");
    let bytes = tokio::fs::read(source_path)
        .await
        .map_err(|err| AppError::not_found(&format!("epg source file not available: {err}")))?;
    tokio::fs::write(&tmp_path, bytes)
        .await
        .map_err(AppError::internal)?;
    tokio::fs::rename(&tmp_path, target_path)
        .await
        .map_err(AppError::internal)?;
    Ok(())
}

async fn fetch_epg_to_file(
    state: &AppState,
    config: &Config,
    source_url: &str,
    target_path: &PathBuf,
) -> Result<(), AppError> {
    let user_agent = config
        .user_agent
        .clone()
        .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string());
    let client = proxied_client(&state.client, config.epg_proxy_url.as_deref(), &user_agent)?;
    let response = client
        .get(source_url)
        .send()
        .await
        .map_err(AppError::bad_gateway)?
        .error_for_status()
        .map_err(AppError::bad_gateway)?;

    let tmp_path = temp_path_for(target_path, "fetch");
    let mut file = tokio::fs::File::create(&tmp_path)
        .await
        .map_err(AppError::internal)?;
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.try_next().await.map_err(AppError::bad_gateway)? {
        file.write_all(&chunk).await.map_err(AppError::internal)?;
    }
    file.flush().await.map_err(AppError::internal)?;
    drop(file);
    tokio::fs::rename(&tmp_path, target_path)
        .await
        .map_err(AppError::internal)?;
    Ok(())
}

async fn gzip_file(source_path: &FsPath, target_path: &FsPath) -> Result<(), AppError> {
    let source_path = source_path.to_path_buf();
    let target_path = target_path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<(), AppError> {
        let input = std::fs::read(&source_path).map_err(AppError::internal)?;
        let tmp_path = temp_path_for(&target_path, "gzip");
        let file = std::fs::File::create(&tmp_path).map_err(AppError::internal)?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        use std::io::Write;
        encoder.write_all(&input).map_err(AppError::internal)?;
        encoder.finish().map_err(AppError::internal)?;
        std::fs::rename(&tmp_path, &target_path).map_err(AppError::internal)?;
        Ok(())
    })
    .await
    .map_err(AppError::internal)?
}

fn temp_path_for(path: &FsPath, suffix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    path.with_extension(format!("{suffix}.{nanos}.tmp"))
}

async fn stream_file_response(
    path: PathBuf,
    content_type: &'static str,
    cache_control: Option<&'static str>,
) -> Result<Response<Body>, AppError> {
    let file = tokio::fs::File::open(&path)
        .await
        .map_err(|err| AppError::not_found(&format!("file not available: {err}")))?;
    let metadata = file.metadata().await.map_err(AppError::internal)?;
    let mut response = Response::new(Body::from_stream(ReaderStream::new(file)));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    if let Some(value) = cache_control {
        response.headers_mut().insert(
            axum::http::header::CACHE_CONTROL,
            HeaderValue::from_static(value),
        );
    }
    if let Ok(value) = HeaderValue::from_str(&metadata.len().to_string()) {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LENGTH, value);
    }
    Ok(response)
}

async fn tick_recordings(state: AppState) -> Result<(), AppError> {
    let now = Local::now().fixed_offset();
    let tasks = state.recordings.read().await.clone();
    for task in tasks {
        if !task.enabled || task.status != RecordingStatus::Scheduled {
            continue;
        }

        let actual_start = recording_actual_start(&task)?;
        let actual_end = recording_actual_end(&task)?;

        if now >= actual_end {
            update_recording_status(
                &state,
                &task.id,
                RecordingStatus::Missed,
                None,
                Some("已错过录制时间窗".to_string()),
            )
            .await?;
            continue;
        }

        if now < actual_start {
            continue;
        }

        let mut active = state.active_recordings.lock().await;
        if active.contains(&task.id) {
            continue;
        }
        active.insert(task.id.clone());
        drop(active);

        update_recording_status(&state, &task.id, RecordingStatus::Running, None, None).await?;
        let state_cloned = state.clone();
        tokio::spawn(async move {
            let result = run_recording_task(&state_cloned, &task).await;
            let (status, output_file, last_error) = match result {
                Ok(file) => (RecordingStatus::Completed, Some(file), None),
                Err(err) => (RecordingStatus::Failed, None, Some(err.message)),
            };
            if let Err(err) =
                update_recording_status(&state_cloned, &task.id, status, output_file, last_error)
                    .await
            {
                warn!("update recording status failed: {}", err.message);
            }
            state_cloned.active_recordings.lock().await.remove(&task.id);
        });
    }
    Ok(())
}

async fn update_recording_status(
    state: &AppState,
    id: &str,
    status: RecordingStatus,
    output_file: Option<String>,
    last_error: Option<String>,
) -> Result<(), AppError> {
    {
        let mut tasks = state.recordings.write().await;
        if let Some(task) = tasks.iter_mut().find(|task| task.id == id) {
            task.status = status;
            if output_file.is_some() {
                task.output_file = output_file;
            }
            task.last_error = last_error;
        }
    }
    save_recordings(state).await
}

async fn run_recording_task(state: &AppState, task: &RecordingTask) -> Result<String, AppError> {
    let config = state.config.read().await.clone();
    let output_dir = PathBuf::from(
        config
            .recordings_dir
            .unwrap_or_else(|| "/app/config/recordings".to_string()),
    );
    tokio::fs::create_dir_all(&output_dir)
        .await
        .map_err(AppError::internal)?;

    let actual_start = recording_actual_start(task)?;
    let actual_end = recording_actual_end(task)?;
    let duration = (actual_end - actual_start).num_seconds().max(1);
    let file_name = sanitize_filename(&format!(
        "{}-{}-{}.ts",
        task.channel_name,
        task.program_title,
        actual_start.format("%Y%m%d-%H%M%S")
    ));
    let output_path = output_dir.join(file_name);
    let stream_url = format!("http://127.0.0.1:8787/live/{}", task.channel_id);

    let output = Command::new("ffmpeg")
        .arg("-y")
        .arg("-i")
        .arg(stream_url)
        .arg("-t")
        .arg(duration.to_string())
        .arg("-c")
        .arg("copy")
        .arg(output_path.to_string_lossy().to_string())
        .output()
        .await
        .map_err(AppError::internal)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(AppError::bad_gateway(if stderr.is_empty() {
            "ffmpeg 录制失败".to_string()
        } else {
            stderr
        }));
    }

    Ok(output_path.to_string_lossy().to_string())
}

fn recording_actual_start(task: &RecordingTask) -> Result<DateTime<FixedOffset>, AppError> {
    let start = parse_rfc3339_local(&task.start_at)?;
    Ok(start - chrono::Duration::minutes(task.pre_minutes))
}

fn recording_actual_end(task: &RecordingTask) -> Result<DateTime<FixedOffset>, AppError> {
    let end = parse_rfc3339_local(&task.end_at)?;
    Ok(end + chrono::Duration::minutes(task.post_minutes))
}

fn parse_rfc3339_local(value: &str) -> Result<DateTime<FixedOffset>, AppError> {
    DateTime::parse_from_rfc3339(value).map_err(|_| AppError::bad_request("时间格式无效"))
}

fn sanitize_filename(input: &str) -> String {
    input
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => ch,
        })
        .collect()
}

async fn load_epg_programmes_for_channel(
    state: &AppState,
    channel_id: &str,
) -> Result<Vec<EpgProgrammeView>, AppError> {
    let paths = ensure_epg_cache(state).await?;
    let channel = {
        let runtime = state.runtime.read().await;
        runtime
            .by_id
            .get(channel_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("channel not found"))?
    };
    let mut candidates = vec![channel.name.clone()];
    if let Some(tvg_id) = channel.tvg_id.clone() {
        candidates.push(tvg_id);
    }

    let path = paths.raw_path.clone();
    tokio::task::spawn_blocking(move || parse_programmes_from_xml(&path, &candidates))
        .await
        .map_err(AppError::internal)?
}

fn parse_programmes_from_xml(
    path: &PathBuf,
    candidates: &[String],
) -> Result<Vec<EpgProgrammeView>, AppError> {
    let data = std::fs::read(path).map_err(AppError::internal)?;
    let mut reader = Reader::from_reader(data.as_slice());
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    let candidate_set: HashSet<String> = candidates.iter().cloned().collect();
    let mut programmes = Vec::new();
    let mut current_channel = String::new();
    let mut current_start = String::new();
    let mut current_end = String::new();
    let mut current_title = String::new();
    let mut in_target_programme = false;
    let mut in_title = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(event)) if event.name().as_ref() == b"programme" => {
                current_channel.clear();
                current_start.clear();
                current_end.clear();
                current_title.clear();
                for attr in event.attributes().flatten() {
                    match attr.key.as_ref() {
                        b"channel" => {
                            current_channel =
                                String::from_utf8_lossy(attr.value.as_ref()).to_string()
                        }
                        b"start" => {
                            current_start = String::from_utf8_lossy(attr.value.as_ref()).to_string()
                        }
                        b"stop" => {
                            current_end = String::from_utf8_lossy(attr.value.as_ref()).to_string()
                        }
                        _ => {}
                    }
                }
                in_target_programme = candidate_set.contains(&current_channel);
            }
            Ok(Event::Start(event)) if event.name().as_ref() == b"title" => {
                in_title = in_target_programme;
            }
            Ok(Event::Text(text)) if in_title => {
                current_title = String::from_utf8_lossy(text.as_ref()).to_string();
            }
            Ok(Event::End(event)) if event.name().as_ref() == b"title" => {
                in_title = false;
            }
            Ok(Event::End(event)) if event.name().as_ref() == b"programme" => {
                if in_target_programme {
                    let start = parse_xmltv_datetime(&current_start)?;
                    let end = parse_xmltv_datetime(&current_end)?;
                    programmes.push(EpgProgrammeView {
                        title: current_title.clone(),
                        start_at: start.to_rfc3339(),
                        end_at: end.to_rfc3339(),
                    });
                }
                in_target_programme = false;
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(AppError::bad_gateway(err)),
            _ => {}
        }
        buf.clear();
    }

    programmes.sort_by(|a, b| a.start_at.cmp(&b.start_at));
    Ok(programmes)
}

fn parse_xmltv_datetime(value: &str) -> Result<DateTime<FixedOffset>, AppError> {
    DateTime::parse_from_str(value, "%Y%m%d%H%M%S %z")
        .or_else(|_| DateTime::parse_from_str(value, "%Y%m%d%H%M%S%z"))
        .map_err(|_| AppError::bad_request("节目单时间格式无效"))
}

async fn list_m3u(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    let base = public_base_url(&state, &headers, &uri).await;
    let epg_url = {
        let config = state.config.read().await;
        config
            .epg_source_url
            .as_ref()
            .map(|_| format!("{}/epg.xml", base))
    };

    let mut body = String::from("#EXTM3U");
    if let Some(epg_url) = epg_url {
        body.push_str(&format!(r#" x-tvg-url="{}""#, epg_url));
    }
    body.push('\n');
    for channel in &runtime.channels {
        body.push_str("#EXTINF:-1");
        if let Some(tvg_id) = &channel.tvg_id {
            body.push_str(&format!(r#" tvg-id="{}""#, tvg_id));
        }
        if let Some(tvg_logo) = &channel.tvg_logo {
            body.push_str(&format!(r#" tvg-logo="{}""#, tvg_logo));
        }
        body.push_str(&format!(
            r#" group-title="{}" source-name="{}" source-slug="{}""#,
            channel.group, channel.source_name, channel.source_slug
        ));
        body.push_str(&format!(
            r#" access-mode="{}""#,
            output_mode_label(Some(&channel.source_output_mode))
        ));
        body.push_str(&format!(",{} [{}]\n", channel.name, channel.source_name));
        body.push_str(&format!("{}\n", channel_output_url(&base, channel)));
    }

    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-mpegURL; charset=utf-8"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate, max-age=0"),
    );
    response.headers_mut().insert(
        axum::http::header::PRAGMA,
        HeaderValue::from_static("no-cache"),
    );
    response
        .headers_mut()
        .insert(axum::http::header::EXPIRES, HeaderValue::from_static("0"));
    response
}

async fn list_txt(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    let runtime = state.runtime.read().await;
    let base = public_base_url(&state, &headers, &uri).await;

    let mut body = String::new();
    for channel in &runtime.channels {
        body.push_str(&channel.name);
        body.push_str(" [");
        body.push_str(&channel.source_name);
        body.push(']');
        body.push(',');
        body.push_str(&channel_output_url(&base, channel));
        body.push('\n');
    }

    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
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

    proxy_upstream(
        &state,
        &id,
        &channel.upstream_url,
        channel.source_proxy_url.as_deref(),
        &headers,
        &uri,
    )
    .await
}

async fn live_with_source(
    State(state): State<AppState>,
    Path((source, id)): Path<(String, String)>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response<Body>, AppError> {
    let channel = {
        let runtime = state.runtime.read().await;
        runtime.by_id.get(&id).cloned()
    }
    .ok_or_else(|| AppError::not_found("channel not found"))?;

    if channel.source_slug != source {
        return Err(AppError::not_found("channel not found for source"));
    }

    proxy_upstream(
        &state,
        &id,
        &channel.upstream_url,
        channel.source_proxy_url.as_deref(),
        &headers,
        &uri,
    )
    .await
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
    let source_proxy_url = {
        let runtime = state.runtime.read().await;
        runtime
            .by_id
            .get(&id)
            .and_then(|channel| channel.source_proxy_url.clone())
    };
    proxy_upstream(
        &state,
        &id,
        &query.u,
        source_proxy_url.as_deref(),
        &headers,
        &uri,
    )
    .await
}

async fn proxy_upstream(
    state: &AppState,
    channel_id: &str,
    target_url: &str,
    source_proxy_url: Option<&str>,
    headers: &HeaderMap,
    uri: &Uri,
) -> Result<Response<Body>, AppError> {
    let user_agent = {
        let config = state.config.read().await;
        config
            .user_agent
            .clone()
            .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string())
    };
    let client = proxied_client(&state.client, source_proxy_url, &user_agent)?;
    let mut request = client.get(target_url);

    for header_name in [
        reqwest::header::RANGE,
        reqwest::header::IF_RANGE,
        reqwest::header::ACCEPT,
        reqwest::header::ACCEPT_LANGUAGE,
        reqwest::header::REFERER,
    ] {
        if let Some(value) = headers.get(&header_name) {
            request = request.header(&header_name, value);
        }
    }

    let upstream = request.send().await.map_err(AppError::bad_gateway)?;
    let upstream = upstream.error_for_status().map_err(AppError::bad_gateway)?;
    let upstream_status = upstream.status();

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

    let upstream_headers = upstream.headers().clone();
    let mut response = Response::new(Body::from_stream(
        upstream
            .bytes_stream()
            .map_err(|err| std::io::Error::other(err.to_string())),
    ));
    *response.status_mut() = upstream_status;
    for header_name in [
        reqwest::header::CONTENT_TYPE,
        reqwest::header::CONTENT_LENGTH,
        reqwest::header::CONTENT_RANGE,
        reqwest::header::ACCEPT_RANGES,
        reqwest::header::CACHE_CONTROL,
        reqwest::header::ETAG,
        reqwest::header::LAST_MODIFIED,
    ] {
        if let Some(value) = upstream_headers.get(&header_name) {
            response.headers_mut().insert(header_name, value.clone());
        }
    }
    Ok(response)
}

fn proxied_client(
    base: &Client,
    proxy_url: Option<&str>,
    user_agent: &str,
) -> Result<Client, AppError> {
    let Some(proxy_url) = proxy_url.filter(|value| !value.trim().is_empty()) else {
        return Ok(base.clone());
    };

    Client::builder()
        .user_agent(user_agent)
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(10))
        .proxy(
            reqwest::Proxy::all(proxy_url)
                .map_err(|err| AppError::bad_request(&format!("代理地址无效: {err}")))?,
        )
        .build()
        .map_err(AppError::internal)
}

async fn admin_page(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Html<String>, AppError> {
    let config = state.config.read().await.clone();
    let recordings = state.recordings.read().await.clone();
    let (channels_snapshot, source_stats_snapshot) = {
        let runtime = state.runtime.read().await;
        (runtime.channels.clone(), runtime.source_stats.clone())
    };
    let current_version = state.app_version.as_ref().clone();
    let latest_version = fetch_latest_version(&state)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| current_version.clone());
    let update_available = latest_version != current_version;
    let status = query.get("status").map(String::as_str).unwrap_or("");
    let (status_message, status_class) = match status {
        "saved" => ("保存成功，频道列表正在后台刷新。", "ok"),
        "uploaded" => ("本地文件已上传并加入源列表，频道列表正在后台刷新。", "ok"),
        "refreshed" => ("已重新抓取全部直播源，频道列表和诊断信息都已更新。", "ok"),
        "saved_xhs_applied" => (
            "保存成功，已把 xhsuhd 凭证应用到上游容器，频道列表正在后台刷新。",
            "ok",
        ),
        "saved_xhs_pending" => (
            "保存成功，频道列表正在后台刷新；xhsuhd 凭证已写入配置，但当前部署未启用一键应用。",
            "ok",
        ),
        "updated" => ("手动更新命令已执行完成，容器重启后版本号会自动刷新。", "ok"),
        "update_error" => (
            query
                .get("message")
                .map(String::as_str)
                .unwrap_or("手动更新失败，请查看容器日志确认原因。"),
            "error",
        ),
        "xhs_applied" => ("xhsuhd 凭证已重新应用，失效后可在这里直接替换。", "ok"),
        "xhs_apply_error" => ("xhsuhd 凭证应用失败，请查看容器日志确认原因。", "error"),
        "error" => ("保存失败，请检查输入内容后重试。", "error"),
        _ => ("", ""),
    };

    let channels_json = serde_json::to_string(
        &channels_snapshot
            .iter()
            .take(ADMIN_CHANNEL_OPTION_LIMIT)
            .map(|channel| ChannelOptionView {
                id: channel.id.clone(),
                name: channel.name.clone(),
                label: format!("{} [{}]", channel.name, channel.source_name),
                tvg_id: channel.tvg_id.clone().unwrap_or_default(),
                source_name: channel.source_name.clone(),
            })
            .collect::<Vec<_>>(),
    )
    .map_err(AppError::internal)?;
    let source_statuses_json =
        serde_json::to_string(&source_stats_snapshot).map_err(AppError::internal)?;
    let source_stats_by_name = source_stats_snapshot
        .iter()
        .map(|item| (item.name.clone(), item.clone()))
        .collect::<HashMap<_, _>>();
    let channel_statuses_json = serde_json::to_string(
        &channels_snapshot
            .iter()
            .take(ADMIN_CHANNEL_DIAGNOSTIC_LIMIT)
            .map(|channel| {
                let source_stat = source_stats_by_name.get(&channel.source_name);
                let upstream_host = Url::parse(&channel.upstream_url)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_else(|| "-".to_string());
                AdminChannelStatusView {
                    id: channel.id.clone(),
                    name: channel.name.clone(),
                    normalized_name: channel.normalized_name.clone(),
                    group: channel.group.clone(),
                    source_name: channel.source_name.clone(),
                    source_slug: channel.source_slug.clone(),
                    upstream_host,
                    access_mode: source_stat
                        .map(|item| item.access_mode.clone())
                        .unwrap_or_else(|| "直连".to_string()),
                    proxy_region: source_stat
                        .map(|item| item.proxy_region.clone())
                        .unwrap_or_else(|| "本地出口".to_string()),
                    refresh_latency_ms: source_stat
                        .and_then(|item| item.refresh_latency_ms)
                        .unwrap_or_default(),
                    last_status: source_stat
                        .map(|item| item.last_status.clone())
                        .unwrap_or_else(|| "未知".to_string()),
                }
            })
            .collect::<Vec<_>>(),
    )
    .map_err(AppError::internal)?;
    let recordings_json = serde_json::to_string(
        &recordings
            .into_iter()
            .map(|task| RecordingTaskView {
                id: task.id,
                channel_name: task.channel_name,
                program_title: task.program_title,
                start_at: task.start_at,
                end_at: task.end_at,
                pre_minutes: task.pre_minutes,
                post_minutes: task.post_minutes,
                output_name: task.output_name,
                enabled: task.enabled,
                status: format!("{:?}", task.status).to_lowercase(),
                output_file: task.output_file.unwrap_or_default(),
                last_error: task.last_error.unwrap_or_default(),
            })
            .collect::<Vec<_>>(),
    )
    .map_err(AppError::internal)?;

    let manual_update_enabled = state.manual_update_command.is_some();
    let xhs_apply_enabled = state.xhs_apply_command.is_some();
    let manual_update_hint = if manual_update_enabled {
        "会在容器内执行预设更新命令，页面可能会短暂断开。".to_string()
    } else {
        "当前未启用手动更新，需要在部署时配置 IPTV_UPDATE_COMMAND。".to_string()
    };

    let data = AdminPageData {
        app_version: current_version,
        latest_version: latest_version.clone(),
        update_available,
        update_status: if update_available {
            format!("发现新版本 v{latest_version}，可以手动更新或等待自动更新。")
        } else {
            "当前已经是最新版本。".to_string()
        },
        app_image: state.app_image.as_ref().clone(),
        auto_update_status: if state.auto_update_enabled {
            "已启用（Watchtower 或外部策略会自动拉取新版本）".to_string()
        } else {
            "未启用".to_string()
        },
        manual_update_label: if manual_update_enabled {
            if update_available {
                format!("立即更新到 v{latest_version}")
            } else {
                "重新拉取并更新".to_string()
            }
        } else {
            "手动更新未启用".to_string()
        },
        manual_update_hint,
        manual_update_enabled,
        xhs_a1: config.xhs_a1.clone().unwrap_or_default(),
        xhs_web_session: config.xhs_web_session.clone().unwrap_or_default(),
        xhs_apply_enabled,
        xhs_apply_hint: if xhs_apply_enabled {
            "保存后可一键把新 Cookie 应用到 xhsuhd 容器。".to_string()
        } else {
            "当前部署未启用一键应用，只会把值保存到配置文件和 xhsuhd.env。".to_string()
        },
        public_base_url: config.public_base_url.unwrap_or_default(),
        refresh_minutes: config
            .refresh_minutes
            .map(|v| v.to_string())
            .unwrap_or_else(|| "30".to_string()),
        user_agent: config
            .user_agent
            .unwrap_or_else(|| "home-iptv-proxy/0.1".to_string()),
        epg_source_url: config.epg_source_url.unwrap_or_default(),
        epg_proxy_url: config.epg_proxy_url.unwrap_or_default(),
        epg_cache_minutes: config
            .epg_cache_minutes
            .map(|v| v.to_string())
            .unwrap_or_else(|| "720".to_string()),
        epg_cache_dir: config
            .epg_cache_dir
            .unwrap_or_else(|| "/app/config/cache".to_string()),
        recordings_dir: config
            .recordings_dir
            .unwrap_or_else(|| "/app/config/recordings".to_string()),
        signing_secret: config.signing_secret,
        sources: if config.sources.is_empty() {
            vec![AdminSourceView {
                name: "xhsuhd".to_string(),
                source_kind: "remote".to_string(),
                url: "http://xhsuhd:34567/xhslist.m3u".to_string(),
                proxy_url: String::new(),
                output_mode: "proxy".to_string(),
                enabled: true,
            }]
        } else {
            config
                .sources
                .into_iter()
                .map(|source| AdminSourceView {
                    name: source.name,
                    source_kind: detect_source_kind(&source.url).to_string(),
                    url: source.url,
                    proxy_url: source.proxy_url.unwrap_or_default(),
                    output_mode: normalize_output_mode(source.output_mode.as_deref()).to_string(),
                    enabled: source.enabled != Some(false),
                })
                .collect()
        },
        source_statuses_json,
        channel_statuses_json,
        channels_json,
        recordings_json,
        status_message: status_message.to_string(),
        status_class: status_class.to_string(),
    };

    let html = render_admin_page(data)?;
    Ok(Html(html))
}

async fn save_admin(
    State(state): State<AppState>,
    RawForm(form): RawForm,
) -> Result<Redirect, AppError> {
    let form_text = String::from_utf8(form.to_vec()).map_err(AppError::internal)?;
    let mut values: HashMap<String, Vec<String>> = HashMap::new();
    for (key, value) in url::form_urlencoded::parse(form_text.as_bytes()) {
        values
            .entry(key.into_owned())
            .or_default()
            .push(value.into_owned());
    }

    let source_names = values.remove("source_name").unwrap_or_default();
    let source_urls = values.remove("source_url").unwrap_or_default();
    let source_proxy_urls = values.remove("source_proxy_url").unwrap_or_default();
    let source_output_modes = values.remove("source_output_mode").unwrap_or_default();
    let enabled_flags = values.remove("source_enabled").unwrap_or_default();

    let mut sources = Vec::new();
    let row_count = source_names
        .len()
        .max(source_urls.len())
        .max(source_proxy_urls.len())
        .max(source_output_modes.len());
    for idx in 0..row_count {
        let name = source_names
            .get(idx)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let url = source_urls
            .get(idx)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let proxy_url = source_proxy_urls
            .get(idx)
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let output_mode = source_output_modes
            .get(idx)
            .map(|v| normalize_output_mode(Some(v.as_str())).to_string())
            .unwrap_or_else(|| "proxy".to_string());

        if name.is_empty() && url.is_empty() && proxy_url.is_empty() {
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
            proxy_url: clean_optional(Some(proxy_url)),
            output_mode: Some(output_mode),
            enabled: Some(enabled),
        });
    }

    let refresh_minutes = values
        .get("refresh_minutes")
        .and_then(|v| v.first())
        .map(String::as_str)
        .unwrap_or("30")
        .trim()
        .parse::<u64>()
        .map_err(|_| AppError::bad_request("刷新间隔必须是数字"))?;

    let epg_cache_minutes = values
        .get("epg_cache_minutes")
        .and_then(|v| v.first())
        .map(String::as_str)
        .unwrap_or("720")
        .trim()
        .parse::<u64>()
        .map_err(|_| AppError::bad_request("节目单缓存分钟数必须是数字"))?;

    let signing_secret = values
        .get("signing_secret")
        .and_then(|v| v.first())
        .map(|v| v.trim().to_string())
        .unwrap_or_default();
    if signing_secret.is_empty() {
        return Err(AppError::bad_request("签名密钥不能为空"));
    }

    let new_config = Config {
        bind: Some("0.0.0.0:8787".to_string()),
        public_base_url: clean_optional(first_value(&values, "public_base_url")),
        refresh_minutes: Some(refresh_minutes.max(1)),
        user_agent: clean_optional(first_value(&values, "user_agent")),
        epg_source_url: clean_optional(first_value(&values, "epg_source_url")),
        epg_proxy_url: clean_optional(first_value(&values, "epg_proxy_url")),
        epg_cache_minutes: Some(epg_cache_minutes.max(1)),
        epg_cache_dir: clean_optional(first_value(&values, "epg_cache_dir")),
        recordings_dir: clean_optional(first_value(&values, "recordings_dir")),
        xhs_a1: clean_optional(first_value(&values, "xhs_a1")),
        xhs_web_session: clean_optional(first_value(&values, "xhs_web_session")),
        signing_secret,
        sources,
    };

    persist_config(&state, new_config).await?;
    sync_xhs_env_file(&state).await?;
    spawn_channel_refresh(state.clone());
    let status = if state.xhs_apply_command.is_some() {
        match run_xhs_apply_command(&state).await {
            Ok(()) => "saved_xhs_applied",
            Err(err) => {
                warn!("xhs apply after save failed: {}", err.message);
                "saved_xhs_pending"
            }
        }
    } else {
        "saved_xhs_pending"
    };
    Ok(Redirect::to(&format!("/admin?status={status}")))
}

async fn toggle_source(
    State(state): State<AppState>,
    Json(form): Json<ToggleSourceForm>,
) -> Result<Json<ToggleSourceResponse>, AppError> {
    let mut config = state.config.read().await.clone();
    let Some(source) = config.sources.get_mut(form.index) else {
        return Err(AppError::bad_request("订阅源不存在，请刷新后台后再试"));
    };

    source.enabled = Some(form.enabled);
    let source_name = source.name.clone();
    persist_config(&state, config).await?;

    if form.enabled {
        spawn_channel_refresh(state.clone());
    } else {
        prune_runtime_source(&state, &source_name).await;
    }

    Ok(Json(ToggleSourceResponse {
        ok: true,
        index: form.index,
        enabled: form.enabled,
        message: if form.enabled {
            "已启用，正在后台刷新这批频道。".to_string()
        } else {
            "已关闭，播放列表已立即移除这条源的频道。".to_string()
        },
    }))
}

async fn upload_source_file(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Redirect, AppError> {
    let mut source_name = String::new();
    let mut proxy_url: Option<String> = None;
    let mut output_mode = "proxy".to_string();
    let mut enabled = true;
    let mut uploaded_file: Option<(String, Vec<u8>)> = None;

    while let Some(field) = multipart.next_field().await.map_err(AppError::internal)? {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "source_name" => {
                source_name = field
                    .text()
                    .await
                    .map_err(AppError::internal)?
                    .trim()
                    .to_string();
            }
            "source_proxy_url" => {
                proxy_url = clean_optional(Some(
                    field
                        .text()
                        .await
                        .map_err(AppError::internal)?
                        .trim()
                        .to_string(),
                ));
            }
            "source_output_mode" => {
                output_mode = normalize_output_mode(Some(
                    field.text().await.map_err(AppError::internal)?.trim(),
                ))
                .to_string();
            }
            "source_enabled" => {
                let value = field.text().await.map_err(AppError::internal)?;
                enabled = matches!(value.trim(), "1" | "true" | "on" | "yes");
            }
            "source_file" => {
                let filename = field
                    .file_name()
                    .map(str::to_string)
                    .unwrap_or_else(|| "source.m3u".to_string());
                let data = field.bytes().await.map_err(AppError::internal)?.to_vec();
                uploaded_file = Some((filename, data));
            }
            _ => {}
        }
    }

    let (filename, file_bytes) =
        uploaded_file.ok_or_else(|| AppError::bad_request("请选择一个本地 txt 或 m3u 文件"))?;
    if file_bytes.is_empty() {
        return Err(AppError::bad_request("上传文件不能为空"));
    }

    let safe_name = sanitize_upload_name(&filename);
    if !matches_source_file_name(&safe_name) {
        return Err(AppError::bad_request(
            "仅支持 .txt / .m3u / .m3u8 文件，请重新选择",
        ));
    }

    let upload_dir = source_upload_dir(&state)?;
    tokio::fs::create_dir_all(&upload_dir)
        .await
        .map_err(AppError::internal)?;
    let stored_path = unique_upload_path(&upload_dir, &safe_name);
    tokio::fs::write(&stored_path, file_bytes)
        .await
        .map_err(AppError::internal)?;

    let mut new_config = state.config.read().await.clone();
    let default_name = stored_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("local-upload")
        .to_string();
    new_config.sources.push(SourceConfig {
        name: if source_name.trim().is_empty() {
            default_name
        } else {
            source_name
        },
        url: stored_path.to_string_lossy().to_string(),
        proxy_url,
        output_mode: Some(output_mode),
        enabled: Some(enabled),
    });

    persist_config(&state, new_config).await?;

    Ok(Redirect::to("/admin?status=uploaded"))
}

fn source_upload_dir(state: &AppState) -> Result<PathBuf, AppError> {
    let config_dir = FsPath::new(state.config_path.as_ref())
        .parent()
        .ok_or_else(|| AppError::internal("invalid config path"))?;
    Ok(config_dir.join("uploads"))
}

fn sanitize_upload_name(filename: &str) -> String {
    let mut cleaned = filename
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '-',
        })
        .collect::<String>();
    if cleaned.is_empty() {
        cleaned = "source.m3u".to_string();
    }
    cleaned
}

fn matches_source_file_name(filename: &str) -> bool {
    let lower = filename.to_ascii_lowercase();
    lower.ends_with(".txt") || lower.ends_with(".m3u") || lower.ends_with(".m3u8")
}

fn unique_upload_path(dir: &FsPath, filename: &str) -> PathBuf {
    let candidate = dir.join(filename);
    if !candidate.exists() {
        return candidate;
    }
    let stem = FsPath::new(filename)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("source");
    let ext = FsPath::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    for idx in 1..1000 {
        let alt_name = if ext.is_empty() {
            format!("{stem}-{idx}")
        } else {
            format!("{stem}-{idx}.{ext}")
        };
        let alt = dir.join(alt_name);
        if !alt.exists() {
            return alt;
        }
    }
    dir.join(format!("{}-{}.{}", stem, Local::now().timestamp(), ext))
}

async fn run_manual_update(State(state): State<AppState>) -> Result<Redirect, AppError> {
    let Some(command) = state.manual_update_command.as_ref().clone() else {
        return Err(AppError::bad_request("当前部署未启用手动更新命令"));
    };

    info!("running manual update command");
    let output = Command::new("sh")
        .arg("-lc")
        .arg(&command)
        .output()
        .await
        .map_err(AppError::internal)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = stderr.trim();
        warn!("manual update failed: {}", message);
        let message = if message.is_empty() {
            "手动更新失败，更新命令没有返回错误详情。"
        } else {
            message
        };
        return Ok(Redirect::to(&format!(
            "/admin?status=update_error&message={}",
            urlencoding::encode(message)
        )));
    }

    Ok(Redirect::to("/admin?status=updated"))
}

async fn refresh_now(State(state): State<AppState>) -> Result<Redirect, AppError> {
    spawn_channel_refresh(state.clone());
    Ok(Redirect::to("/admin?status=refreshed"))
}

async fn apply_xhs_credentials(State(state): State<AppState>) -> Result<Redirect, AppError> {
    sync_xhs_env_file(&state).await?;
    run_xhs_apply_command(&state).await?;
    Ok(Redirect::to("/admin?status=xhs_applied"))
}

async fn sync_xhs_env_file(state: &AppState) -> Result<(), AppError> {
    let config = state.config.read().await.clone();
    let body = format!(
        "XHS_A1={}\nXHS_WEB_SESSION={}\n",
        config.xhs_a1.unwrap_or_default(),
        config.xhs_web_session.unwrap_or_default()
    );
    tokio::fs::write(&*state.xhs_env_path, body)
        .await
        .map_err(AppError::internal)
}

async fn run_xhs_apply_command(state: &AppState) -> Result<(), AppError> {
    let Some(command) = state.xhs_apply_command.as_ref().clone() else {
        return Err(AppError::bad_request("当前部署未启用 xhsuhd 一键应用命令"));
    };

    let output = Command::new("sh")
        .arg("-lc")
        .arg(&command)
        .output()
        .await
        .map_err(AppError::internal)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!("xhs apply command failed: {}", stderr.trim());
        return Err(AppError::internal("xhsuhd apply command failed"));
    }

    Ok(())
}

async fn admin_epg_programmes(
    State(state): State<AppState>,
    Query(query): Query<EpgProgrammesQuery>,
) -> Result<Json<Vec<EpgProgrammeView>>, AppError> {
    let programmes = load_epg_programmes_for_channel(&state, &query.channel_id).await?;
    Ok(Json(programmes))
}

async fn create_recording(
    State(state): State<AppState>,
    RawForm(form): RawForm,
) -> Result<Redirect, AppError> {
    let form_text = String::from_utf8(form.to_vec()).map_err(AppError::internal)?;
    let form: CreateRecordingForm = serde_urlencoded::from_str(&form_text)
        .map_err(|_| AppError::bad_request("录制参数无效"))?;
    let task = RecordingTask {
        id: format!(
            "rec-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_millis()
        ),
        channel_id: form.channel_id,
        channel_name: form.channel_name,
        program_title: form.program_title.clone(),
        start_at: form.start_at,
        end_at: form.end_at,
        pre_minutes: form.pre_minutes.max(0),
        post_minutes: form.post_minutes.max(0),
        output_name: sanitize_filename(&form.program_title),
        enabled: true,
        status: RecordingStatus::Scheduled,
        output_file: None,
        last_error: None,
    };
    {
        let mut tasks = state.recordings.write().await;
        tasks.push(task);
    }
    save_recordings(&state).await?;
    Ok(Redirect::to("/admin?status=saved"))
}

async fn delete_recording(
    State(state): State<AppState>,
    RawForm(form): RawForm,
) -> Result<Redirect, AppError> {
    let form_text = String::from_utf8(form.to_vec()).map_err(AppError::internal)?;
    let form: DeleteRecordingForm = serde_urlencoded::from_str(&form_text)
        .map_err(|_| AppError::bad_request("录制任务参数无效"))?;
    {
        let mut tasks = state.recordings.write().await;
        tasks.retain(|task| task.id != form.id);
    }
    save_recordings(&state).await?;
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

fn first_value(values: &HashMap<String, Vec<String>>, key: &str) -> Option<String> {
    values.get(key).and_then(|v| v.first()).cloned()
}

fn detect_source_kind(url: &str) -> &'static str {
    if Url::parse(url)
        .map(|parsed| matches!(parsed.scheme(), "http" | "https"))
        .unwrap_or(false)
    {
        "remote"
    } else {
        "local"
    }
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
    let manual_update_action = if data.manual_update_enabled {
        format!(
            r#"<form method="post" action="/admin/update"><button class="primary" type="submit">{}</button></form>"#,
            escape_html(&data.manual_update_label)
        )
    } else {
        format!(
            r#"<button class="ghost" type="button" disabled>{}</button>"#,
            escape_html(&data.manual_update_label)
        )
    };
    let xhs_apply_action = if data.xhs_apply_enabled {
        r#"<form class="inline" method="post" action="/admin/xhs/apply"><button class="ghost" type="submit">应用 xhsuhd 凭证</button></form>"#.to_string()
    } else {
        r#"<button class="ghost" type="button" disabled>应用 xhsuhd 凭证</button>"#.to_string()
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
    .nav-tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 18px;
      position: sticky;
      top: 0;
      z-index: 5;
      padding: 10px;
      border: 1px solid rgba(217, 204, 183, 0.9);
      border-radius: 18px;
      background: rgba(255, 253, 248, 0.88);
      backdrop-filter: blur(10px);
    }}
    .nav-tab {{
      border: 1px solid var(--line);
      background: #fff7ef;
      color: var(--ink);
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
    }}
    .nav-tab.active {{
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
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
    .stack {{
      display: grid;
      gap: 18px;
    }}
    .panel-section {{
      display: none;
    }}
    .panel-section.active {{
      display: grid;
      gap: 18px;
    }}
    .version-grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      align-items: start;
    }}
    .stat-card {{
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 18px;
      padding: 16px;
      display: grid;
      gap: 8px;
      min-height: 132px;
    }}
    .stat-kicker {{
      font-size: 12px;
      color: var(--muted);
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .stat-value {{
      font-size: 24px;
      font-weight: 800;
      line-height: 1.1;
      word-break: break-word;
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
    input, select {{
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 14px;
      padding: 12px 14px;
      font-size: 15px;
      color: var(--ink);
      outline: none;
    }}
    input:focus, select:focus {{
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
      grid-template-columns: 220px 1fr 1fr;
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
    button:disabled {{
      opacity: 0.55;
      cursor: not-allowed;
      transform: none;
    }}
    form.inline {{
      margin: 0;
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
    .link-list {{
      display: grid;
      gap: 10px;
      margin-top: 12px;
    }}
    .link-item {{
      display: grid;
      gap: 6px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
    }}
    .link-item code {{
      word-break: break-all;
    }}
    .diagnostics {{
      display: grid;
      gap: 14px;
    }}
    .diag-toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin-bottom: 14px;
    }}
    .diag-search {{
      max-width: 320px;
    }}
    .diag-filter {{
      min-width: 180px;
      max-width: 220px;
    }}
    .diag-table-wrap {{
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fff;
    }}
    .card-grid {{
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }}
    .card-grid.three {{
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 880px;
    }}
    th, td {{
      text-align: left;
      padding: 11px 12px;
      border-bottom: 1px solid #efe3d1;
      font-size: 14px;
      vertical-align: top;
    }}
    th {{
      background: #fbf5ea;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .tag {{
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #f3e6d4;
      color: #70431e;
    }}
    .tag.ok {{
      background: #e6f5ea;
      color: #246d3d;
    }}
    .tag.error {{
      background: #fdebea;
      color: #9b3d36;
    }}
    @media (max-width: 760px) {{
      .grid, .source-grid, .version-grid, .card-grid, .card-grid.three {{
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
    <div class="nav-tabs" id="nav-tabs">
      <button class="nav-tab active" type="button" data-section-target="overview">概览</button>
      <button class="nav-tab" type="button" data-section-target="sources">源管理</button>
      <button class="nav-tab" type="button" data-section-target="diagnostics">频道诊断</button>
      <button class="nav-tab" type="button" data-section-target="recordings">EPG 与录制</button>
    </div>
    <div class="stack">
      <div class="panel-section active" data-section="overview">
      <div class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title">版本与更新</div>
            <div class="sub">这里会显示当前版本、镜像来源，以及自动更新和手动更新状态。</div>
          </div>
        </div>
        <div class="panel-body">
          {status_block}
          <div class="version-grid">
            <div class="stat-card">
              <div class="stat-kicker">当前版本</div>
              <div class="stat-value">v{app_version}</div>
              <div class="sub">最新版本：v{latest_version}</div>
            </div>
            <div class="stat-card">
              <div class="stat-kicker">自动更新</div>
              <div class="stat-value" style="font-size:18px;">{auto_update_status}</div>
              <div class="sub">{update_status}</div>
            </div>
            <div class="stat-card">
              <div class="stat-kicker">手动更新</div>
              <div class="actions" style="margin-top:0;">{manual_update_action}</div>
              <div class="sub">{manual_update_hint}</div>
              <div class="sub">镜像：<code>{app_image}</code></div>
            </div>
          </div>
        </div>
      </div>
      </div>

      <div class="panel-section" data-section="sources">
      <div class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title">源地址管理</div>
            <div class="sub">本地输出：<code>/list.m3u</code> / <code>/m3u</code> / <code>/txt</code> / <code>/epg.xml.gz</code></div>
          </div>
        </div>
        <div class="panel-body">
          <div class="panel-title">添加源</div>
          <div class="source-card" style="margin-bottom:16px;">
            <div class="source-grid">
              <div class="field">
                <label for="quick-add-kind">添加方式</label>
                <select id="quick-add-kind">
                  <option value="remote">订阅源地址</option>
                  <option value="local">本地 txt / m3u 文件</option>
                </select>
              </div>
            </div>
            <div class="actions" style="margin-top:12px;">
              <button class="ghost" type="button" id="quick-add-source">添加订阅源</button>
            </div>
            <div class="footer-note">选“订阅源地址”会在下方新增一条可编辑源；选“本地 txt / m3u 文件”会跳到上传表单，上传后会直接加入源列表。</div>
          </div>

          <div class="source-card" style="margin-bottom:16px;">
            <div class="source-grid">
              <div class="field">
                <label for="domain-generator-input">域名一键生成订阅源</label>
                <input id="domain-generator-input" placeholder="例如 tv.example.com 或 https://tv.example.com">
              </div>
              <div class="field">
                <label for="domain-generator-scheme">协议</label>
                <select id="domain-generator-scheme">
                  <option value="https">https</option>
                  <option value="http">http</option>
                </select>
              </div>
              <div class="field">
                <label>&nbsp;</label>
                <button class="ghost" type="button" id="generate-domain-links">一键生成并回填</button>
              </div>
            </div>
            <div class="footer-note">会自动生成 <code>list.m3u</code>、<code>m3u</code>、<code>txt</code>、<code>epg.xml.gz</code> 和后台地址，并把“外部访问地址”回填成你输入的域名。</div>
            <div class="link-list" id="generated-link-list"></div>
          </div>

          <form method="post" action="/admin/upload-source-file" enctype="multipart/form-data">
            <div class="source-card" style="margin-bottom:16px;" id="upload-source-card">
              <div class="source-grid">
                <div class="field">
                  <label for="upload-source-name">本地源名称</label>
                  <input id="upload-source-name" name="source_name" placeholder="例如 本地备份源">
                </div>
                <div class="field">
                  <label for="upload-source-file">上传 txt / m3u 文件</label>
                  <input id="upload-source-file" name="source_file" type="file" accept=".txt,.m3u,.m3u8,text/plain,application/vnd.apple.mpegurl,audio/x-mpegurl">
                </div>
                <div class="field">
                  <label for="upload-source-proxy">代理地址（可选）</label>
                  <input id="upload-source-proxy" name="source_proxy_url" placeholder="http://127.0.0.1:7890">
                </div>
                <div class="field">
                  <label for="upload-source-output-mode">输出方式</label>
                  <select id="upload-source-output-mode" name="source_output_mode">
                    <option value="proxy">代理输出</option>
                    <option value="direct">直连输出</option>
                  </select>
                </div>
              </div>
              <label class="toggle" style="margin-top:12px;">
                <input type="checkbox" name="source_enabled" value="1" checked>
                上传后立即启用这条本地源
              </label>
              <div class="actions" style="margin-top:12px;">
                <button class="ghost" type="submit">上传本地源文件</button>
              </div>
              <div class="footer-note">上传后的文件会保存到服务器配置目录下的 <code>uploads/</code>，并作为本地 M3U/TXT 源参与合并。</div>
            </div>
          </form>

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
            <div class="field full">
              <label for="epg_source_url">节目单地址（可选）</label>
              <input id="epg_source_url" name="epg_source_url" value="{epg_source_url}" placeholder="例如 http://10.10.10.20:30008/all 或 /epg/tvb/tvb_*.xml">
            </div>
            <div class="field full">
              <label for="epg_proxy_url">节目单代理（可选）</label>
              <input id="epg_proxy_url" name="epg_proxy_url" value="{epg_proxy_url}" placeholder="例如 socks5://127.0.0.1:7890">
            </div>
            <div class="field">
              <label for="epg_cache_minutes">节目单缓存（分钟）</label>
              <input id="epg_cache_minutes" name="epg_cache_minutes" value="{epg_cache_minutes}" inputmode="numeric">
            </div>
            <div class="field">
              <label for="epg_cache_dir">节目单缓存目录</label>
              <input id="epg_cache_dir" name="epg_cache_dir" value="{epg_cache_dir}" placeholder="例如 /app/config/cache">
            </div>
            <div class="field full">
              <label for="recordings_dir">录制输出目录</label>
              <input id="recordings_dir" name="recordings_dir" value="{recordings_dir}" placeholder="例如 /app/config/recordings">
            </div>
            <div class="field full">
              <label for="xhs_a1">xhsuhd 的 a1 Cookie</label>
              <input id="xhs_a1" name="xhs_a1" type="password" value="{xhs_a1}" placeholder="小红书 Cookie 里的 a1">
            </div>
            <div class="field full">
              <label for="xhs_web_session">xhsuhd 的 web_session Cookie</label>
              <input id="xhs_web_session" name="xhs_web_session" type="password" value="{xhs_web_session}" placeholder="小红书 Cookie 里的 web_session">
            </div>
          </div>

          <div class="panel-title">源列表</div>
          <div class="sources" id="sources"></div>

          <div class="actions">
            <button class="ghost" type="button" id="add-source">新增一条订阅源</button>
            <form class="inline" method="post" action="/admin/refresh"><button class="ghost" type="submit">立即重新抓源</button></form>
            <button class="primary" type="submit">保存并刷新</button>
            {xhs_apply_action}
          </div>
          <div class="footer-note">{xhs_apply_hint}</div>
        </form>
        </div>
      </div>
      </div>

      <div class="panel-section" data-section="diagnostics">
      <div class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title">频道诊断</div>
            <div class="sub">把抓源状态、输出方式、代理地区和整合频道放到一页里集中排查。</div>
          </div>
        </div>
        <div class="panel-body">
        <div class="panel-title">源抓取诊断</div>
        <div class="sub">这里会直接告诉你每条源有没有抓到、抓到多少频道、耗时多少、是直连还是代理，以及代理出口地区。</div>
        <div class="diagnostics" style="margin-top:14px;">
          <div class="diag-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>源名称</th>
                  <th>状态</th>
                  <th>频道数</th>
                  <th>耗时</th>
                  <th>方式</th>
                  <th>代理目标</th>
                  <th>代理地区</th>
                  <th>最后刷新</th>
                  <th>错误</th>
                </tr>
              </thead>
              <tbody id="source-status-body"></tbody>
            </table>
          </div>
        </div>

        <div class="panel-title" style="margin-top:28px;">整合频道总览</div>
        <div class="sub">这里显示前 {channel_diagnostic_limit} 个频道用于快速诊断；录制候选显示前 {channel_option_limit} 个频道，完整频道仍会输出到本地订阅。</div>
        <div class="diagnostics" style="margin-top:14px;">
          <div class="diag-toolbar">
            <input id="channel-diagnostics-search" class="diag-search" placeholder="搜索频道名 / 分组 / 来源">
            <select id="channel-mode-filter" class="diag-filter">
              <option value="">全部输出方式</option>
              <option value="直连">仅直连</option>
              <option value="代理">仅代理</option>
            </select>
          </div>
          <div class="diag-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>频道</th>
                  <th>归并名</th>
                  <th>分组</th>
                  <th>来源</th>
                  <th>源标识</th>
                  <th>上游主机</th>
                  <th>直连/代理</th>
                  <th>代理地区</th>
                  <th>延迟</th>
                  <th>状态</th>
                </tr>
              </thead>
              <tbody id="channel-status-body"></tbody>
            </table>
          </div>
        </div>
        </div>
      </div>
      </div>

      <div class="panel-section" data-section="recordings">
      <div class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title">EPG 与录制</div>
            <div class="sub">按节目单时间链创建录制任务，并设置提前和延后分钟数。</div>
          </div>
        </div>
        <div class="panel-body">
        <div class="panel-title">节目单录制</div>
        <div class="grid">
          <div class="field full">
            <label for="record-channel">选择频道</label>
            <input id="record-channel" list="record-channel-list" placeholder="输入频道名筛选">
            <datalist id="record-channel-list"></datalist>
          </div>
          <div class="field">
            <label for="record-pre">提前录制（分钟）</label>
            <input id="record-pre" value="3" inputmode="numeric">
          </div>
          <div class="field">
            <label for="record-post">延后结束（分钟）</label>
            <input id="record-post" value="3" inputmode="numeric">
          </div>
        </div>
        <div class="actions" style="margin-top:0;">
          <button class="ghost" type="button" id="load-epg">查看节目时间链</button>
        </div>
        <div class="sources" id="epg-timeline"></div>

        <div class="panel-title" style="margin-top:28px;">已创建录制任务</div>
        <div class="sources" id="recording-list"></div>
        <div class="footer-note">保存后会直接写入服务器配置文件，并重新抓取频道列表。节目单会按缓存策略落盘，`/epg.xml.gz` 适合给播放器长期订阅。</div>
        </div>
      </div>
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
          <label>源类型</label>
          <select data-role="kind">
            <option value="remote">订阅源地址</option>
            <option value="local">本地文件路径</option>
          </select>
        </div>
        <div class="field">
          <label data-role="url-label">m3u 地址</label>
          <input data-role="url" name="source_url" placeholder="https://example.com/live.m3u">
        </div>
        <div class="field">
          <label>代理地址（可选）</label>
          <input data-role="proxy_url" name="source_proxy_url" placeholder="http://127.0.0.1:7890">
        </div>
        <div class="field">
          <label>输出方式</label>
          <select data-role="output_mode" name="source_output_mode">
            <option value="proxy">代理输出</option>
            <option value="direct">直连输出</option>
          </select>
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
    const sourceStatuses = {source_statuses_json};
    const channelStatuses = {channel_statuses_json};
    const channels = {channels_json};
    const recordings = {recordings_json};
    const navTabs = [...document.querySelectorAll(".nav-tab")];
    const panelSections = [...document.querySelectorAll(".panel-section")];
    const container = document.getElementById("sources");
    const template = document.getElementById("source-template");
    const addButton = document.getElementById("add-source");
    const quickAddButton = document.getElementById("quick-add-source");
    const quickAddKind = document.getElementById("quick-add-kind");
    const uploadSourceCard = document.getElementById("upload-source-card");
    const publicBaseUrlInput = document.getElementById("public_base_url");
    const domainGeneratorInput = document.getElementById("domain-generator-input");
    const domainGeneratorScheme = document.getElementById("domain-generator-scheme");
    const generateDomainLinksButton = document.getElementById("generate-domain-links");
    const generatedLinkList = document.getElementById("generated-link-list");
    const channelInput = document.getElementById("record-channel");
    const channelList = document.getElementById("record-channel-list");
    const timeline = document.getElementById("epg-timeline");
    const recordingList = document.getElementById("recording-list");
    const loadEpgButton = document.getElementById("load-epg");
    const sourceStatusBody = document.getElementById("source-status-body");
    const channelStatusBody = document.getElementById("channel-status-body");
    const channelDiagnosticsSearch = document.getElementById("channel-diagnostics-search");
    const channelModeFilter = document.getElementById("channel-mode-filter");

    function showSection(sectionName) {{
      navTabs.forEach((button) => {{
        button.classList.toggle("active", button.dataset.sectionTarget === sectionName);
      }});
      panelSections.forEach((panel) => {{
        panel.classList.toggle("active", panel.dataset.section === sectionName);
      }});
      window.scrollTo({{ top: 0, behavior: "smooth" }});
    }}

    navTabs.forEach((button) => {{
      button.addEventListener("click", () => showSection(button.dataset.sectionTarget));
    }});

    function refreshIndexes() {{
      [...container.querySelectorAll(".source-card")].forEach((card, index) => {{
        card.querySelector(".source-badge").textContent = "Source " + (index + 1);
        card.querySelector('[data-role="enabled"]').value = String(index);
        card.dataset.index = String(index);
      }});
    }}

    function normalizeBaseUrl(rawValue, preferredScheme) {{
      const value = (rawValue || "").trim();
      if (!value) return "";
      if (/^https?:\/\//i.test(value)) {{
        return value.replace(/\/+$/, "");
      }}
      return preferredScheme + "://" + value.replace(/^\/+/, "").replace(/\/+$/, "");
    }}

    function renderGeneratedLinks(baseUrl) {{
      if (!baseUrl) {{
        generatedLinkList.innerHTML = "";
        return;
      }}
      const links = [
        {{ label: "M3U 主订阅", url: baseUrl + "/list.m3u" }},
        {{ label: "M3U 短地址", url: baseUrl + "/m3u" }},
        {{ label: "TXT 订阅", url: baseUrl + "/txt" }},
        {{ label: "EPG 节目单", url: baseUrl + "/epg.xml.gz" }},
        {{ label: "后台地址", url: baseUrl + "/admin" }}
      ];
      generatedLinkList.innerHTML = links.map((item) => `
        <div class="link-item">
          <label>${{item.label}}</label>
          <code>${{item.url}}</code>
        </div>
      `).join("");
    }}

    function applySourceKind(node, kind) {{
      const normalizedKind = kind === "local" ? "local" : "remote";
      const urlLabel = node.querySelector('[data-role="url-label"]');
      const urlInput = node.querySelector('[data-role="url"]');
      const proxyInput = node.querySelector('[data-role="proxy_url"]');
      const outputModeInput = node.querySelector('[data-role="output_mode"]');
      const proxyField = proxyInput.closest(".field");
      node.querySelector('[data-role="kind"]').value = normalizedKind;
      if (normalizedKind === "local") {{
        urlLabel.textContent = "本地 txt / m3u 路径";
        urlInput.placeholder = "/app/config/uploads/local-list.m3u";
        proxyField.style.display = outputModeInput.value === "direct" ? "none" : "";
        if (outputModeInput.value === "direct") {{
          proxyInput.value = "";
        }}
      }} else {{
        urlLabel.textContent = "m3u 地址";
        urlInput.placeholder = "https://example.com/live.m3u";
        proxyField.style.display = outputModeInput.value === "direct" ? "none" : "";
      }}
    }}

    function addSourceRow(source = {{ name: "", source_kind: "remote", url: "", proxy_url: "", output_mode: "proxy", enabled: true }}) {{
      const node = template.content.firstElementChild.cloneNode(true);
      const enabledInput = node.querySelector('[data-role="enabled"]');
      const kindInput = node.querySelector('[data-role="kind"]');
      const outputModeInput = node.querySelector('[data-role="output_mode"]');
      node.querySelector('[data-role="name"]').value = source.name || "";
      node.querySelector('[data-role="url"]').value = source.url || "";
      node.querySelector('[data-role="proxy_url"]').value = source.proxy_url || "";
      outputModeInput.value = source.output_mode === "direct" ? "direct" : "proxy";
      applySourceKind(node, source.source_kind || "remote");
      kindInput.addEventListener("change", () => applySourceKind(node, kindInput.value));
      outputModeInput.addEventListener("change", () => applySourceKind(node, kindInput.value));
      enabledInput.checked = source.enabled !== false;
      enabledInput.dataset.savedChecked = enabledInput.checked ? "true" : "false";
      enabledInput.addEventListener("change", () => toggleSourceEnabled(node, enabledInput));
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
    quickAddButton.addEventListener("click", () => {{
      if (quickAddKind.value === "local") {{
        uploadSourceCard.scrollIntoView({{ behavior: "smooth", block: "center" }});
        const fileInput = document.getElementById("upload-source-file");
        if (fileInput) fileInput.click();
        return;
      }}
      addSourceRow({{ source_kind: "remote", enabled: true }});
      container.lastElementChild?.scrollIntoView({{ behavior: "smooth", block: "center" }});
    }});
    generateDomainLinksButton.addEventListener("click", () => {{
      const baseUrl = normalizeBaseUrl(domainGeneratorInput.value, domainGeneratorScheme.value);
      if (!baseUrl) {{
        alert("先输入域名再生成");
        return;
      }}
      publicBaseUrlInput.value = baseUrl;
      renderGeneratedLinks(baseUrl);
      publicBaseUrlInput.scrollIntoView({{ behavior: "smooth", block: "center" }});
    }});
    renderGeneratedLinks(normalizeBaseUrl(publicBaseUrlInput.value, domainGeneratorScheme.value));

    async function toggleSourceEnabled(card, input) {{
      const index = Number(card.dataset.index || input.value);
      const savedChecked = input.dataset.savedChecked === "true";
      input.disabled = true;
      const badge = card.querySelector(".source-badge");
      const previousBadge = badge.textContent;
      badge.textContent = previousBadge + " · 保存中";
      try {{
        const response = await fetch("/admin/sources/toggle", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ index, enabled: input.checked }})
        }});
        if (!response.ok) {{
          throw new Error(await response.text());
        }}
        const payload = await response.json();
        input.dataset.savedChecked = payload.enabled ? "true" : "false";
        badge.textContent = "Source " + (index + 1) + " · 已保存";
        setTimeout(() => refreshIndexes(), 1200);
      }} catch (error) {{
        input.checked = savedChecked;
        badge.textContent = "Source " + (index + 1) + " · 保存失败";
        alert(error.message || "保存失败");
        setTimeout(() => refreshIndexes(), 1600);
      }} finally {{
        input.disabled = false;
      }}
    }}

    function statusTag(label) {{
      if (label === "正常") return '<span class="tag ok">' + label + '</span>';
      if (label === "抓取失败") return '<span class="tag error">' + label + '</span>';
      return '<span class="tag">' + label + '</span>';
    }}

    function renderSourceStatuses() {{
      sourceStatusBody.innerHTML = "";
      sourceStatuses.forEach((item) => {{
        const row = document.createElement("tr");
        row.innerHTML =
          "<td>" + item.name + "</td>" +
          "<td>" + statusTag(item.last_status) + "</td>" +
          "<td>" + item.channel_count + "</td>" +
          "<td>" + (item.refresh_latency_ms ? item.refresh_latency_ms + " ms" : "-") + "</td>" +
          "<td>" + item.access_mode + "</td>" +
          "<td>" + item.proxy_target + "</td>" +
          "<td>" + item.proxy_region + "</td>" +
          "<td>" + item.last_refresh_at + "</td>" +
          "<td>" + (item.last_error || "-") + "</td>";
        sourceStatusBody.appendChild(row);
      }});
    }}

    function renderChannelStatuses(keyword = "", mode = "") {{
      const normalized = keyword.trim().toLowerCase();
      channelStatusBody.innerHTML = "";
      channelStatuses
        .filter((item) => {{
          if (mode && item.access_mode !== mode) return false;
          if (!normalized) return true;
          return [
            item.name,
            item.normalized_name,
            item.group,
            item.source_name,
            item.source_slug,
            item.upstream_host
          ].join(" ").toLowerCase().includes(normalized);
        }})
        .forEach((item) => {{
          const row = document.createElement("tr");
          row.innerHTML =
            "<td>" + item.name + "</td>" +
            "<td>" + item.normalized_name + "</td>" +
            "<td>" + item.group + "</td>" +
            "<td>" + item.source_name + "</td>" +
            "<td>" + item.source_slug + "</td>" +
            "<td>" + item.upstream_host + "</td>" +
            "<td>" + item.access_mode + "</td>" +
            "<td>" + item.proxy_region + "</td>" +
            "<td>" + (item.refresh_latency_ms ? item.refresh_latency_ms + " ms" : "-") + "</td>" +
            "<td>" + statusTag(item.last_status) + "</td>";
          channelStatusBody.appendChild(row);
        }});
    }}

    renderSourceStatuses();
    renderChannelStatuses();
    function rerenderChannelDiagnostics() {{
      renderChannelStatuses(channelDiagnosticsSearch.value, channelModeFilter.value);
    }}
    channelDiagnosticsSearch.addEventListener("input", rerenderChannelDiagnostics);
    channelModeFilter.addEventListener("change", rerenderChannelDiagnostics);

    channels.forEach((channel) => {{
      const option = document.createElement("option");
      option.value = channel.id;
      option.label = channel.label;
      channelList.appendChild(option);
    }});

    function findChannelByInput() {{
      const value = channelInput.value.trim();
      return channels.find((item) => item.id === value || item.label === value || item.name === value);
    }}

    function createRecording(programme, channel) {{
      const pre = Number(document.getElementById("record-pre").value || "0");
      const post = Number(document.getElementById("record-post").value || "0");
      const form = document.createElement("form");
      form.method = "post";
      form.action = "/admin/recordings/create";
      const fields = {{
        channel_id: channel.id,
        channel_name: channel.name,
        program_title: programme.title,
        start_at: programme.start_at,
        end_at: programme.end_at,
        pre_minutes: String(Math.max(0, pre)),
        post_minutes: String(Math.max(0, post)),
      }};
      for (const [key, value] of Object.entries(fields)) {{
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = key;
        input.value = value;
        form.appendChild(input);
      }}
      document.body.appendChild(form);
      form.submit();
    }}

    function renderTimeline(programmes, channel) {{
      timeline.innerHTML = "";
      if (!programmes.length) {{
        timeline.innerHTML = '<div class="source-card"><div class="sub">当前频道没有匹配到节目单时间链。</div></div>';
        return;
      }}
      programmes.forEach((programme) => {{
        const card = document.createElement("div");
        card.className = "source-card";
        const start = new Date(programme.start_at);
        const end = new Date(programme.end_at);
        card.innerHTML = `
          <div class="source-top">
            <div>
              <div class="source-badge">${{channel.name}}</div>
              <div style="font-weight:700;font-size:16px;">${{programme.title}}</div>
              <div class="sub">${{start.toLocaleString()}} - ${{end.toLocaleString()}}</div>
            </div>
            <button class="primary" type="button">加入录制</button>
          </div>
        `;
        card.querySelector("button").addEventListener("click", () => createRecording(programme, channel));
        timeline.appendChild(card);
      }});
    }}

    function renderRecordings() {{
      recordingList.innerHTML = "";
      if (!recordings.length) {{
        recordingList.innerHTML = '<div class="source-card"><div class="sub">暂时还没有录制任务。</div></div>';
        return;
      }}
      recordings.forEach((task) => {{
        const card = document.createElement("div");
        card.className = "source-card";
        const errorBlock = task.last_error ? `<div class="sub" style="color:#9c3d34;">${{task.last_error}}</div>` : "";
        const outputBlock = task.output_file ? `<div class="sub">输出文件：${{task.output_file}}</div>` : "";
        card.innerHTML = `
          <div class="source-top">
            <div>
              <div class="source-badge">${{task.status}}</div>
              <div style="font-weight:700;font-size:16px;">${{task.program_title}}</div>
              <div class="sub">${{task.channel_name}} | ${{new Date(task.start_at).toLocaleString()}} - ${{new Date(task.end_at).toLocaleString()}}</div>
              <div class="sub">提前 ${{task.pre_minutes}} 分钟，延后 ${{task.post_minutes}} 分钟</div>
              ${{outputBlock}}
              ${{errorBlock}}
            </div>
            <form method="post" action="/admin/recordings/delete">
              <input type="hidden" name="id" value="${{task.id}}">
              <button class="danger" type="submit">删除</button>
            </form>
          </div>
        `;
        recordingList.appendChild(card);
      }});
    }}

    loadEpgButton.addEventListener("click", async () => {{
      const channel = findChannelByInput();
      if (!channel) {{
        timeline.innerHTML = '<div class="source-card"><div class="sub">先从频道列表里选一个有效频道。</div></div>';
        return;
      }}
      timeline.innerHTML = '<div class="source-card"><div class="sub">正在读取节目单时间链...</div></div>';
      const response = await fetch('/admin/epg/programmes?channel_id=' + encodeURIComponent(channel.id));
      if (!response.ok) {{
        timeline.innerHTML = '<div class="source-card"><div class="sub">节目单读取失败，请先确认 `/epg.xml` 可用。</div></div>';
        return;
      }}
      const programmes = await response.json();
      renderTimeline(programmes, channel);
    }});

    renderRecordings();
  </script>
</body>
</html>"#,
        status_block = status_block,
        app_version = escape_html(&data.app_version),
        latest_version = escape_html(&data.latest_version),
        update_status = escape_html(&data.update_status),
        app_image = escape_html(&data.app_image),
        auto_update_status = escape_html(&data.auto_update_status),
        manual_update_action = manual_update_action,
        manual_update_hint = escape_html(&data.manual_update_hint),
        xhs_a1 = escape_html(&data.xhs_a1),
        xhs_web_session = escape_html(&data.xhs_web_session),
        xhs_apply_action = xhs_apply_action,
        xhs_apply_hint = escape_html(&data.xhs_apply_hint),
        public_base_url = escape_html(&data.public_base_url),
        refresh_minutes = escape_html(&data.refresh_minutes),
        user_agent = escape_html(&data.user_agent),
        epg_source_url = escape_html(&data.epg_source_url),
        epg_proxy_url = escape_html(&data.epg_proxy_url),
        epg_cache_minutes = escape_html(&data.epg_cache_minutes),
        epg_cache_dir = escape_html(&data.epg_cache_dir),
        recordings_dir = escape_html(&data.recordings_dir),
        signing_secret = escape_html(&data.signing_secret),
        sources_json = sources_json,
        source_statuses_json = data.source_statuses_json,
        channel_statuses_json = data.channel_statuses_json,
        channels_json = data.channels_json,
        recordings_json = data.recordings_json,
        channel_diagnostic_limit = ADMIN_CHANNEL_DIAGNOSTIC_LIMIT,
        channel_option_limit = ADMIN_CHANNEL_OPTION_LIMIT
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
