use std::{
    env,
    net::{IpAddr, TcpListener},
    os::unix::fs::PermissionsExt,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::{ws::Message, ws::WebSocket, State, WebSocketUpgrade},
    http::Uri,
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures::{sink::SinkExt, stream::StreamExt};
use reqwest;
use serde_json::Value;
use sha2::{Digest, Sha224};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Command,
    time::{sleep, timeout},
};
use tracing::Level;
use tracing_subscriber::{FmtSubscriber, filter::LevelFilter};

const FAKE_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Alex's Tech Blog - 记录技术与生活</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: #f5f5f5; color: #333; line-height: 1.6; }
        header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem 0; text-align: center; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        header h1 { font-size: 2rem; margin-bottom: 0.5rem; }
        header p { opacity: 0.9; font-size: 1.1rem; }
        nav { background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.05); position: sticky; top: 0; z-index: 100; }
        nav ul { list-style: none; display: flex; justify-content: center; flex-wrap: wrap; max-width: 800px; margin: 0 auto; }
        nav li { margin: 0; }
        nav a { display: block; padding: 1rem 1.5rem; text-decoration: none; color: #555; font-weight: 500; transition: color 0.3s; }
        nav a:hover { color: #667eea; }
        .container { max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
        .post { background: white; border-radius: 12px; padding: 2rem; margin-bottom: 2rem; box-shadow: 0 2px 15px rgba(0,0,0,0.05); transition: transform 0.2s; }
        .post:hover { transform: translateY(-3px); box-shadow: 0 5px 25px rgba(0,0,0,0.1); }
        .post h2 { color: #333; margin-bottom: 0.5rem; font-size: 1.5rem; }
        .meta { color: #999; font-size: 0.9rem; margin-bottom: 1rem; }
        .post p { color: #555; margin-bottom: 1rem; }
        .tag { display: inline-block; background: #f0f0f0; color: #666; padding: 0.2rem 0.8rem; border-radius: 20px; font-size: 0.85rem; margin-right: 0.5rem; }
        footer { text-align: center; padding: 2rem; color: #999; font-size: 0.9rem; margin-top: 3rem; border-top: 1px solid #eee; }
        @media (max-width: 600px) { header h1 { font-size: 1.5rem; } .post { padding: 1.5rem; } }
    </style>
</head>
<body>
    <header>
        <h1>Alex's Tech Blog</h1>
        <p>分享编程、开源与数码生活</p>
    </header>
    <nav>
        <ul>
            <li><a href="/">首页</a></li>
            <li><a href="/">文章</a></li>
            <li><a href="/">项目</a></li>
            <li><a href="/">关于</a></li>
        </ul>
    </nav>
    <div class="container">
        <article class="post">
            <h2>使用 Python 构建高性能异步服务</h2>
            <div class="meta">2026-08-10 · 阅读 2,341 · Python</div>
            <p>在现代 Web 开发中，异步编程已经成为提升并发能力的关键技术。本文将深入探讨 asyncio 与 aiohttp 的最佳实践...</p>
            <span class="tag">Python</span>
            <span class="tag">Async</span>
            <span class="tag">Backend</span>
        </article>
        <article class="post">
            <h2>我的 Homelab 搭建日记：从 0 到 All-in-One</h2>
            <div class="meta">2026-07-28 · 阅读 4,128 · 数码</div>
            <p>最近把家里闲置的 NUC 改造成了 All-in-One 服务器，跑了 Docker、NAS、智能家居中枢...</p>
            <span class="tag">Homelab</span>
            <span class="tag">Docker</span>
            <span class="tag">NAS</span>
        </article>
        <article class="post">
            <h2>Git 工作流进阶：Rebase 还是 Merge？</h2>
            <div class="meta">2026-07-15 · 阅读 1,892 · 工具</div>
            <p>团队协作中，分支管理策略往往决定了代码历史的整洁程度。今天我们来聊聊什么时候该用 rebase...</p>
            <span class="tag">Git</span>
            <span class="tag">DevOps</span>
        </article>
    </div>
    <footer>
        <p> Alex's Tech Blog · Powered by Rust & Love</p>
    </footer>
</body>
</html>"#;

const BLOCKED_DOMAINS: &[&str] = &[
    "speedtest.net", "fast.com", "speedtest.cn", "speed.cloudflare.com", "speedof.me",
    "testmy.net", "bandwidth.place", "speed.io", "librespeed.org", "speedcheck.org",
];

struct Config {
    uuid: String,
    domain: String,
    sub_path: String,
    name: String,
    ws_path: String,
    port: u16,
    auto_access: bool,
    debug: bool,
    cloudflared_token: String,
}

impl Config {
    fn from_env() -> Self {
        let uuid = env::var("UUID").unwrap_or_else(|_| "4bda47ec-5ca6-42ff-a225-7861f492a71f".to_string());
        let domain = env::var("DOMAIN").unwrap_or_else(|_| "temalix.cnav.cn.eu.org".to_string());
        let sub_path = env::var("SUB_PATH").unwrap_or_else(|_| "hello-word".to_string());
        let name = env::var("NAME").unwrap_or_else(|_| "temalix".to_string());
        let ws_path = env::var("WSPATH").unwrap_or_else(|_| uuid[..8.min(uuid.len())].to_string());

        let mut port = 3000u16;
        for key in ["SERVER_PORT", "PORT"] {
            if let Ok(v) = env::var(key) {
                let v = v.trim();
                if !v.is_empty() && v != "0" {
                    if let Ok(p) = v.parse::<u16>() {
                        if p > 0 {
                            port = p;
                            break;
                        }
                    }
                }
            }
        }

        let auto_access = env::var("AUTO_ACCESS").unwrap_or_default().to_lowercase() == "true";
        let debug = env::var("DEBUG").unwrap_or_default().to_lowercase() == "true";
        let cloudflared_token = env::var("CLOUDFLARED_TOKEN").unwrap_or_else(|_| "eyJhIjoiZDZlNGIzNDY3N2MzNjljOTViODM3YTcxNWFjZWNjYzciLCJ0IjoiODQ3ODAyZTktYzMzZS00YWQ2LTllMzYtZjMwZTA5N2Y5MThmIiwicyI6IlltWTRaakUzWVRjdFl6aGpZeTAwWkRnNExUZzBOelF0TURVM09UVmhaVFJqTmpGayJ9".to_string());

        Self { uuid, domain, sub_path, name, ws_path, port, auto_access, debug, cloudflared_token }
    }
}

struct AppState {
    config: Config,
    current_domain: std::sync::Mutex<String>,
    current_port: std::sync::Mutex<u16>,
    tls: std::sync::Mutex<String>,
    isp: std::sync::Mutex<String>,
}

impl AppState {
    fn new(config: Config) -> Self {
        let domain = config.domain.clone();
        let (tls, port) = if domain.is_empty() || domain == "your-domain.com" {
            ("none".to_string(), config.port)
        } else {
            ("tls".to_string(), 443)
        };
        Self {
            config,
            current_domain: std::sync::Mutex::new(domain.clone()),
            current_port: std::sync::Mutex::new(port),
            tls: std::sync::Mutex::new(tls),
            isp: std::sync::Mutex::new("Unknown".to_string()),
        }
    }
}

fn is_port_available(port: u16) -> bool {
    TcpListener::bind(("0.0.0.0", port)).is_ok()
}

fn find_available_port(start: u16) -> Option<u16> {
    for p in start..=65535 {
        if is_port_available(p) {
            return Some(p);
        }
    }
    None
}

fn is_blocked_domain(host: &str) -> bool {
    let host_lower = host.to_lowercase();
    BLOCKED_DOMAINS.iter().any(|blocked| {
        host_lower == *blocked || host_lower.ends_with(&format!(".{}", blocked))
    })
}

async fn resolve_host(host: &str) -> String {
    if host.parse::<IpAddr>().is_ok() {
        return host.to_string();
    }
    let client = reqwest::Client::new();
    let url = format!("https://dns.google/resolve?name={}&type=A", host);
    if let Ok(Ok(resp)) = timeout(Duration::from_secs(5), client.get(&url).send()).await {
        if resp.status().is_success() {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(answers) = json.get("Answer").and_then(|a| a.as_array()) {
                    for ans in answers {
                        if ans.get("type").and_then(|t| t.as_i64()) == Some(1) {
                            if let Some(ip) = ans.get("data").and_then(|d| d.as_str()) {
                                return ip.to_string();
                            }
                        }
                    }
                }
            }
        }
    }
    host.to_string()
}

async fn get_isp() -> String {
    let client = reqwest::Client::new();
    if let Ok(Ok(resp)) = timeout(Duration::from_secs(3), client.get("https://api.ip.sb/geoip")
        .header("User-Agent", "Mozilla/5.0").send()).await
    {
        if resp.status().is_success() {
            if let Ok(json) = resp.json::<Value>().await {
                let cc = json.get("country_code").and_then(|v| v.as_str()).unwrap_or("");
                let isp = json.get("isp").and_then(|v| v.as_str()).unwrap_or("");
                return format!("{}-{}", cc, isp.replace(' ', "_"));
            }
        }
    }
    if let Ok(Ok(resp)) = timeout(Duration::from_secs(3), client.get("http://ip-api.com/json")
        .header("User-Agent", "Mozilla/5.0").send()).await
    {
        if resp.status().is_success() {
            if let Ok(json) = resp.json::<Value>().await {
                let cc = json.get("countryCode").and_then(|v| v.as_str()).unwrap_or("");
                let org = json.get("org").and_then(|v| v.as_str()).unwrap_or("");
                return format!("{}-{}", cc, org.replace(' ', "_"));
            }
        }
    }
    "Unknown".to_string()
}

async fn get_ip_info(config: &Config) -> (String, String, u16) {
    if config.domain.is_empty() || config.domain == "your-domain.com" {
        let client = reqwest::Client::new();
        if let Ok(Ok(resp)) = timeout(Duration::from_secs(5), client.get("https://api-ipv4.ip.sb/ip").send()).await {
            if resp.status().is_success() {
                if let Ok(ip) = resp.text().await {
                    let ip = ip.trim().to_string();
                    return (ip, "none".to_string(), config.port);
                }
            }
        }
        ("change-your-domain.com".to_string(), "tls".to_string(), 443)
    } else {
        (config.domain.clone(), "tls".to_string(), 443)
    }
}

fn parse_trojan_request(data: &[u8], uuid: &str) -> Option<(String, u16, usize)> {
    if data.len() < 58 {
        return None;
    }
    let received_hash = std::str::from_utf8(&data[..56]).ok()?;

    let mut hasher1 = Sha224::new();
    hasher1.update(uuid.replace('-', "").as_bytes());
    let expected1 = hex::encode(hasher1.finalize());

    let mut hasher2 = Sha224::new();
    hasher2.update(uuid.as_bytes());
    let expected2 = hex::encode(hasher2.finalize());

    if received_hash != expected1 && received_hash != expected2 {
        return None;
    }

    let mut offset = 56usize;
    if data.len() >= offset + 2 && &data[offset..offset + 2] == b"\r\n" {
        offset += 2;
    }
    if offset >= data.len() || data[offset] != 1 {
        return None;
    }
    offset += 1;
    if offset >= data.len() {
        return None;
    }
    let atyp = data[offset];
    offset += 1;

    let (host, new_offset) = match atyp {
        1 => {
            if offset + 4 > data.len() { return None; }
            let ip = format!("{}.{}.{}.{}", data[offset], data[offset + 1], data[offset + 2], data[offset + 3]);
            (ip, offset + 4)
        }
        3 => {
            if offset >= data.len() { return None; }
            let len = data[offset] as usize;
            offset += 1;
            if offset + len > data.len() { return None; }
            let domain = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
            (domain, offset + len)
        }
        4 => {
            if offset + 16 > data.len() { return None; }
            let mut ip = String::new();
            for i in 0..8 {
                if i > 0 { ip.push(':'); }
                ip.push_str(&format!("{:02x}{:02x}", data[offset + i * 2], data[offset + i * 2 + 1]));
            }
            (ip, offset + 16)
        }
        _ => return None,
    };
    offset = new_offset;
    if offset + 2 > data.len() {
        return None;
    }
    let port = u16::from_be_bytes([data[offset], data[offset + 1]]);
    offset += 2;
    if data.len() >= offset + 2 && &data[offset..offset + 2] == b"\r\n" {
        offset += 2;
    }
    Some((host, port, offset))
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = socket.split();
    let first_msg = match timeout(Duration::from_secs(5), receiver.next()).await {
        Ok(Some(Ok(Message::Binary(data)))) => data,
        _ => return,
    };
    let (host, port, rest_offset) = match parse_trojan_request(&first_msg, &state.config.uuid) {
        Some(v) => v,
        None => return,
    };
    if is_blocked_domain(&host) {
        return;
    }
    let resolved = resolve_host(&host).await;
    let mut tcp = match TcpStream::connect((resolved.as_str(), port)).await {
        Ok(s) => s,
        Err(_) => return,
    };
    if rest_offset < first_msg.len() {
        let _ = tcp.write_all(&first_msg[rest_offset..]).await;
    }
    let (mut tcp_read, mut tcp_write) = tcp.split();
    let ws_to_tcp = async {
        while let Ok(Some(Ok(msg))) = timeout(Duration::from_secs(300), receiver.next()).await {
            if let Message::Binary(data) = msg {
                if tcp_write.write_all(&data).await.is_err() { break; }
            }
        }
    };
    let tcp_to_ws = async {
        let mut buf = vec![0u8; 4096];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if sender.send(Message::Binary(buf[..n].to_vec())).await.is_err() { break; }
                }
                Err(_) => break,
            }
        }
    };
    let _ = tokio::join!(ws_to_tcp, tcp_to_ws);
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    uri: Uri,
) -> Response {
    let path = uri.path();
    if !path.contains(&state.config.ws_path) {
        return (axum::http::StatusCode::NOT_FOUND, "Not Found\n").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn http_handler(State(state): State<Arc<AppState>>, uri: Uri) -> impl IntoResponse {
    let path = uri.path();
    if path == "/" || path == "/index.html" {
        if let Ok(content) = tokio::fs::read_to_string("index.html").await {
            return Html(content).into_response();
        }
        return Html(FAKE_HTML).into_response();
    }
    if path == format!("/{}", state.config.sub_path) {
        let isp = get_isp().await;
        *state.isp.lock().unwrap() = isp.clone();
        let (domain, tls, port) = get_ip_info(&state.config).await;
        *state.current_domain.lock().unwrap() = domain.clone();
        *state.tls.lock().unwrap() = tls.clone();
        *state.current_port.lock().unwrap() = port;
        let name_part = if state.config.name.is_empty() { isp } else { format!("{}-{}", state.config.name, isp) };
        let tls_param = if tls == "tls" { "tls" } else { "none" };
        let trojan_url = format!(
            "trojan://{}@{}:{}?security={}&sni={}&fp=chrome&type=ws&host={}&path=%2F{}#{}",
            state.config.uuid, domain, port, tls_param, domain, domain, state.config.ws_path, name_part
        );
        let sub = BASE64.encode(trojan_url) + "\n";
        return sub.into_response();
    }
    (axum::http::StatusCode::NOT_FOUND, "Not Found\n").into_response()
}

fn get_cloudflared_url() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64",
        _ => "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64",
    }
}

async fn is_cloudflared_running() -> bool {
    let output = match Command::new("ps").arg("aux").output().await {
        Ok(o) => o,
        Err(e) => {
            println!("DEBUG - ps command failed: {}", e);
            return false;
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let running = stdout.contains("./cloudflared") && stdout.contains("tunnel");
    if running {
        println!("DEBUG - detected existing cloudflared process");
    }
    running
}

async fn run_cloudflared(token: String) {
    if token.is_empty() {
        println!("INFO - CLOUDFLARED_TOKEN is empty, skip tunnel");
        return;
    }
    if is_cloudflared_running().await {
        println!("INFO - cloudflared already running, skip");
        return;
    }

    // 如果本地已有 cloudflared 文件，直接使用
    if fs::metadata("cloudflared").await.is_ok() {
        println!("INFO - found local cloudflared binary, using it");
    } else {
        // 多镜像源回退下载
        let urls = vec![
            get_cloudflared_url().to_string(),
            format!("https://gh-proxy.org/{}", get_cloudflared_url()),
            format!("https://ghps.cc/{}", get_cloudflared_url()),
            format!("https://ghproxy.net/{}", get_cloudflared_url()),
            format!("https://mirror.ghproxy.com/{}", get_cloudflared_url()),
        ];

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let mut downloaded = false;
        for (i, url) in urls.iter().enumerate() {
            println!("INFO - downloading cloudflared from mirror {}...", i + 1);
            match client.get(url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    match resp.bytes().await {
                        Ok(bytes) => {
                            if let Err(e) = fs::write("cloudflared", &bytes).await {
                                println!("ERROR - failed to write cloudflared: {}", e);
                                continue;
                            }
                            if let Ok(metadata) = fs::metadata("cloudflared").await {
                                let mut perms = metadata.permissions();
                                perms.set_mode(0o755);
                                let _ = fs::set_permissions("cloudflared", perms).await;
                            }
                            println!("INFO - cloudflared downloaded ({} bytes) from mirror {}", bytes.len(), i + 1);
                            downloaded = true;
                            break;
                        }
                        Err(e) => {
                            println!("ERROR - failed to read bytes from mirror {}: {}", i + 1, e);
                        }
                    }
                }
                Ok(resp) => {
                    println!("ERROR - mirror {} returned status: {}", i + 1, resp.status());
                }
                Err(e) => {
                    println!("ERROR - mirror {} failed: {}", i + 1, e);
                }
            }
        }

        if !downloaded {
            println!("ERROR - all download mirrors failed, cloudflared not available");
            return;
        }
    }

    let cmd = format!("./cloudflared tunnel --no-autoupdate run --token {} >/dev/null 2>&1 &", token);
    println!("INFO - starting cloudflared tunnel...");

    match Command::new("sh").arg("-c").arg(&cmd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => {
            println!("INFO - cloudflared spawned (pid: {:?})", child.id());
        }
        Err(e) => {
            println!("ERROR - failed to spawn cloudflared: {}", e);
            return;
        }
    }

    sleep(Duration::from_secs(5)).await;

    if is_cloudflared_running().await {
        println!("INFO - ✅ cloudflared tunnel is running");
        if let Err(e) = fs::remove_file("cloudflared").await {
            println!("WARN - failed to remove cloudflared binary: {}", e);
        } else {
            println!("INFO - cloudflared binary removed (running in memory)");
        }
    } else {
        println!("ERROR - cloudflared process not found after start, keeping binary for debug");
    }
}

async fn add_access_task(domain: String, sub_path: String) {
    if domain.is_empty() { return; }
    let full_url = format!("https://{}/{}", domain, sub_path);
    let client = reqwest::Client::new();
    let _ = client.post("https://oooo.serv00.net/add-url")
        .json(&serde_json::json!({"url": full_url}))
        .header("Content-Type", "application/json")
        .send().await;
}

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    if config.debug {
        let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();
        tracing::subscriber::set_global_default(subscriber).ok();
    } else {
        let subscriber = FmtSubscriber::builder().with_max_level(LevelFilter::OFF).finish();
        tracing::subscriber::set_global_default(subscriber).ok();
    }

    let mut actual_port = config.port;
    if !is_port_available(actual_port) {
        if let Some(p) = find_available_port(actual_port + 1) {
            actual_port = p;
        } else {
            println!("ERROR - No available ports found");
            std::process::exit(1);
        }
    }

    let state = Arc::new(AppState::new(config));
    let app = Router::new()
        .route("/", get(http_handler))
        .route(&format!("/{}", state.config.sub_path), get(http_handler))
        .route(&format!("/{}", state.config.ws_path), get(ws_handler))
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", actual_port).parse::<std::net::SocketAddr>().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("INFO - ✅ server is running on port {}", actual_port);

    let cf_token = state.config.cloudflared_token.clone();
    tokio::spawn(run_cloudflared(cf_token));

    tokio::spawn(async {
        sleep(Duration::from_secs(180)).await;
        let _ = fs::remove_file("cloudflared").await;
    });

    let domain = state.config.domain.clone();
    let sub_path = state.config.sub_path.clone();
    tokio::spawn(add_access_task(domain, sub_path));

    axum::serve(listener, app).await.unwrap();
}
