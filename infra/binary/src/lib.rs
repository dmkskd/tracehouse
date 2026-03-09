use axum::{
    body::Body,
    extract::{Request, State},
    http::{self, HeaderMap, HeaderValue, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../frontend/dist/single/"]
struct Assets;

#[derive(Clone)]
struct AppState {
    insecure: bool,
}

// ---------------------------------------------------------------------------
// Static file serving (embedded frontend)
// ---------------------------------------------------------------------------

async fn index_handler() -> impl IntoResponse {
    serve_embedded("index.html")
}

async fn static_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        return serve_embedded("index.html");
    }
    serve_embedded(path)
}

fn serve_embedded(path: &str) -> Response {
    match Assets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .as_ref()
                .to_string();
            Response::builder()
                .status(StatusCode::OK)
                .header(http::header::CONTENT_TYPE, mime)
                .body(Body::from(content.data))
                .unwrap()
        }
        None => serve_embedded("index.html"),
    }
}

// ---------------------------------------------------------------------------
// Health / ping
// ---------------------------------------------------------------------------

async fn ping_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"ok"}"#,
    )
}

async fn health_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(http::header::CONTENT_TYPE, "application/json")],
        r#"{"status":"ok","service":"tracehouse"}"#,
    )
}

// ---------------------------------------------------------------------------
// CORS proxy → ClickHouse
// ---------------------------------------------------------------------------

fn get_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

async fn proxy_handler(State(state): State<AppState>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let headers = &parts.headers;

    // Build target URL from x-ch-* headers
    let host = match get_header(headers, "x-ch-host") {
        Some(h) => h,
        None => {
            return (StatusCode::BAD_REQUEST, "Missing x-ch-host header").into_response();
        }
    };
    let port = get_header(headers, "x-ch-port").unwrap_or_else(|| "8443".into());
    let secure = get_header(headers, "x-ch-secure")
        .map(|v| v != "false")
        .unwrap_or(true);
    let scheme = if secure { "https" } else { "http" };

    // Extract query string, rewrite format → default_format
    let query_string = parts.uri.query().unwrap_or("");
    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(query_string.as_bytes())
        .into_owned()
        .collect();
    for pair in &mut pairs {
        if pair.0 == "format" {
            pair.0 = "default_format".to_string();
        }
    }
    let new_qs: String = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(&pairs)
        .finish();

    let target_url = if new_qs.is_empty() {
        format!("{scheme}://{host}:{port}/")
    } else {
        format!("{scheme}://{host}:{port}/?{new_qs}")
    };

    // Build outgoing request
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(state.insecure)
        .build()
        .unwrap();

    let ch_user = get_header(headers, "x-ch-user").unwrap_or_else(|| "default".into());
    let ch_password = get_header(headers, "x-ch-password").unwrap_or_default();
    let ch_database = get_header(headers, "x-ch-database").unwrap_or_else(|| "default".into());

    // Read the body
    let body_bytes = match axum::body::to_bytes(body, 100 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("Failed to read body: {e}")).into_response();
        }
    };

    let method = match parts.method {
        http::Method::POST => reqwest::Method::POST,
        http::Method::GET => reqwest::Method::GET,
        _ => reqwest::Method::POST,
    };

    let result = client
        .request(method, &target_url)
        .header("X-ClickHouse-User", &ch_user)
        .header("X-ClickHouse-Key", &ch_password)
        .header("X-ClickHouse-Database", &ch_database)
        .body(body_bytes)
        .send()
        .await;

    match result {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let mut builder = Response::builder().status(status);

            // Forward content-type from ClickHouse
            if let Some(ct) = resp.headers().get("content-type") {
                builder = builder.header(http::header::CONTENT_TYPE, ct);
            }

            let resp_bytes = resp.bytes().await.unwrap_or_default();
            builder.body(Body::from(resp_bytes)).unwrap()
        }
        Err(e) => {
            let msg = format!(r#"{{"error":"{}"}}"#, e);
            (StatusCode::BAD_GATEWAY, [(http::header::CONTENT_TYPE, "application/json")], msg)
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// CORS layer
// ---------------------------------------------------------------------------

async fn cors_middleware(req: Request, next: axum::middleware::Next) -> Response {
    if req.method() == http::Method::OPTIONS {
        return Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(http::header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
            .header(http::header::ACCESS_CONTROL_ALLOW_HEADERS, "*")
            .header(http::header::ACCESS_CONTROL_MAX_AGE, "86400")
            .body(Body::empty())
            .unwrap();
    }

    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(http::header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(http::header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST, OPTIONS"));
    headers.insert(http::header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
    response
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Options for building the router.
#[derive(Clone, Default)]
pub struct RouterOptions {
    /// Skip TLS certificate verification for upstream ClickHouse connections.
    pub insecure: bool,
}

pub fn build_router() -> Router {
    build_router_with(RouterOptions::default())
}

pub fn build_router_with(opts: RouterOptions) -> Router {
    let state = AppState {
        insecure: opts.insecure,
    };

    Router::new()
        .route("/proxy/ping", get(ping_handler))
        .route("/proxy/query", post(proxy_handler).get(proxy_handler))
        .route("/proxy/command", post(proxy_handler))
        .route("/health", get(health_handler))
        .route("/", get(index_handler))
        .fallback(static_handler)
        .layer(axum::middleware::from_fn(cors_middleware))
        .with_state(state)
}
