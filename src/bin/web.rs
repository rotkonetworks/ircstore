use ircstore::{IrcEvent, S2Store};
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade, ws::Message},
    response::{Html, IntoResponse},
    routing::get,
    Router,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(short, long, env = "WEB_PORT", default_value = "8080")]
    port: u16,

    #[arg(short, long, env = "WEB_BIND", default_value = "127.0.0.1")]
    bind: String,

    #[arg(long, env = "WEB_ALLOW_CHANNELS", value_delimiter = ',')]
    allow: Option<Vec<String>>,

    #[arg(long, env = "WEB_DENY_CHANNELS", value_delimiter = ',')]
    deny: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct LogQuery {
    limit: Option<usize>,
    offset: Option<u64>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<S2Store>,
    allowed: Arc<dyn Fn(&str) -> bool + Send + Sync>,
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../../static/index.html"))
}

async fn help() -> Html<&'static str> {
    Html(include_str!("../../static/help.html"))
}

async fn channel_logs(
    Path(channel): Path<String>,
    Query(params): Query<LogQuery>,
    State(state): State<AppState>,
) -> Result<Json<Vec<IrcEvent>>, String> {
    if !(state.allowed)(&channel) {
        return Err("Forbidden".to_string());
    }

    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);

    state.store.read(&channel, offset, limit).await
        .map(Json)
        .map_err(|e| e.to_string())
}

async fn list_channels(State(state): State<AppState>) -> Result<Json<Vec<String>>, String> {
    let channels = state.store.list_streams().await.map_err(|e| e.to_string())?;
    Ok(Json(channels.into_iter()
            .filter(|c| c != "_global" && (state.allowed)(c))
            .collect()))
}

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: axum::extract::ws::WebSocket, state: AppState) {
    let mut rx = state.store.subscribe();

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Ok(event) => {
                        if let Some(ch) = &event.channel {
                            if !(state.allowed)(ch) { continue; }
                        }
                        if let Ok(json) = serde_json::to_string(&event) {
                            if socket.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            Some(msg) = socket.recv() => {
                if msg.is_err() { break; }
            }
        }
    }
}

async fn tail_all_streams(store: Arc<S2Store>) {
    loop {
        if let Ok(streams) = store.list_streams().await {
            for stream in streams {
                let store = store.clone();
                tokio::spawn(async move {
                    let _ = store.tail(&stream).await;
                });
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    let filter: Arc<dyn Fn(&str) -> bool + Send + Sync> = match (args.allow, args.deny) {
        (Some(allow), _) => Arc::new(move |c| allow.contains(&c.to_string())),
        (_, Some(deny)) => Arc::new(move |c| !deny.contains(&c.to_string())),
        _ => Arc::new(|_| true),
    };

    let store = Arc::new(S2Store::new().await?);

    let store_clone = store.clone();
    tokio::spawn(tail_all_streams(store_clone));

    let state = AppState { 
        store,
        allowed: filter,
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/help", get(help))
        .route("/api/channels", get(list_channels))
        .route("/api/logs/:channel", get(channel_logs))
        .route("/ws", get(websocket_handler))
        .layer(CorsLayer::new()
            .allow_origin(tower_http::cors::Any)
            .allow_methods(vec![http::Method::GET]))
        .layer(CompressionLayer::new())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", args.bind, args.port)).await?;
    println!("Listening on http://{}:{}", args.bind, args.port);

    axum::serve(listener, app).await?;
    Ok(())
}
