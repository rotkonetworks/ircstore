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
    #[arg(short, long, default_value = "8080")]
    port: u16,
}

#[derive(Deserialize)]
struct LogQuery {
    limit: Option<usize>,
    offset: Option<u64>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<S2Store>,
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
    let limit = params.limit.unwrap_or(100).min(1000);
    let offset = params.offset.unwrap_or(0);
    let events = state.store.read(&channel, offset, limit).await.map_err(|e| e.to_string())?;
    Ok(Json(events))
}

async fn list_channels(State(state): State<AppState>) -> Result<Json<Vec<String>>, String> {
    let channels = state.store.list_streams().await.map_err(|e| e.to_string())?;
    Ok(Json(channels))
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
                        let json = serde_json::to_string(&event).unwrap();
                        if socket.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            Some(msg) = socket.recv() => {
                if msg.is_err() {
                    break;
                }
            }
        }
    }
}

async fn tail_all_streams(store: Arc<S2Store>) {
    loop {
        match store.list_streams().await {
            Ok(streams) => {
                for stream in streams {
                    let store_clone = store.clone();
                    let stream_clone = stream.clone();
                    tokio::spawn(async move {
                        if let Err(e) = store_clone.tail(&stream_clone).await {
                            eprintln!("Error tailing stream {}: {}", stream_clone, e);
                        }
                    });
                }
            }
            Err(e) => eprintln!("Error listing streams: {}", e),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    
    let store = Arc::new(S2Store::new().await?);
    
    // Start tailing all streams
    let store_clone = store.clone();
    tokio::spawn(tail_all_streams(store_clone));
    
    let state = AppState { store };
    
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
    
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    println!("Web UI listening on http://127.0.0.1:{}", args.port);
    
    axum::serve(listener, app).await?;
    Ok(())
}
