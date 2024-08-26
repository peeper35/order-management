use std::{env, sync::Arc};

use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;

use crate::consts::WebSocket;

pub async fn create_shared_websocket() -> Arc<Mutex<WebSocket>> {
    let binding = env::var("LTP_WEBSOCKET_URL").expect("Missing LTP_WEBSOCKET_URL");
    let ws_url = binding.as_str();
    let (ws_stream, _) = connect_async(ws_url).await.expect("Failed to connect");
    Arc::new(Mutex::new(ws_stream))
}
