use std::sync::Arc;

use futures_util::SinkExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::{consts::WebSocket, dto::WebsocketMsgSnd};

pub async fn send_subscribe_message(ws_stream: Arc<Mutex<WebSocket>>, message_symbol: &str) {
    let mut ws_lock = ws_stream.lock().await;

    let subscribe_message = WebsocketMsgSnd {
        action: "ADD".to_string(),
        symbol: message_symbol,
    };

    let message_text = serde_json::to_string(&subscribe_message).unwrap();
    ws_lock.send(Message::Text(message_text)).await.unwrap();
}
