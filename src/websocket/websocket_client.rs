use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{consts::WebSocket, dto::WebsocketMsgSnd};

pub struct WebsocketClient {
    ws_stream: WebSocket,
}

impl WebsocketClient {
    pub async fn connect(url: &str) -> Arc<Mutex<WebsocketClient>> {
        let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

        Arc::new(Mutex::new(Self { ws_stream }))
    }

    pub async fn send_subscribe_message(&mut self, message_symbol: &str) {
        let subscribe_message = WebsocketMsgSnd {
            action: "ADD".to_string(),
            symbol: message_symbol,
        };

        let message_text = serde_json::to_string(&subscribe_message).unwrap();
        self.ws_stream
            .send(Message::Text(message_text))
            .await
            .unwrap();
    }

    pub async fn recv_next(
        &mut self,
    ) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
        self.ws_stream.next().await
    }
}
