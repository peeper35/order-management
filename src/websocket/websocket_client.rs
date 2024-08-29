use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    consts::{SharedWebsocketClient, WebSocket},
    utils::deserialize_message_str,
    websocket::recv_dto::AUTH_RECV_MSG,
};

use super::{
    recv_dto::AuthenticateRecv,
    send_dto::{Authenticate, SubscribeRealtime},
};

pub struct WebsocketClient {
    ws_stream: WebSocket,
}

impl WebsocketClient {
    pub async fn connect(url: &str) -> SharedWebsocketClient {
        let (ws_stream, _) = connect_async(url)
            .await
            .expect("Failed to connect to WebSocket");

        Arc::new(Mutex::new(Self { ws_stream }))
    }

    pub async fn authenticate(&mut self) {
        let auth_message = Authenticate::new().serialize();

        self.send_message(auth_message).await;

        while let Some(Ok(msg)) = self.recv_next().await {
            if let Ok(auth) = deserialize_message_str::<AuthenticateRecv>(&msg.into_text().unwrap())
            {
                if auth.message_type == AUTH_RECV_MSG {
                    if auth.complete {
                        tracing::info!("[WebSocket] Authentication successful");
                    } else {
                        tracing::error!("[WebSocket] Authentication failed: {}", auth.message);
                    }
                    break;
                }
            }
        }
    }

    pub async fn subscribe_instrument(&mut self, exchange: &str, instrument_identifier: &str) {
        let subscribe_message = SubscribeRealtime::new(exchange, instrument_identifier).serialize();

        self.send_message(subscribe_message).await;
    }

    pub async fn recv_next(
        &mut self,
    ) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
        self.ws_stream.next().await
    }

    async fn send_message(&mut self, message: String) {
        self.ws_stream
            .send(Message::Text(message))
            .await
            .expect("Failed to send message over WebSocket");
    }
}
