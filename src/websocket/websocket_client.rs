use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::SEC_WEBSOCKET_PROTOCOL, HeaderValue},
        Message,
    },
};

use crate::{
    consts::{SharedWebsocketClient, WebSocket},
    utils::get_env_var,
};

use super::send_dto::SubscribeRealtime;

pub struct WebsocketClient {
    ws_stream: WebSocket,
}

impl WebsocketClient {
    pub async fn connect(url: &str) -> SharedWebsocketClient {
        let mut request = url.into_client_request().unwrap();
        request.headers_mut().append(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&get_env_var("LTP_WEBSOCKET_AUTH_KEY")).unwrap(),
        );
        let (ws_stream, _) = connect_async(request)
            .await
            .expect("Failed to connect to WebSocket");

        Arc::new(Mutex::new(Self { ws_stream }))
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
