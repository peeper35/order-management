use std::sync::Arc;

use tokio::sync::Mutex;

use crate::utils::get_env_var;

use super::websocket_client::WebsocketClient;

pub async fn create_shared_websocket_client() -> Arc<Mutex<WebsocketClient>> {
    let ws_url = get_env_var("LTP_WEBSOCKET_URL");

    WebsocketClient::connect(&ws_url).await
}
