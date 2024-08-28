use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::websocket::websocket_client::WebsocketClient;

pub async fn update_subscribed_symbols(
    message_symbol: String,
    ws_client: Arc<Mutex<WebsocketClient>>,
    subscribed_symbols: Arc<Mutex<HashMap<String, usize>>>,
) {
    let mut subscribed_symbols_hmap = subscribed_symbols.lock().await;

    match subscribed_symbols_hmap.entry(message_symbol.clone()) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            *entry.get_mut() += 1;
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(1);

            ws_client
                .lock()
                .await
                .send_subscribe_message(&message_symbol)
                .await;
        }
    }
}
