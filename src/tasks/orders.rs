use chrono::Local;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    consts::{SharedRedisClient, SharedSubscribedHashMap, SharedWebsocketClient},
    rabbit_mq::dto::OrderMessage,
    utils::deserialize_message_str,
    websocket::recv_dto::{RealtimeRecv, SUBSCRIBE_REALTIME_RECV_MSG},
};

use super::helpers::{
    cutoff_time, handle_cutoff_time, refresh_order_message, update_subscribed_symbols,
};

pub async fn create_order_task(
    message: OrderMessage,
    ws_client: SharedWebsocketClient,
    redis_client: SharedRedisClient,
    subscribed_symbols: SharedSubscribedHashMap,
) {
    // Update the subscribed symbols
    update_subscribed_symbols(
        message.exchange.clone(),
        message.symbol.clone(),
        Arc::clone(&ws_client),
        Arc::clone(&subscribed_symbols),
    )
    .await;

    // Process the websocket stream
    process_websocket_stream(message, ws_client, redis_client).await;
}

async fn process_websocket_stream(
    mut message: OrderMessage,
    ws_client: SharedWebsocketClient,
    redis_conn: SharedRedisClient,
) {
    while let Some(msg) = receive_next_message(&ws_client).await {
        if let Ok(msg) = msg {
            if let Message::Text(inner_message) = msg {
                process_message(inner_message, &mut message, &redis_conn).await;
            }
        } else {
            tracing::error!("WebSocket error: {:?}", msg);
            break;
        }

        if cutoff_time(Local::now().time()) && !handle_cutoff_time(&message, &redis_conn).await {
            break;
        }
    }
}

async fn process_message(
    inner_message: String,
    message: &mut OrderMessage,
    redis_conn: &SharedRedisClient,
) {
    // Refresh message data from Redis
    if let Some(refreshed_message) = refresh_order_message(message, Arc::clone(redis_conn)).await {
        *message = refreshed_message;
    }

    if let Ok(r_data) = deserialize_message_str::<RealtimeRecv>(&inner_message) {
        if r_data.message_type == SUBSCRIBE_REALTIME_RECV_MSG {
            tracing::info!("[{}] - LTP: {}", message.symbol, r_data.last_trade_price);
        }
    }
    // test
    else {
        tracing::warn!("Data: {:?}", inner_message);
    }
    // test
}

async fn receive_next_message(
    ws_client: &SharedWebsocketClient,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    ws_client.lock().await.recv_next().await
}
