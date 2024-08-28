use chrono::Local;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    common::ClientConnected,
    consts::{CUTOFF_TIME, START_TIME},
    dto::{OrderMessage, OrderType, WebsocketMsgRcv},
    redis_utils::redis_client::RedisClient,
    utils::sleep_until_next_trading_day,
    websocket::websocket_client::WebsocketClient,
};

use super::helpers::update_subscribed_symbols;

pub async fn create_order_task(
    message: OrderMessage,
    ws_client: Arc<Mutex<WebsocketClient>>,
    redis_client: Arc<Mutex<RedisClient<ClientConnected>>>,
    subscribed_symbols: Arc<Mutex<HashMap<String, usize>>>,
) {
    // Update the subscribed symbols
    update_subscribed_symbols(
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
    ws_client: Arc<Mutex<WebsocketClient>>,
    redis_conn: Arc<Mutex<RedisClient<ClientConnected>>>,
) {
    while let Some(msg) = ws_client.lock().await.recv_next().await {
        // Refresh message data from Redis
        if let Some(refreshed_message) =
            refresh_order_message(&mut message, Arc::clone(&redis_conn)).await
        {
            message = refreshed_message;
        }

        // Get the current time
        let current_time = Local::now().time();

        if let Ok(Message::Text(inner_message)) = msg {
            // Handle out-of-trading-hours actions
            if cutoff_time(current_time) {
                if !handle_cutoff_time(&message, Arc::clone(&redis_conn)).await {
                    break;
                }
            } else {
                // Process the received websocket message
                process_received_message(&message, inner_message).await;
            }
        }
    }
}

async fn refresh_order_message(
    message: &mut OrderMessage,
    redis_conn: Arc<Mutex<RedisClient<ClientConnected>>>,
) -> Option<OrderMessage> {
    redis_conn.lock().await.get(message.id.clone()).await
}

fn cutoff_time(current_time: chrono::NaiveTime) -> bool {
    current_time <= *START_TIME || current_time >= *CUTOFF_TIME
}

async fn handle_cutoff_time(
    message: &OrderMessage,
    redis_conn: Arc<Mutex<RedisClient<ClientConnected>>>,
) -> bool {
    match message.order_type {
        OrderType::CNC => {
            sleep_until_next_trading_day().await;
            true
        }
        OrderType::MIS => {
            tracing::info!("MIS Square off: [{}]. Out of trading hours", &message.id,);
            redis_conn.lock().await.delete(message.id.clone()).await;
            false
        }
    }
}

async fn process_received_message(message: &OrderMessage, inner_message: String) {
    let websocket_recv_msg = parse_websocket_message(&inner_message);

    let filtered_msg = filter_messages_by_symbol(websocket_recv_msg, &message.symbol);

    tracing::info!("Task Id: {}, Message: {:?}", message.id, filtered_msg);
}

fn parse_websocket_message(inner_message: &str) -> Vec<WebsocketMsgRcv> {
    serde_json::from_str::<Vec<WebsocketMsgRcv>>(inner_message)
        .expect("Failed to parse websocket message")
}

fn filter_messages_by_symbol(messages: Vec<WebsocketMsgRcv>, symbol: &str) -> Vec<WebsocketMsgRcv> {
    messages
        .into_iter()
        .filter(|msg| msg.symbol == *symbol)
        .collect::<Vec<WebsocketMsgRcv>>()
}
