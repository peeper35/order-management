// use std::sync::Arc;

// use chrono::{Local, Timelike};
// use futures_util::StreamExt;
// use redis::Client;
// use tokio::sync::Mutex;
// use tokio_tungstenite::tungstenite::Message;

// use crate::{
//     consts::{WebSocket, CUTOFF_TIME, START_TIME},
//     dto::{OrderMessage, OrderType, WebsocketMsgRcv},
//     redis_utils::commands::{delete_redis_order_entry, get_redis_order_entry},
//     utils::sleep_until_next_trading_day,
//     websocket::send::send_subscribe_message,
// };

// pub async fn create_order_task(
//     mut message: OrderMessage,
//     ws_stream: Arc<Mutex<WebSocket>>,
//     redis_client: Arc<Mutex<Client>>,
// ) {
//     send_subscribe_message(Arc::clone(&ws_stream), &message.symbol).await;

//     let mut redis_conn = redis_client
//         .lock()
//         .await
//         .get_multiplexed_tokio_connection()
//         .await
//         .expect("Unable to get connection");

//     'ws_loop: while let Some(msg) = ws_stream.lock().await.next().await {
//         message = get_redis_order_entry(&mut redis_conn, message.id)
//             .await
//             .unwrap();
//         // Get the current time in UTC
//         let now = Local::now();
//         let current_time = now.time();

//         if let Ok(Message::Text(inner_message)) = msg {
//             if current_time <= *START_TIME || current_time >= *CUTOFF_TIME {
//                 match message.order_type {
//                     OrderType::CNC => {
//                         sleep_until_next_trading_day().await;
//                     }
//                     OrderType::MIS => {
//                         tracing::info!(
//                             "MIS Square off: {}. Time: {}:{}",
//                             &message.id,
//                             current_time.hour(),
//                             current_time.minute()
//                         );
//                         delete_redis_order_entry(&mut redis_conn, message.id).await;
//                         break 'ws_loop;
//                     }
//                 }
//             }

//             let websocket_recv_msg =
//                 serde_json::from_str::<Vec<WebsocketMsgRcv>>(&inner_message).unwrap();

//             let filtered_msg = websocket_recv_msg
//                 .into_iter()
//                 .filter(|msg| msg.symbol == message.symbol)
//                 .collect::<Vec<WebsocketMsgRcv>>();

//             tracing::info!("Task Id: {}, Message: {:?}", message.id, filtered_msg);
//         }
//     }
// }

use chrono::Local;
use futures_util::StreamExt;
use redis::{aio::MultiplexedConnection, Client};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    consts::{WebSocket, CUTOFF_TIME, START_TIME},
    dto::{OrderMessage, OrderType, WebsocketMsgRcv},
    redis_utils::commands::{delete_redis_order_entry, get_redis_order_entry},
    utils::sleep_until_next_trading_day,
    websocket::send::send_subscribe_message,
};

pub async fn create_order_task(
    message: OrderMessage,
    ws_stream: Arc<Mutex<WebSocket>>,
    redis_client: Arc<Mutex<Client>>,
) {
    // Subscribe to the websocket with the symbol
    send_subscribe_message(Arc::clone(&ws_stream), &message.symbol).await;

    // Get Redis connection
    let mut redis_conn = get_redis_connection(redis_client).await;

    // Process the websocket stream
    process_websocket_stream(message, ws_stream, &mut redis_conn).await;
}

async fn get_redis_connection(redis_client: Arc<Mutex<Client>>) -> MultiplexedConnection {
    redis_client
        .lock()
        .await
        .get_multiplexed_tokio_connection()
        .await
        .expect("Unable to get Redis connection")
}

async fn process_websocket_stream(
    mut message: OrderMessage,
    ws_stream: Arc<Mutex<WebSocket>>,
    redis_conn: &mut MultiplexedConnection,
) {
    while let Some(msg) = ws_stream.lock().await.next().await {
        // Refresh message data from Redis
        if let Some(refreshed_message) = refresh_order_message(&mut message, redis_conn).await {
            message = refreshed_message;
        }

        // Get the current time
        let current_time = Local::now().time();

        if let Ok(Message::Text(inner_message)) = msg {
            // Handle out-of-trading-hours actions
            if is_out_of_trading_hours(current_time) {
                if !handle_out_of_trading_hours(&message, redis_conn).await {
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
    redis_conn: &mut MultiplexedConnection,
) -> Option<OrderMessage> {
    get_redis_order_entry(redis_conn, message.id.clone()).await
}

fn is_out_of_trading_hours(current_time: chrono::NaiveTime) -> bool {
    current_time <= *START_TIME || current_time >= *CUTOFF_TIME
}

async fn handle_out_of_trading_hours(
    message: &OrderMessage,
    redis_conn: &mut MultiplexedConnection,
) -> bool {
    match message.order_type {
        OrderType::CNC => {
            sleep_until_next_trading_day().await;
            true
        }
        OrderType::MIS => {
            tracing::info!("MIS Square off: [{}]. Out of trading hours", &message.id,);
            delete_redis_order_entry(redis_conn, message.id.clone()).await;
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
