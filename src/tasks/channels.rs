use std::{collections::HashMap, sync::Arc};

use amqprs::channel::{BasicConsumeArguments, Channel, QueueBindArguments};
use chrono::Local;
use tokio::sync::Mutex;

use crate::{
    common::ClientConnected,
    consts::{
        CUTOFF_TIME, EXCHANGE, EXECUTED_ORDERS_ROUTING_KEY, PENDING_ORDERS_ROUTING_KEY, START_TIME,
    },
    dto::OrderMessage,
    redis_utils::redis_client::RedisClient,
    tasks::orders::create_order_task,
    websocket::websocket_client::WebsocketClient,
};

pub async fn create_process_channel_task(
    channel: Channel,
    ws_client: Arc<Mutex<WebsocketClient>>,
    redis_client: Arc<Mutex<RedisClient<ClientConnected>>>,
    subscribed_symbols: Arc<Mutex<HashMap<String, usize>>>,
    queue_name: &str,
    routing_key: &str,
) {
    bind_queue_to_channel(&channel, routing_key).await;

    let args = BasicConsumeArguments::new(routing_key, routing_key)
        .manual_ack(false)
        .finish();

    let (_, mut messages_rx) = channel.basic_consume_rx(args).await.unwrap();

    while let Some(msg) = messages_rx.recv().await {
        let order_message = deserialize_order_message(&msg.content.unwrap());

        process_order_message(
            order_message,
            &ws_client,
            &redis_client,
            &subscribed_symbols,
            queue_name,
        )
        .await;
    }
}

async fn bind_queue_to_channel(channel: &Channel, routing_key: &str) {
    channel
        .queue_bind(QueueBindArguments::new(routing_key, EXCHANGE, routing_key))
        .await
        .unwrap();
}

fn deserialize_order_message(content: &[u8]) -> OrderMessage {
    serde_json::from_slice::<OrderMessage>(content).unwrap()
}

async fn process_order_message(
    order_message: OrderMessage,
    ws_client: &Arc<Mutex<WebsocketClient>>,
    redis_client: &Arc<Mutex<RedisClient<ClientConnected>>>,
    subscribed_symbols: &Arc<Mutex<HashMap<String, usize>>>,
    queue_name: &str,
) {
    tracing::info!(
        "Received order on queue - [{}], Order Id: {}",
        queue_name,
        order_message.id
    );

    if is_trading_time() {
        redis_client
            .lock()
            .await
            .set(order_message.id.clone(), order_message.clone())
            .await;

        if should_process_order(queue_name) {
            tokio::spawn(create_order_task(
                order_message,
                Arc::clone(ws_client),
                Arc::clone(redis_client),
                Arc::clone(subscribed_symbols),
            ));
        }
    } else {
        tracing::warn!(
            "Create/Update Order Not Available. Not trading time. Time {}",
            Local::now().time()
        );
    }
}

fn should_process_order(queue_name: &str) -> bool {
    queue_name == PENDING_ORDERS_ROUTING_KEY || queue_name == EXECUTED_ORDERS_ROUTING_KEY
}

fn is_trading_time() -> bool {
    let current_time = Local::now().time();
    current_time > *START_TIME && current_time < *CUTOFF_TIME
}
