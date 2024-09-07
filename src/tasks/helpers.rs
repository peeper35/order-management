use std::collections::HashMap;

use amqprs::channel::{Channel, QueueBindArguments};
use chrono::Local;

use crate::{
    consts::{
        SharedRedisClient, SharedSubscribedHashMap, SharedWebsocketClient, CUTOFF_TIME,
        EXECUTED_ORDERS_ROUTING_KEY, PENDING_ORDERS_ROUTING_KEY, START_TIME,
    },
    rabbit_mq::dto::{OrderMessage, OrderType},
    utils::sleep_until_next_trading_day,
};

pub async fn bind_queue_to_channel(channel: &Channel, exchange: &str, routing_key: &str) {
    channel
        .queue_bind(QueueBindArguments::new(routing_key, exchange, routing_key))
        .await
        .unwrap();
}

pub async fn refresh_order_message(
    message: &OrderMessage,
    redis_conn: SharedRedisClient,
) -> Option<OrderMessage> {
    redis_conn.lock().await.get(message.id.clone()).await
}

pub fn should_process_order(queue_name: &str) -> bool {
    matches!(
        queue_name,
        PENDING_ORDERS_ROUTING_KEY | EXECUTED_ORDERS_ROUTING_KEY
    )
}

pub fn is_trading_time() -> bool {
    let current_time = Local::now().time();
    current_time > *START_TIME && current_time < *CUTOFF_TIME
}

pub fn cutoff_time(current_time: chrono::NaiveTime) -> bool {
    current_time <= *START_TIME || current_time >= *CUTOFF_TIME
}

pub async fn handle_cutoff_time(message: &OrderMessage, redis_conn: &SharedRedisClient) -> bool {
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

pub async fn update_subscribed_symbols(
    exchange: String,
    instrument_token: i32,
    ws_client: SharedWebsocketClient,
    subscribed_symbols: SharedSubscribedHashMap,
) {
    let mut should_subscribe = false;
    let mut sub_count = 0;

    {
        let mut subscribed_symbols_hmap = subscribed_symbols.lock().await;

        let exchange_entry = subscribed_symbols_hmap
            .entry(exchange.clone())
            .or_insert_with(HashMap::new);

        match exchange_entry.entry(instrument_token.clone()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                sub_count = *entry.get_mut() + 1;
                entry.insert(sub_count);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(1);
                sub_count = 1;
                should_subscribe = true;
            }
        }
    }

    if should_subscribe {
        ws_client
            .lock()
            .await
            .subscribe_instrument(instrument_token)
            .await;
    }

    tracing::info!(
        "[Websocket] Subscribing to {} on {}. Subscribe count: {}",
        instrument_token,
        exchange,
        sub_count
    );
}
