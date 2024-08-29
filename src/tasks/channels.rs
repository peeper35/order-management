use std::sync::Arc;

use amqprs::channel::{BasicConsumeArguments, Channel};
use chrono::Local;

use crate::{
    consts::{
        SharedRedisClient, SharedSubscribedHashMap, SharedTupleLtpChannel, SharedWebsocketClient,
        ORDERS_EXCHANGE,
    },
    rabbit_mq::dto::OrderMessage,
    tasks::{helpers::should_process_order, orders::create_order_task},
    utils::deserialize_message_bytes,
};

use super::helpers::{bind_queue_to_channel, is_trading_time};

pub async fn create_process_channel_task(
    channel: Channel,
    shared_ltp_send_channel: SharedTupleLtpChannel,
    shared_ltp_recv_channel: SharedTupleLtpChannel,
    ws_client: SharedWebsocketClient,
    redis_client: SharedRedisClient,
    subscribed_symbols: SharedSubscribedHashMap,
    queue_name_and_routing_key: &str,
) {
    if !is_trading_time() {
        tracing::warn!(
            "Create/Update Order Not Available. Not trading time. Time {}",
            Local::now().time()
        );
        return;
    }

    bind_queue_to_channel(&channel, ORDERS_EXCHANGE, queue_name_and_routing_key).await;

    let args = BasicConsumeArguments::new(queue_name_and_routing_key, queue_name_and_routing_key)
        .manual_ack(false)
        .finish();

    let (_, mut messages_rx) = channel.basic_consume_rx(args).await.unwrap();

    while let Some(msg) = messages_rx.recv().await {
        if let Some(order_message) = process_incoming_message(&msg.content) {
            process_order_message(
                order_message,
                &ws_client,
                &redis_client,
                &subscribed_symbols,
                queue_name_and_routing_key,
            )
            .await;
        }
    }
}

async fn process_order_message(
    order_message: OrderMessage,
    ws_client: &SharedWebsocketClient,
    redis_client: &SharedRedisClient,
    subscribed_symbols: &SharedSubscribedHashMap,
    queue_name: &str,
) {
    tracing::info!(
        "Received order on queue - [{}], Order Id: {}",
        queue_name,
        order_message.id
    );

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
}

fn process_incoming_message(content: &Option<Vec<u8>>) -> Option<OrderMessage> {
    content
        .as_ref()
        .and_then(|data| deserialize_message_bytes(data).ok())
}
