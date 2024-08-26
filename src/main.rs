use rabbit_mq::{create_channels, create_rabbit_mq_connection};
use redis_utils::connection::create_shared_redis_client;
use std::sync::Arc;
use tasks::channel::create_process_channel_task;
use tokio::{self, runtime::Handle};
use utils::init_subscriber;
use websocket::connection::create_shared_websocket;

use dotenv::dotenv;
use tracing;

mod connection;
mod consts;
mod dto;
mod rabbit_mq;
mod redis_utils;
mod tasks;
mod utils;
mod websocket;

#[tokio::main]
async fn main() {
    dotenv().ok();

    init_subscriber();

    let rabbit_mq_connection = create_rabbit_mq_connection().await;

    let rabbit_mq_channels = create_channels(rabbit_mq_connection.clone()).await;

    let ws_stream = create_shared_websocket().await;

    let redis_client = create_shared_redis_client().await;

    for (channel, routing_key) in rabbit_mq_channels {
        tokio::spawn(create_process_channel_task(
            channel,
            Arc::clone(&ws_stream),
            Arc::clone(&redis_client),
            routing_key,
            routing_key,
        ));
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;

        let metrics = Handle::current().metrics();

        let n = metrics.num_alive_tasks();
        tracing::info!("Runtime has {} active tasks", n);
    }
}
