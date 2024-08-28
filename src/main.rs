use rabbit_mq::connection::rabbit_mq_channels;
use redis_utils::connection::create_shared_redis_client;
use std::sync::Arc;
use tasks::channels::create_process_channel_task;
use tokio::{self, runtime::Handle};
use utils::{create_shared_subed_symbol_hmap, init_subscriber};
use websocket::connection::create_shared_websocket_client;

use dotenv::dotenv;

mod common;
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

    #[allow(unused_variables)]
    let (rabbit_mq_channels, client) = rabbit_mq_channels().await;

    let ws_stream = create_shared_websocket_client().await;

    let shared_redis_client = create_shared_redis_client().await;

    let subscribed_symbols = create_shared_subed_symbol_hmap();

    for (channel, routing_key) in rabbit_mq_channels {
        tokio::spawn(create_process_channel_task(
            channel,
            Arc::clone(&ws_stream),
            Arc::clone(&shared_redis_client),
            Arc::clone(&subscribed_symbols),
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
