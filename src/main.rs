#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(clippy::enum_variant_names)]
#![allow(clippy::upper_case_acronyms)]

use std::sync::Arc;

use dotenv::dotenv;
use tokio::{self, runtime::Handle};

use rabbit_mq::rabbit_mq_client::RabbitMqClient;
use redis_utils::connection::create_shared_redis_client;
use tasks::channels::create_process_channel_task;
use utils::{create_shared_subd_instruments_hmap, init_subscriber};
use websocket::connection::create_shared_websocket_client;

mod consts;
mod rabbit_mq;
mod redis_utils;
mod tasks;
mod utils;
mod websocket;

#[tokio::main]
async fn main() {
    dotenv().ok();
    init_subscriber();

    let rabbit_mq_client = RabbitMqClient::connect().await;
    let orders_channels = rabbit_mq_client.create_orders_channels().await;
    let ltp_channels = rabbit_mq_client.create_ltp_channels().await;
    let ws_client = create_shared_websocket_client().await;
    let shared_redis_client = create_shared_redis_client().await;
    let subscribed_symbols = create_shared_subd_instruments_hmap();

    // Spawn tasks for each channel
    for (channel, queue_name) in orders_channels {
        tokio::spawn(create_process_channel_task(
            channel,
            ltp_channels.0.clone(),
            ltp_channels.1.clone(),
            Arc::clone(&ws_client),
            Arc::clone(&shared_redis_client),
            Arc::clone(&subscribed_symbols),
            queue_name,
        ));
    }

    // Monitor runtime metrics
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
        let active_tasks = Handle::current().metrics().num_alive_tasks();
        tracing::info!("Runtime has {} active tasks", active_tasks);
    }
}
