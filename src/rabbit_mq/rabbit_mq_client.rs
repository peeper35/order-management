use std::sync::Arc;

use amqprs::connection::{Connection, OpenConnectionArguments};
use tokio::sync::Mutex;

use crate::{
    consts::{
        OrderChannels, SharedTupleLtpChannel, EXECUTED_ORDERS_ROUTING_KEY, LTP_RECV, LTP_SUBSCRIBE,
        PENDING_ORDERS_ROUTING_KEY, UPDATE_ORDERS_ROUTING_KEY,
    },
    utils::get_env_var,
};

#[derive(Clone)]
pub struct RabbitMqClient {
    connection: Connection,
}

impl RabbitMqClient {
    pub async fn connect() -> Self {
        let connection = Connection::open(&OpenConnectionArguments::new(
            &get_env_var("RABBIT_MQ_HOST"),
            get_env_var("RABBIT_MQ_PORT")
                .parse::<u16>()
                .expect("Parsing Error"),
            &get_env_var("RABBIT_MQ_USERNAME"),
            &get_env_var("RABBIT_MQ_PASSWORD"),
        ))
        .await
        .expect("Failed to connect to RabbitMQ");

        Self { connection }
    }

    pub async fn create_orders_channels(&self) -> OrderChannels {
        let pending_channel = self
            .connection
            .open_channel(None)
            .await
            .expect("Failed to open pending order channel");

        let executed_channel = self
            .connection
            .open_channel(None)
            .await
            .expect("Failed to open executed order channel");

        let update_channel = self
            .connection
            .open_channel(None)
            .await
            .expect("Failed to open update order channel");

        [
            (pending_channel, PENDING_ORDERS_ROUTING_KEY),
            (executed_channel, EXECUTED_ORDERS_ROUTING_KEY),
            (update_channel, UPDATE_ORDERS_ROUTING_KEY),
        ]
    }

    pub async fn create_ltp_channels(&self) -> (SharedTupleLtpChannel, SharedTupleLtpChannel) {
        let ltp_subscribe_channel = self
            .connection
            .open_channel(None)
            .await
            .expect("Failed to open update order channel");

        let ltp_recv_channel = self
            .connection
            .open_channel(None)
            .await
            .expect("Failed to open update order channel");

        (
            (Arc::new(Mutex::new(ltp_subscribe_channel)), LTP_SUBSCRIBE),
            (Arc::new(Mutex::new(ltp_recv_channel)), LTP_RECV),
        )
    }
}
