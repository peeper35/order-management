use amqprs::{
    channel::Channel,
    connection::{Connection, OpenConnectionArguments},
};

use crate::{
    consts::{EXECUTED_ORDERS_ROUTING_KEY, PENDING_ORDERS_ROUTING_KEY, UPDATE_ORDERS_ROUTING_KEY},
    utils::get_env_var,
};

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

    pub async fn create_channels(&self) -> [(Channel, &'static str); 3] {
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
}
