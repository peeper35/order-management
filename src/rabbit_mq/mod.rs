use std::env;

use amqprs::{
    channel::Channel,
    connection::{Connection, OpenConnectionArguments},
};

use crate::consts::{
    EXECUTED_ORDERS_ROUTING_KEY, PENDING_ORDERS_ROUTING_KEY, UPDATE_ORDERS_ROUTING_KEY,
};

pub async fn create_rabbit_mq_connection() -> Connection {
    let connection = Connection::open(&OpenConnectionArguments::new(
        env::var("RABBIT_MQ_HOST")
            .expect("Missing RABBIT_MQ_HOST")
            .as_str(),
        env::var("RABBIT_MQ_PORT")
            .expect("Missing RABBIT_MQ_PORT")
            .parse::<u16>()
            .expect("Parsing Error"),
        env::var("RABBIT_MQ_USERNAME")
            .expect("Missing RABBIT_MQ_USERNAME")
            .as_str(),
        env::var("RABBIT_MQ_PASSWORD")
            .expect("Missing RABBIT_MQ_PASSWORD")
            .as_str(),
    ))
    .await
    .expect("Failed to connect to RabbitMQ");

    connection
}

pub async fn create_channels(connection: Connection) -> [(Channel, &'static str); 3] {
    let pending_channel = connection
        .open_channel(None)
        .await
        .expect("Failed to open pending order channel");

    let executed_channel = connection
        .open_channel(None)
        .await
        .expect("Failed to open executed order channel");

    let update_channel = connection
        .open_channel(None)
        .await
        .expect("Failed to open update order channel");

    [
        (pending_channel, PENDING_ORDERS_ROUTING_KEY),
        (executed_channel, EXECUTED_ORDERS_ROUTING_KEY),
        (update_channel, UPDATE_ORDERS_ROUTING_KEY),
    ]
}
