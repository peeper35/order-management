use super::rabbit_mq_client::RabbitMqClient;
use amqprs::channel::Channel;

pub async fn rabbit_mq_channels() -> ([(Channel, &'static str); 3], RabbitMqClient) {
    let client = RabbitMqClient::connect().await;
    (client.create_channels().await, client)
}
