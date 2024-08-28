use std::{marker::PhantomData, sync::Arc};

use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use tokio::sync::Mutex;

use crate::{
    common::{ClientConnected, ClientNotConnected},
    dto::OrderMessage,
};

pub struct RedisClient<State> {
    client: Client,
    conn: Option<MultiplexedConnection>,
    state: PhantomData<State>,
}

impl RedisClient<ClientNotConnected> {
    pub fn new(url: &str) -> RedisClient<ClientNotConnected> {
        let client = redis::Client::open(url).unwrap();

        RedisClient::<ClientNotConnected> {
            client,
            conn: None,
            state: PhantomData,
        }
    }

    pub async fn connect(self) -> Arc<Mutex<RedisClient<ClientConnected>>> {
        let conn = self
            .client
            .get_multiplexed_tokio_connection()
            .await
            .expect("Unable to get Redis connection");

        Arc::new(Mutex::new(RedisClient::<ClientConnected> {
            client: self.client,
            conn: Some(conn),
            state: PhantomData,
        }))
    }
}

impl RedisClient<ClientConnected> {
    pub async fn set(&mut self, key: String, value: OrderMessage) {
        let conn = self.conn.as_mut().unwrap();
        let serialized_value = serde_json::to_string(&value).unwrap();
        let _: () = conn.set(key, serialized_value).await.unwrap();
    }

    pub async fn get(&mut self, key: String) -> Option<OrderMessage> {
        let conn = self.conn.as_mut().unwrap();
        let value: Option<String> = conn.get(key).await.unwrap();

        value.map(|v| serde_json::from_str::<OrderMessage>(&v).unwrap())
    }

    pub async fn delete(&mut self, key: String) {
        let conn = self.conn.as_mut().unwrap();
        let _: () = conn.del(key).await.unwrap();
    }
}
