use redis::{aio::MultiplexedConnection, AsyncCommands};

use crate::dto::OrderMessage;

pub async fn set_update_redis_order_entry(
    redis_conn: &mut MultiplexedConnection,
    key: String,
    value: OrderMessage,
) {
    let _: () = redis_conn
        .set(key, serde_json::to_string(&value).unwrap())
        .await
        .unwrap();
}

pub async fn get_redis_order_entry(
    redis_conn: &mut MultiplexedConnection,
    key: String,
) -> Option<OrderMessage> {
    if let Ok(Some(value)) = redis_conn.get(key).await {
        let value: String = value;
        let value_desr = serde_json::from_str::<OrderMessage>(value.as_str()).unwrap();
        Some(value_desr)
    } else {
        None
    }
}

pub async fn delete_redis_order_entry(redis_conn: &mut MultiplexedConnection, key: String) {
    let _: () = redis_conn.del(key).await.unwrap();
}
