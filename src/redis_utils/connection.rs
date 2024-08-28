use std::sync::Arc;

use tokio::sync::Mutex;

use super::redis_client::RedisClient;
use crate::{common::ClientConnected, utils::get_env_var};

pub async fn create_shared_redis_client() -> Arc<Mutex<RedisClient<ClientConnected>>> {
    let redis_url = get_env_var("REDIS_URL");

    let redis_client = RedisClient::new(&redis_url);
    redis_client.connect().await
}
