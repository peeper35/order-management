use std::{env, sync::Arc};

use redis::Client;
use tokio::sync::Mutex;

pub async fn create_shared_redis_client() -> Arc<Mutex<Client>> {
    let binding = env::var("REDIS_URL").expect("Missing REDIS_URL");
    let redis_url = binding.as_str();
    let client: redis::Client = redis::Client::open(redis_url).unwrap();
    Arc::new(Mutex::new(client))
}
