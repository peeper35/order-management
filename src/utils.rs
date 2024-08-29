use std::{collections::HashMap, env, sync::Arc};

use chrono::{Datelike, Duration, Local, Weekday};
use serde::Deserialize;
use serde_json::Error;
use tokio::sync::Mutex;
use tokio::time::{sleep_until, Duration as TokioDuration, Instant};
use tracing_appender::rolling;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::EnvFilter;

use crate::consts::SharedSubscribedHashMap;

pub fn get_env_var(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Missing environment variable: {}", key))
}

pub fn create_shared_subd_instruments_hmap() -> SharedSubscribedHashMap {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn sleep_until_next_trading_day() {
    let now = Local::now();
    let current_date = now.date_naive();

    // Determine the next trading day, skipping to Monday if today is Friday or Saturday
    let next_date = match now.weekday() {
        Weekday::Fri => current_date + Duration::days(3), // Skip to Monday
        Weekday::Sat => current_date + Duration::days(2), // Skip to Monday
        _ => current_date + Duration::days(1),            // Otherwise, move to the next day
    };

    let next_trading_time = next_date.and_hms_opt(9, 30, 0).unwrap();

    let duration_until_next_trading = next_trading_time - now.naive_local();
    let sleep_duration = TokioDuration::from_secs(duration_until_next_trading.num_seconds() as u64);

    tracing::info!("Sleeping until next trading day: {}", next_trading_time);
    sleep_until(Instant::now() + sleep_duration).await;
}

pub fn deserialize_message_str<T: for<'a> Deserialize<'a>>(data: &str) -> Result<T, Error> {
    serde_json::from_str(data)
}

pub fn deserialize_message_bytes<T: for<'a> Deserialize<'a>>(data: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(data)
}

pub fn init_subscriber() {
    let log_files = rolling::hourly("./logs/debug", "debug")
        .and(rolling::daily("./logs/warnings", "warnings").with_max_level(tracing::Level::WARN));

    tracing_subscriber::fmt()
        .with_writer(log_files.and(std::io::stdout))
        .with_target(true)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_log=warn,aznxt_order_management=info".into()),
        )
        .init();
}
