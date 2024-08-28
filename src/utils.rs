use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use chrono::{Datelike, Duration, Local, Weekday};
use tokio::sync::Mutex;
use tokio::time::sleep_until;
use tokio::time::Duration as TokioDuration;
use tokio::time::Instant;
use tracing_appender::rolling;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::EnvFilter;

pub fn get_env_var(key: &str) -> String {
    env::var(key).expect(format!("Missing {}", key).as_str())
}

pub fn create_shared_subed_symbol_hmap() -> Arc<Mutex<HashMap<String, usize>>> {
    let subscribed_sumbols = HashMap::new();
    Arc::new(Mutex::new(subscribed_sumbols))
}

pub async fn sleep_until_next_trading_day() {
    let now = Local::now();
    let current_date = now.date_naive();

    // Check if today is Friday or Saturday
    let next_date = match now.weekday() {
        Weekday::Fri => current_date + Duration::days(3), // Skip to Monday
        Weekday::Sat => current_date + Duration::days(2), // Skip to Monday
        _ => current_date + Duration::days(1),            // Otherwise, move to the next day
    };

    let next_trading_time = next_date.and_hms_opt(9, 30, 0).unwrap();

    let now_naive = now.naive_local();

    // Calculate the duration to sleep until the next trading day at 09:30
    let duration_until_next_trading = next_trading_time - now_naive;

    let sleep_duration = TokioDuration::from_secs(duration_until_next_trading.num_seconds() as u64);

    tracing::info!("Sleeping until next trading day: {}", next_trading_time);
    sleep_until(Instant::now() + sleep_duration).await;
}

pub fn init_subscriber() {
    let all_files = rolling::hourly("./logs/debug", "debug")
        .and(rolling::daily("./logs/warnings", "warnings").with_max_level(tracing::Level::WARN));

    tracing_subscriber::fmt()
        .with_writer(all_files.and(std::io::stdout))
        .with_target(true)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_log=warn,aznxt_order_management=info".into()),
        )
        .init();
}
