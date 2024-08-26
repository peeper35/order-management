use chrono::NaiveTime;
use lazy_static::lazy_static;

pub const PENDING_ORDERS_ROUTING_KEY: &str = "pending_orders";
pub const EXECUTED_ORDERS_ROUTING_KEY: &str = "executed_orders";
pub const UPDATE_ORDERS_ROUTING_KEY: &str = "update_orders";

pub const EXCHANGE: &str = "orders";

lazy_static! {
    pub static ref START_TIME: NaiveTime = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
}

lazy_static! {
    pub static ref CUTOFF_TIME: NaiveTime = NaiveTime::from_hms_opt(15, 15, 0).unwrap();
}

pub type WebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
