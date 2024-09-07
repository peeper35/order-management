use std::{collections::HashMap, sync::Arc};

use amqprs::channel::Channel;
use chrono::NaiveTime;
use lazy_static::lazy_static;
use tokio::sync::Mutex;

use crate::{
    redis_utils::redis_client::{ClientConnected, RedisClient},
    websocket::websocket_client::WebsocketClient,
};

pub const PENDING_ORDERS_ROUTING_KEY: &str = "pending_orders";
pub const EXECUTED_ORDERS_ROUTING_KEY: &str = "executed_orders";
pub const UPDATE_ORDERS_ROUTING_KEY: &str = "update_orders";
pub const LTP_SUBSCRIBE: &str = "ltp_subscribe";
pub const LTP_RECV: &str = "ltp_recv";

pub const ORDERS_EXCHANGE: &str = "orders";
pub const LTP_EXCHANGE: &str = "ltp";

lazy_static! {
    pub static ref START_TIME: NaiveTime = NaiveTime::from_hms_opt(9, 30, 0).unwrap();
}

lazy_static! {
    pub static ref CUTOFF_TIME: NaiveTime = NaiveTime::from_hms_opt(15, 15, 0).unwrap();
}

pub type WebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
pub type SharedWebsocketClient = Arc<Mutex<WebsocketClient>>;
pub type SharedRedisClient = Arc<Mutex<RedisClient<ClientConnected>>>;
pub type SharedSubscribedHashMap = Arc<Mutex<HashMap<String, HashMap<i32, usize>>>>;
pub type SharedTupleLtpChannel = (Arc<Mutex<Channel>>, &'static str);
pub type OrderChannels = [(Channel, &'static str); 3];
