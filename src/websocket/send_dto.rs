use crate::utils::get_env_var;
use serde::Serialize;

const AUTH_MESSAGE_TYPE: &str = "Authenticate";
const SUBSCRIBE_REALTIME_MESSAGE_TYPE: &str = "SubscribeRealtime";

#[derive(Debug, Serialize)]
pub struct Authenticate {
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "Password")]
    password: String,
}

impl Authenticate {
    pub fn new() -> Self {
        Self {
            message_type: AUTH_MESSAGE_TYPE.into(),
            password: get_env_var("LTP_WEBSOCKET_ACCESS_KEY"),
        }
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(self).expect("Failed to serialize Authenticate message")
    }
}

#[derive(Debug, Serialize)]
pub struct SubscribeRealtime {
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "Exchange")]
    exchange: String,
    #[serde(rename = "InstrumentIdentifier")]
    instrument_identifier: String,
}

impl SubscribeRealtime {
    pub fn new(exchange: &str, instrument_identifier: &str) -> Self {
        Self {
            message_type: SUBSCRIBE_REALTIME_MESSAGE_TYPE.into(),
            exchange: exchange.into(),
            instrument_identifier: instrument_identifier.into(),
        }
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(self).expect("Failed to serialize SubscribeRealtime message")
    }
}
