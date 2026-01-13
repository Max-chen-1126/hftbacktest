//! HashKey API message types
//!
//! This module defines all JSON message structures for HashKey Exchange API.
//! Reference: https://hashkeypro-apidoc.readme.io/reference/introduction

use std::fmt;

use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{
    Deserialize,
    Deserializer,
    Serialize,
    de::{self, Error, Unexpected, Visitor},
};

use crate::utils::{from_str_to_f64, from_str_to_f64_opt};

// ============================================================================
// Custom Deserializers
// ============================================================================

struct SideVisitor;

impl Visitor<'_> for SideVisitor {
    type Value = Side;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"BUY\" or \"SELL\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s.to_uppercase().as_str() {
            "BUY" => Ok(Side::Buy),
            "SELL" => Ok(Side::Sell),
            s => Err(Error::invalid_value(Unexpected::Other(s), &"BUY or SELL")),
        }
    }
}

fn from_str_to_side<'de, D>(deserializer: D) -> Result<Side, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(SideVisitor)
}

struct OrdTypeVisitor;

impl Visitor<'_> for OrdTypeVisitor {
    type Value = OrdType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"MARKET\", \"LIMIT\", or \"LIMIT_MAKER\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s.to_uppercase().as_str() {
            "MARKET" => Ok(OrdType::Market),
            "LIMIT" => Ok(OrdType::Limit),
            "LIMIT_MAKER" => Ok(OrdType::Limit), // Post-only limit order
            s => Err(Error::invalid_value(
                Unexpected::Other(s),
                &"MARKET, LIMIT, or LIMIT_MAKER",
            )),
        }
    }
}

fn from_str_to_ord_type<'de, D>(deserializer: D) -> Result<OrdType, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(OrdTypeVisitor)
}

struct TimeInForceVisitor;

impl Visitor<'_> for TimeInForceVisitor {
    type Value = TimeInForce;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing \"GTC\", \"IOC\", or \"FOK\"")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s.to_uppercase().as_str() {
            "GTC" => Ok(TimeInForce::GTC),
            "IOC" => Ok(TimeInForce::IOC),
            "FOK" => Ok(TimeInForce::FOK),
            s => Err(Error::invalid_value(Unexpected::Other(s), &"GTC, IOC, or FOK")),
        }
    }
}

fn from_str_to_time_in_force<'de, D>(deserializer: D) -> Result<TimeInForce, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(TimeInForceVisitor)
}

struct StatusVisitor;

impl Visitor<'_> for StatusVisitor {
    type Value = Status;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string containing order status")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match s.to_uppercase().as_str() {
            "NEW" => Ok(Status::New),
            "PARTIALLY_FILLED" => Ok(Status::PartiallyFilled),
            "FILLED" => Ok(Status::Filled),
            "CANCELED" => Ok(Status::Canceled),
            "CANCELLED" => Ok(Status::Canceled),
            "REJECTED" => Ok(Status::Rejected),
            "EXPIRED" => Ok(Status::Expired),
            "PENDING_CANCEL" => Ok(Status::New), // Still active until confirmed
            s => Err(Error::invalid_value(Unexpected::Other(s), &"valid order status")),
        }
    }
}

fn from_str_to_status<'de, D>(deserializer: D) -> Result<Status, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(StatusVisitor)
}

// ============================================================================
// WebSocket Public Stream Messages
// ============================================================================

/// WebSocket subscription request
#[derive(Serialize, Debug)]
pub struct WsSubscription {
    pub topic: String,
    pub event: String, // "sub" or "cancel"
    pub params: WsSubscriptionParams,
}

#[derive(Serialize, Debug)]
pub struct WsSubscriptionParams {
    pub symbol: String,
}

/// WebSocket ping message
#[derive(Serialize, Debug)]
pub struct WsPing {
    pub ping: i64,
}

/// WebSocket pong response
#[derive(Deserialize, Debug)]
pub struct WsPong {
    pub pong: i64,
    #[serde(rename = "channelId")]
    pub channel_id: Option<String>,
}

/// Public WebSocket message wrapper
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PublicWsMessage {
    Pong(WsPong),
    Depth(DepthMessage),
    Trade(TradeMessage),
    Realtimes(RealtimesMessage),
}

/// Depth (order book) update message
#[derive(Deserialize, Debug)]
pub struct DepthMessage {
    pub topic: String,
    pub params: DepthParams,
    pub data: DepthData,
}

#[derive(Deserialize, Debug)]
pub struct DepthParams {
    pub symbol: String,
}

#[derive(Deserialize, Debug)]
pub struct DepthData {
    /// Symbol
    pub s: String,
    /// Timestamp in milliseconds
    pub t: i64,
    /// Version
    pub v: String,
    /// Bids: [[price, quantity], ...]
    pub b: Vec<(String, String)>,
    /// Asks: [[price, quantity], ...]
    pub a: Vec<(String, String)>,
}

/// Trade message
#[derive(Deserialize, Debug)]
pub struct TradeMessage {
    pub topic: String,
    pub params: TradeParams,
    pub data: TradeData,
}

#[derive(Deserialize, Debug)]
pub struct TradeParams {
    pub symbol: String,
}

#[derive(Deserialize, Debug)]
pub struct TradeData {
    /// Trade ID
    pub v: String,
    /// Timestamp in milliseconds
    pub t: i64,
    /// Price
    pub p: String,
    /// Quantity
    pub q: String,
    /// Is buyer maker? true = buyer is maker (sell trade), false = buyer is taker (buy trade)
    pub m: bool,
}

/// Realtimes (24h ticker) message
#[derive(Deserialize, Debug)]
pub struct RealtimesMessage {
    pub topic: String,
    pub params: RealtimesParams,
    pub data: Vec<RealtimesData>,
}

#[derive(Deserialize, Debug)]
pub struct RealtimesParams {
    #[serde(rename = "realtimeInterval")]
    pub realtime_interval: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct RealtimesData {
    /// Timestamp
    pub t: i64,
    /// Symbol
    pub s: String,
    /// Close price
    pub c: String,
    /// High price
    pub h: String,
    /// Low price
    pub l: String,
    /// Open price
    pub o: String,
    /// Volume
    pub v: String,
    /// Quote volume
    pub qv: String,
}

// ============================================================================
// WebSocket Private Stream Messages
// ============================================================================

/// Private WebSocket message wrapper
#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PrivateWsMessage {
    Pong(WsPong),
    ExecutionReport(ExecutionReport),
    AccountInfo(AccountInfo),
    TicketInfo(TicketInfo),
}

/// Execution report - real-time order updates
#[derive(Deserialize, Debug)]
pub struct ExecutionReport {
    #[serde(rename = "e")]
    pub event_type: String, // "executionReport"
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "o")]
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(rename = "f")]
    #[serde(deserialize_with = "from_str_to_time_in_force")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub quantity: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "X")]
    #[serde(deserialize_with = "from_str_to_status")]
    pub order_status: Status,
    #[serde(rename = "i")]
    pub order_id: String, // Exchange order ID
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_executed_qty: f64,
    #[serde(rename = "z")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cumulative_filled_qty: f64,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_executed_price: f64,
    #[serde(rename = "n")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub commission: Option<f64>,
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "t")]
    pub trade_id: Option<String>,
    #[serde(rename = "O")]
    pub order_creation_time: i64,
    #[serde(rename = "Z")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub cumulative_quote_qty: Option<f64>,
}

/// Account info - balance updates
#[derive(Deserialize, Debug)]
pub struct AccountInfo {
    #[serde(rename = "e")]
    pub event_type: String, // "outboundAccountInfo"
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub is_trading_allowed: bool,
    #[serde(rename = "W")]
    pub is_withdrawal_allowed: bool,
    #[serde(rename = "D")]
    pub is_deposit_allowed: bool,
    #[serde(rename = "B")]
    pub balances: Vec<BalanceInfo>,
}

#[derive(Deserialize, Debug)]
pub struct BalanceInfo {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "f")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub free: f64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub locked: f64,
}

/// Ticket info - individual trade confirmations
#[derive(Deserialize, Debug)]
pub struct TicketInfo {
    #[serde(rename = "e")]
    pub event_type: String, // "ticketInfo"
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub quantity: f64,
    #[serde(rename = "t")]
    pub timestamp: i64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "T")]
    pub ticket_id: String,
    #[serde(rename = "o")]
    pub order_id: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "a")]
    pub account_id: String,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
}

// ============================================================================
// REST API Request/Response Messages
// ============================================================================

/// Create order request
#[derive(Serialize, Debug)]
pub struct CreateOrderRequest {
    pub symbol: String,
    pub side: String, // "BUY" or "SELL"
    #[serde(rename = "type")]
    pub order_type: String, // "LIMIT", "MARKET", "LIMIT_MAKER"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    #[serde(rename = "newClientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "timeInForce")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    pub timestamp: i64,
    #[serde(rename = "recvWindow")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recv_window: Option<i64>,
}

/// Create order response
#[derive(Deserialize, Debug)]
pub struct CreateOrderResponse {
    #[serde(rename = "accountId")]
    pub account_id: i64,
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "transactTime")]
    pub transact_time: i64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "origQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(rename = "executedQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(rename = "timeInForce")]
    #[serde(deserialize_with = "from_str_to_time_in_force")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
}

/// Cancel order request (query parameters)
#[derive(Serialize, Debug)]
pub struct CancelOrderRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "orderId")]
    pub order_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "clientOrderId")]
    pub client_order_id: Option<String>,
    pub timestamp: i64,
}

/// Cancel order response
#[derive(Deserialize, Debug)]
pub struct CancelOrderResponse {
    #[serde(rename = "accountId")]
    pub account_id: i64,
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(rename = "transactTime")]
    pub transact_time: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "origQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(rename = "executedQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(rename = "timeInForce")]
    #[serde(deserialize_with = "from_str_to_time_in_force")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
}

/// Get open orders response
#[derive(Deserialize, Debug)]
pub struct OpenOrder {
    #[serde(rename = "accountId")]
    pub account_id: i64,
    #[serde(rename = "exchangeId")]
    pub exchange_id: Option<i64>,
    pub symbol: String,
    #[serde(rename = "symbolName")]
    pub symbol_name: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "origQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(rename = "executedQty")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    #[serde(rename = "avgPrice")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub avg_price: Option<f64>,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(rename = "timeInForce")]
    #[serde(deserialize_with = "from_str_to_time_in_force")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_ord_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "stopPrice")]
    #[serde(default)]
    #[serde(deserialize_with = "from_str_to_f64_opt")]
    pub stop_price: Option<f64>,
    pub time: i64,
    #[serde(rename = "updateTime")]
    pub update_time: i64,
    #[serde(rename = "isWorking")]
    pub is_working: bool,
}

/// Listen key response
#[derive(Deserialize, Debug)]
pub struct ListenKeyResponse {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

/// Generic API error response
#[derive(Deserialize, Debug)]
pub struct ApiError {
    pub code: i64,
    pub msg: String,
}
