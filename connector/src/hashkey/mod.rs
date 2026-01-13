use std::{
    collections::HashSet,
    num::{ParseFloatError, ParseIntError},
    sync::{Arc, Mutex},
};

use hftbacktest::types::{ErrorKind, LiveError, LiveEvent, Order, Value};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender};
use tracing::error;

use crate::{
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    hashkey::{
        ordermanager::{OrderManager, SharedOrderManager},
        public_stream::PublicStream,
        rest::HashKeyClient,
    },
    utils::{ExponentialBackoff, Retry},
};

pub mod msg;
pub mod ordermanager;
pub mod private_stream;
pub mod public_stream;
pub mod rest;

#[derive(Error, Debug)]
pub enum HashKeyError {
    #[error("AssetNotFound")]
    AssetNotFound,
    #[error("AuthError: {code} - {msg}")]
    AuthError { code: i64, msg: String },
    #[error("OrderError: {code} - {msg}")]
    OrderError { code: i64, msg: String },
    #[error("InvalidPxQty: {0}")]
    InvalidPxQty(#[from] ParseFloatError),
    #[error("InvalidOrderId: {0}")]
    InvalidOrderId(ParseIntError),
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("InvalidReqId")]
    InvalidReqId,
    #[error("InvalidArg: {0}")]
    InvalidArg(&'static str),
    #[error("OrderAlreadyExist")]
    OrderAlreadyExist,
    #[error("Serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Reqwest: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Tungstenite: {0}")]
    Tungstenite(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("OpError: {0}")]
    OpError(String),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
    #[error("ListenKeyError: {0}")]
    ListenKeyError(String),
}

impl HashKeyError {
    pub fn to_value(&self) -> Value {
        match self {
            HashKeyError::AssetNotFound => Value::Empty,
            HashKeyError::AuthError { code, msg } => Value::Map({
                let mut map = std::collections::HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            HashKeyError::OrderError { code, msg } => Value::Map({
                let mut map = std::collections::HashMap::new();
                map.insert("code".to_string(), Value::Int(*code));
                map.insert("msg".to_string(), Value::String(msg.clone()));
                map
            }),
            _ => Value::String(self.to_string()),
        }
    }
}

/// HashKey Connector configuration
///
/// Example config.toml:
/// ```toml
/// public_ws_url = "wss://stream-pro.hashkey.com/quote/ws/v1"
/// private_ws_url = "wss://stream-pro.hashkey.com/api/v1/ws"
/// rest_url = "https://api-pro.hashkey.com"
/// api_key = "your_api_key"
/// secret = "your_secret"
/// order_prefix = "HK"
/// ```
#[derive(Deserialize, Clone)]
pub struct Config {
    /// WebSocket URL for public market data
    /// Sandbox: wss://stream-pro.sim.hashkeydev.com/quote/ws/v1
    /// Production: wss://stream-pro.hashkey.com/quote/ws/v1
    pub public_ws_url: String,

    /// WebSocket URL for private data (without listenKey)
    /// Sandbox: wss://stream-pro.sim.hashkeydev.com/api/v1/ws
    /// Production: wss://stream-pro.hashkey.com/api/v1/ws
    pub private_ws_url: String,

    /// REST API base URL
    /// Sandbox: https://api-pro.sim.hashkeydev.com
    /// Production: https://api-pro.hashkey.com
    pub rest_url: String,

    /// API Key (x-access-key header)
    pub api_key: String,

    /// API Secret for HMAC-SHA256 signing
    pub secret: String,

    /// Order ID prefix for identifying orders created by this connector
    pub order_prefix: String,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

pub struct HashKey {
    config: Config,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    client: HashKeyClient,
    symbol_tx: Sender<String>,
}

impl HashKey {
    fn connect_public_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let public_ws_url = self.config.public_ws_url.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: HashKeyError| {
                    error!(?error, "An error occurred in the public stream connection.");
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = PublicStream::new(ev_tx.clone(), symbol_tx.subscribe());
                    if let Err(error) = stream.connect(&public_ws_url).await {
                        error!(?error, "A connection error occurred.");
                        ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::ConnectionInterrupted,
                                error.to_value(),
                            ))))
                            .unwrap();
                    } else {
                        ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::new(
                                ErrorKind::ConnectionInterrupted,
                            ))))
                            .unwrap();
                    }
                    Err::<(), HashKeyError>(HashKeyError::ConnectionInterrupted)
                })
                .await;
        });
    }

    fn connect_private_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let private_ws_url = self.config.private_ws_url.clone();
        let order_manager = self.order_manager.clone();
        let symbols = self.symbols.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: HashKeyError| {
                    error!(
                        ?error,
                        "An error occurred in the private stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.to_value(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = private_stream::PrivateStream::new(
                        ev_tx.clone(),
                        order_manager.clone(),
                        symbols.clone(),
                        client.clone(),
                        symbol_tx.subscribe(),
                    );
                    stream.connect(&private_ws_url).await?;
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for HashKey {
    type Error = HashKeyError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;

        if config.order_prefix.contains("/") {
            panic!("order prefix cannot include '/'.");
        }
        if config.order_prefix.len() > 8 {
            panic!("order prefix length should be not greater than 8.");
        }

        let (symbol_tx, _) = broadcast::channel(500);
        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let client = HashKeyClient::new(
            &config.rest_url,
            &config.api_key,
            &config.secret,
        );

        Ok(HashKey {
            config,
            order_manager,
            client,
            symbols: Default::default(),
            symbol_tx,
        })
    }
}

impl Connector for HashKey {
    fn register(&mut self, symbol: String) {
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_public_stream(ev_tx.clone());
        self.connect_private_stream(ev_tx);
    }

    fn submit(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        let order_manager = self.order_manager.clone();
        let client = self.client.clone();

        // Convert symbol format: BTC_USDT -> BTCUSDT
        let symbol = asset.replace("_", "");

        tokio::spawn(async move {
            // Prepare order inside a block to release the lock before await
            let prepare_result = {
                order_manager
                    .lock()
                    .unwrap()
                    .prepare_new_order(&symbol, order.clone())
            };

            match prepare_result {
                Ok(hashkey_order) => {
                    match client.create_order(hashkey_order).await {
                        Ok(response) => {
                            // Order submitted successfully, update will come via private stream
                            tracing::debug!(?response, "Order submitted successfully");
                        }
                        Err(error) => {
                            // Mark order as failed
                            let fail_result = {
                                order_manager.lock().unwrap().update_submit_fail(order.order_id)
                            };
                            if let Ok(order_ext) = fail_result {
                                ev_tx
                                    .send(PublishEvent::LiveEvent(LiveEvent::Order {
                                        symbol: order_ext.symbol,
                                        order: order_ext.order,
                                    }))
                                    .unwrap();
                            }
                            ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                    ErrorKind::OrderError,
                                    error.to_value(),
                                ))))
                                .unwrap();
                        }
                    }
                }
                Err(error) => {
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::OrderError,
                            error.to_value(),
                        ))))
                        .unwrap();
                }
            }
        });
    }

    fn cancel(&self, asset: String, order: Order, ev_tx: UnboundedSender<PublishEvent>) {
        let order_manager = self.order_manager.clone();
        let client = self.client.clone();

        // Convert symbol format: BTC_USDT -> BTCUSDT
        let symbol = asset.replace("_", "");

        tokio::spawn(async move {
            // Prepare cancel inside a block to release the lock before await
            let prepare_result = {
                order_manager.lock().unwrap().prepare_cancel_order(order.order_id)
            };

            match prepare_result {
                Ok(client_order_id) => {
                    match client.cancel_order(&symbol, &client_order_id).await {
                        Ok(_response) => {
                            tracing::debug!("Order cancelled successfully");
                        }
                        Err(error) => {
                            // Mark cancel as failed, restore order state
                            let fail_result = {
                                order_manager.lock().unwrap().update_cancel_fail(order.order_id)
                            };
                            if let Ok(order_ext) = fail_result {
                                ev_tx
                                    .send(PublishEvent::LiveEvent(LiveEvent::Order {
                                        symbol: order_ext.symbol,
                                        order: order_ext.order,
                                    }))
                                    .unwrap();
                            }
                            ev_tx
                                .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                    ErrorKind::OrderError,
                                    error.to_value(),
                                ))))
                                .unwrap();
                        }
                    }
                }
                Err(error) => {
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::OrderError,
                            error.to_value(),
                        ))))
                        .unwrap();
                }
            }
        });
    }
}
