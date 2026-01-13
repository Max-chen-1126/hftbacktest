//! Order Manager for HashKey Connector
//!
//! Manages the mapping between Bot's u64 order_id and HashKey's clientOrderId (String).

use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::{
    prelude::get_precision,
    types::{OrdType, Order, OrderId, Side, Status, TimeInForce},
};

use crate::{
    connector::GetOrders,
    hashkey::{
        HashKeyError,
        msg::{CreateOrderRequest, ExecutionReport},
    },
    utils::generate_rand_string,
};

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

/// Extended order information with symbol
#[derive(Clone, Debug)]
pub struct OrderExt {
    pub symbol: String,
    pub order: Order,
}

/// Order Manager
///
/// Manages:
/// - Mapping between Bot's order_id (u64) and HashKey's clientOrderId (String)
/// - Order state tracking
pub struct OrderManager {
    prefix: String,
    /// Map from clientOrderId -> OrderExt
    orders: HashMap<String, OrderExt>,
    /// Map from (symbol, order_id) -> clientOrderId
    order_id_map: HashMap<(String, OrderId), String>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    /// Generate a unique clientOrderId
    fn generate_client_order_id(&self) -> String {
        // Format: {prefix}{timestamp}_{random}
        // Max length should be <= 255 chars (HashKey limit)
        let timestamp = Utc::now().timestamp_millis();
        format!("{}{}_{}", self.prefix, timestamp, generate_rand_string(8))
    }

    /// Prepare a new order for submission
    ///
    /// Returns the CreateOrderRequest to send to HashKey API
    pub fn prepare_new_order(
        &mut self,
        symbol: &str,
        order: Order,
    ) -> Result<CreateOrderRequest, HashKeyError> {
        let price_prec = get_precision(order.tick_size);
        let client_order_id = self.generate_client_order_id();

        // Convert side
        let side = match order.side {
            Side::Buy => "BUY".to_string(),
            Side::Sell => "SELL".to_string(),
            _ => return Err(HashKeyError::InvalidArg("side")),
        };

        // Convert order type
        let order_type = match order.order_type {
            OrdType::Limit => "LIMIT".to_string(),
            OrdType::Market => "MARKET".to_string(),
            _ => return Err(HashKeyError::InvalidArg("order_type")),
        };

        // Convert time in force
        let time_in_force = match order.time_in_force {
            TimeInForce::GTC => Some("GTC".to_string()),
            TimeInForce::IOC => Some("IOC".to_string()),
            TimeInForce::FOK => Some("FOK".to_string()),
            TimeInForce::GTX => Some("GTC".to_string()), // Post-only, use LIMIT_MAKER type
            _ => return Err(HashKeyError::InvalidArg("time_in_force")),
        };

        // Adjust order type for GTX (post-only)
        let order_type = if order.time_in_force == TimeInForce::GTX {
            "LIMIT_MAKER".to_string()
        } else {
            order_type
        };

        // Calculate price from tick
        let price = order.price_tick as f64 * order.tick_size;

        let request = CreateOrderRequest {
            symbol: symbol.to_string(),
            side,
            order_type,
            quantity: Some(format!("{:.8}", order.qty)),
            price: Some(format!("{:.prec$}", price, prec = price_prec)),
            client_order_id: client_order_id.clone(),
            time_in_force,
            timestamp: Utc::now().timestamp_millis(),
            recv_window: Some(5000),
        };

        // Check for duplicate order
        let key = (symbol.to_string(), order.order_id);
        if self.order_id_map.contains_key(&key) {
            return Err(HashKeyError::OrderAlreadyExist);
        }

        // Store the order
        self.order_id_map.insert(key, client_order_id.clone());
        self.orders.insert(
            client_order_id,
            OrderExt {
                symbol: symbol.to_string(),
                order,
            },
        );

        Ok(request)
    }

    /// Prepare cancel order request
    ///
    /// Returns the clientOrderId to use for cancellation
    pub fn prepare_cancel_order(&self, order_id: OrderId) -> Result<String, HashKeyError> {
        // Find the clientOrderId for this order
        for ((_, oid), client_order_id) in &self.order_id_map {
            if *oid == order_id {
                return Ok(client_order_id.clone());
            }
        }
        Err(HashKeyError::OrderNotFound)
    }

    /// Update order from execution report
    pub fn update_from_execution_report(
        &mut self,
        report: &ExecutionReport,
    ) -> Result<OrderExt, HashKeyError> {
        // Check if this order belongs to us
        if !report.client_order_id.starts_with(&self.prefix) {
            return Err(HashKeyError::PrefixUnmatched);
        }

        let order_ext = self
            .orders
            .get_mut(&report.client_order_id)
            .ok_or(HashKeyError::OrderNotFound)?;

        // Update order state
        order_ext.order.req = Status::None;
        order_ext.order.status = report.order_status;
        order_ext.order.exch_timestamp = report.transaction_time * 1_000_000; // ms -> ns
        order_ext.order.exec_qty = report.last_executed_qty;
        order_ext.order.leaves_qty = order_ext.order.qty - report.cumulative_filled_qty;

        if report.last_executed_price > 0.0 {
            order_ext.order.exec_price_tick =
                (report.last_executed_price / order_ext.order.tick_size).round() as i64;
        }

        // Check if order is still active
        let is_active = order_ext.order.active();
        if !is_active {
            // Remove from maps
            let key = (order_ext.symbol.clone(), order_ext.order.order_id);
            self.order_id_map.remove(&key);
            Ok(self.orders.remove(&report.client_order_id).unwrap())
        } else {
            Ok(order_ext.clone())
        }
    }

    /// Mark order submission as failed
    pub fn update_submit_fail(&mut self, order_id: OrderId) -> Result<OrderExt, HashKeyError> {
        // Find and remove the order
        let client_order_id = {
            let mut found = None;
            for ((_, oid), cid) in &self.order_id_map {
                if *oid == order_id {
                    found = Some(cid.clone());
                    break;
                }
            }
            found.ok_or(HashKeyError::OrderNotFound)?
        };

        let mut order_ext = self
            .orders
            .remove(&client_order_id)
            .ok_or(HashKeyError::OrderNotFound)?;

        order_ext.order.req = Status::None;
        order_ext.order.status = Status::Expired;

        // Remove from order_id_map
        let key = (order_ext.symbol.clone(), order_id);
        self.order_id_map.remove(&key);

        Ok(order_ext)
    }

    /// Mark cancel as failed, restore order state
    pub fn update_cancel_fail(&mut self, order_id: OrderId) -> Result<OrderExt, HashKeyError> {
        // Find the order
        let client_order_id = {
            let mut found = None;
            for ((_, oid), cid) in &self.order_id_map {
                if *oid == order_id {
                    found = Some(cid.clone());
                    break;
                }
            }
            found.ok_or(HashKeyError::OrderNotFound)?
        };

        let order_ext = self
            .orders
            .get_mut(&client_order_id)
            .ok_or(HashKeyError::OrderNotFound)?;

        // Clear the cancel request
        order_ext.order.req = Status::None;

        Ok(order_ext.clone())
    }

    /// Get order by clientOrderId
    pub fn get_order(&self, client_order_id: &str) -> Option<&OrderExt> {
        self.orders.get(client_order_id)
    }

    /// Get order by Bot's order_id
    pub fn get_order_by_id(&self, order_id: OrderId) -> Option<&OrderExt> {
        for ((_, oid), cid) in &self.order_id_map {
            if *oid == order_id {
                return self.orders.get(cid);
            }
        }
        None
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order_ext)| {
                symbol
                    .as_ref()
                    .map(|s| order_ext.symbol == *s)
                    .unwrap_or(true)
                    && order_ext.order.active()
            })
            .map(|(_, order_ext)| order_ext.order.clone())
            .collect()
    }
}
