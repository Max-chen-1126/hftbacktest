//! HashKey REST API client
//!
//! Handles authentication and API calls to HashKey Exchange.
//! Reference: https://hashkeypro-apidoc.readme.io/reference/authentication

use std::collections::BTreeMap;

use chrono::Utc;
use serde::de::DeserializeOwned;

use crate::hashkey::{
    HashKeyError,
    msg::{
        ApiError, CancelOrderResponse, CreateOrderRequest, CreateOrderResponse,
        ListenKeyResponse, OpenOrder,
    },
};

/// HashKey REST API client
#[derive(Clone)]
pub struct HashKeyClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    secret: String,
}

impl HashKeyClient {
    pub fn new(base_url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(10)
                .build()
                .unwrap(),
            base_url: base_url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
        }
    }

    /// Generate signature for HashKey API
    ///
    /// HashKey signing process:
    /// 1. Combine all parameters (request + headers) into JSON
    /// 2. Sort by key (ASCII)
    /// 3. UTF-8 encode
    /// 4. HMAC-SHA256 with hex-decoded secret
    /// 5. Base64 encode the result
    fn sign(&self, params: &BTreeMap<String, String>, timestamp: i64) -> String {
        use base64::Engine;
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        // Build the signing map with headers included
        let mut sign_map = params.clone();
        sign_map.insert("x-access-key".to_string(), self.api_key.clone());
        sign_map.insert("x-access-timestamp".to_string(), timestamp.to_string());
        sign_map.insert("x-access-version".to_string(), "1".to_string());

        // Sort and build JSON string
        let json_str = serde_json::to_string(&sign_map).unwrap();

        // Decode hex secret to bytes
        let secret_bytes = hex::decode(&self.secret).unwrap_or_else(|_| self.secret.as_bytes().to_vec());

        // HMAC-SHA256
        let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
            .expect("HMAC can take key of any size");
        mac.update(json_str.as_bytes());
        let result = mac.finalize();

        // Base64 encode
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Build common headers for authenticated requests
    fn build_headers(&self, signature: &str, timestamp: i64) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        headers.insert("x-access-key", self.api_key.parse().unwrap());
        headers.insert("x-access-sign", signature.parse().unwrap());
        headers.insert("x-access-timestamp", timestamp.to_string().parse().unwrap());
        headers.insert("x-access-version", "1".parse().unwrap());
        headers
    }

    /// Generic POST request
    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        params: BTreeMap<String, String>,
    ) -> Result<T, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let signature = self.sign(&params, timestamp);
        let headers = self.build_headers(&signature, timestamp);

        let url = format!("{}{}", self.base_url, path);
        let body = serde_json::to_string(&params)?;

        let response = self
            .client
            .post(&url)
            .headers(headers)
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<ApiError>(&text) {
                return Err(HashKeyError::OrderError {
                    code: error.code,
                    msg: error.msg,
                });
            }
            return Err(HashKeyError::OpError(text));
        }

        serde_json::from_str(&text).map_err(HashKeyError::from)
    }

    /// Generic DELETE request
    async fn delete<T: DeserializeOwned>(
        &self,
        path: &str,
        params: BTreeMap<String, String>,
    ) -> Result<T, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let signature = self.sign(&params, timestamp);
        let headers = self.build_headers(&signature, timestamp);

        // Build query string
        let query_string: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let url = format!("{}{}?{}", self.base_url, path, query_string);

        let response = self
            .client
            .delete(&url)
            .headers(headers)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<ApiError>(&text) {
                return Err(HashKeyError::OrderError {
                    code: error.code,
                    msg: error.msg,
                });
            }
            return Err(HashKeyError::OpError(text));
        }

        serde_json::from_str(&text).map_err(HashKeyError::from)
    }

    /// Generic GET request
    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: BTreeMap<String, String>,
    ) -> Result<T, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let signature = self.sign(&params, timestamp);
        let headers = self.build_headers(&signature, timestamp);

        // Build query string
        let query_string: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let url = if query_string.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, query_string)
        };

        let response = self
            .client
            .get(&url)
            .headers(headers)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            if let Ok(error) = serde_json::from_str::<ApiError>(&text) {
                return Err(HashKeyError::OrderError {
                    code: error.code,
                    msg: error.msg,
                });
            }
            return Err(HashKeyError::OpError(text));
        }

        serde_json::from_str(&text).map_err(HashKeyError::from)
    }

    // ========================================================================
    // Order Management APIs
    // ========================================================================

    /// Create a new order
    /// POST /api/v1/spot/order
    pub async fn create_order(
        &self,
        request: CreateOrderRequest,
    ) -> Result<CreateOrderResponse, HashKeyError> {
        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), request.symbol);
        params.insert("side".to_string(), request.side);
        params.insert("type".to_string(), request.order_type);
        params.insert("newClientOrderId".to_string(), request.client_order_id);
        params.insert("timestamp".to_string(), request.timestamp.to_string());

        if let Some(qty) = request.quantity {
            params.insert("quantity".to_string(), qty);
        }
        if let Some(price) = request.price {
            params.insert("price".to_string(), price);
        }
        if let Some(tif) = request.time_in_force {
            params.insert("timeInForce".to_string(), tif);
        }
        if let Some(recv_window) = request.recv_window {
            params.insert("recvWindow".to_string(), recv_window.to_string());
        }

        self.post("/api/v1/spot/order", params).await
    }

    /// Cancel an order
    /// DELETE /api/v1/spot/order
    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<CancelOrderResponse, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("clientOrderId".to_string(), client_order_id.to_string());
        params.insert("timestamp".to_string(), timestamp.to_string());

        self.delete("/api/v1/spot/order", params).await
    }

    /// Cancel order by exchange order ID
    pub async fn cancel_order_by_id(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<CancelOrderResponse, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let mut params = BTreeMap::new();
        params.insert("symbol".to_string(), symbol.to_string());
        params.insert("orderId".to_string(), order_id.to_string());
        params.insert("timestamp".to_string(), timestamp.to_string());

        self.delete("/api/v1/spot/order", params).await
    }

    /// Get open orders
    /// GET /api/v1/spot/openOrders
    pub async fn get_open_orders(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<OpenOrder>, HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let mut params = BTreeMap::new();
        params.insert("timestamp".to_string(), timestamp.to_string());

        if let Some(s) = symbol {
            params.insert("symbol".to_string(), s.to_string());
        }

        self.get("/api/v1/spot/openOrders", params).await
    }

    // ========================================================================
    // User Data Stream APIs
    // ========================================================================

    /// Create a listen key for private WebSocket
    /// POST /api/v1/userDataStream
    pub async fn create_listen_key(&self) -> Result<ListenKeyResponse, HashKeyError> {
        let params = BTreeMap::new();
        self.post("/api/v1/userDataStream", params).await
    }

    /// Keep alive listen key (extend validity)
    /// PUT /api/v1/userDataStream
    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<(), HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let signature = self.sign(&BTreeMap::new(), timestamp);
        let headers = self.build_headers(&signature, timestamp);

        let url = format!(
            "{}/api/v1/userDataStream?listenKey={}",
            self.base_url, listen_key
        );

        let response = self.client.put(&url).headers(headers).send().await?;

        if !response.status().is_success() {
            let text = response.text().await?;
            return Err(HashKeyError::ListenKeyError(text));
        }

        Ok(())
    }

    /// Delete listen key
    /// DELETE /api/v1/userDataStream
    pub async fn delete_listen_key(&self, listen_key: &str) -> Result<(), HashKeyError> {
        let timestamp = Utc::now().timestamp_millis();
        let signature = self.sign(&BTreeMap::new(), timestamp);
        let headers = self.build_headers(&signature, timestamp);

        let url = format!(
            "{}/api/v1/userDataStream?listenKey={}",
            self.base_url, listen_key
        );

        let response = self.client.delete(&url).headers(headers).send().await?;

        if !response.status().is_success() {
            let text = response.text().await?;
            return Err(HashKeyError::ListenKeyError(text));
        }

        Ok(())
    }
}
