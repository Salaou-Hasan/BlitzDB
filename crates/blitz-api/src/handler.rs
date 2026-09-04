use crate::error::{ApiError, ApiResult};
use blitz_types::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP-like method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

/// An incoming API request.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    pub method: HttpMethod,
    pub path: String,
    pub params: HashMap<String, Value>,
    pub body: Option<Value>,
    pub headers: HashMap<String, String>,
}

impl ApiRequest {
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Get,
            path: path.into(),
            params: HashMap::new(),
            body: None,
            headers: HashMap::new(),
        }
    }

    pub fn post(path: impl Into<String>, body: Value) -> Self {
        Self {
            method: HttpMethod::Post,
            path: path.into(),
            params: HashMap::new(),
            body: Some(body),
            headers: HashMap::new(),
        }
    }

    pub fn put(path: impl Into<String>, body: Value) -> Self {
        Self {
            method: HttpMethod::Put,
            path: path.into(),
            params: HashMap::new(),
            body: Some(body),
            headers: HashMap::new(),
        }
    }

    pub fn delete(path: impl Into<String>) -> Self {
        Self {
            method: HttpMethod::Delete,
            path: path.into(),
            params: HashMap::new(),
            body: None,
            headers: HashMap::new(),
        }
    }

    pub fn with_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.params.insert(key.into(), value);
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// An API response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse {
    pub status: u16,
    pub body: Option<Value>,
    pub error: Option<String>,
}

impl ApiResponse {
    pub fn ok(body: Value) -> Self {
        Self {
            status: 200,
            body: Some(body),
            error: None,
        }
    }

    pub fn created(body: Value) -> Self {
        Self {
            status: 201,
            body: Some(body),
            error: None,
        }
    }

    pub fn no_content() -> Self {
        Self {
            status: 204,
            body: None,
            error: None,
        }
    }

    pub fn bad_request(error: impl Into<String>) -> Self {
        Self {
            status: 400,
            body: None,
            error: Some(error.into()),
        }
    }

    pub fn not_found(error: impl Into<String>) -> Self {
        Self {
            status: 404,
            body: None,
            error: Some(error.into()),
        }
    }

    pub fn internal(error: impl Into<String>) -> Self {
        Self {
            status: 500,
            body: None,
            error: Some(error.into()),
        }
    }
}

/// A route handler function.
pub type RouteHandler = Box<dyn Fn(&ApiRequest) -> ApiResponse + Send + Sync>;

/// API handler with route matching.
pub struct ApiHandler {
    routes: Vec<(HttpMethod, String, RouteHandler)>,
}

impl ApiHandler {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
        }
    }

    /// Register a GET route.
    pub fn get(&mut self, path: impl Into<String>, handler: RouteHandler) {
        self.routes
            .push((HttpMethod::Get, path.into(), handler));
    }

    /// Register a POST route.
    pub fn post(&mut self, path: impl Into<String>, handler: RouteHandler) {
        self.routes
            .push((HttpMethod::Post, path.into(), handler));
    }

    /// Register a PUT route.
    pub fn put(&mut self, path: impl Into<String>, handler: RouteHandler) {
        self.routes
            .push((HttpMethod::Put, path.into(), handler));
    }

    /// Register a DELETE route.
    pub fn delete(&mut self, path: impl Into<String>, handler: RouteHandler) {
        self.routes
            .push((HttpMethod::Delete, path.into(), handler));
    }

    /// Handle an incoming request.
    pub fn handle(&self, request: &ApiRequest) -> ApiResponse {
        for (method, path, handler) in &self.routes {
            if *method == request.method && *path == request.path {
                return handler(request);
            }
        }
        ApiResponse::not_found(format!("no route for {} {}", request.method_str(), request.path))
    }

    /// Get the number of registered routes.
    pub fn route_count(&self) -> usize {
        self.routes.len()
    }
}

impl Default for ApiHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpMethod {
    pub fn as_str(&self) -> &str {
        match self {
            HttpMethod::Get => "GET",
            HttpMethod::Post => "POST",
            HttpMethod::Put => "PUT",
            HttpMethod::Delete => "DELETE",
        }
    }
}

impl ApiRequest {
    pub fn method_str(&self) -> &str {
        self.method.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_builders() {
        let req = ApiRequest::get("/users")
            .with_param("page", Value::Int64(1));
        assert_eq!(req.method, HttpMethod::Get);
        assert_eq!(req.path, "/users");

        let req = ApiRequest::post("/users", Value::Null);
        assert_eq!(req.method, HttpMethod::Post);

        let req = ApiRequest::put("/users/1", Value::Null);
        assert_eq!(req.method, HttpMethod::Put);

        let req = ApiRequest::delete("/users/1");
        assert_eq!(req.method, HttpMethod::Delete);
    }

    #[test]
    fn test_response_builders() {
        let resp = ApiResponse::ok(Value::String("hello".into()));
        assert_eq!(resp.status, 200);

        let resp = ApiResponse::created(Value::Null);
        assert_eq!(resp.status, 201);

        let resp = ApiResponse::no_content();
        assert_eq!(resp.status, 204);

        let resp = ApiResponse::bad_request("invalid input");
        assert_eq!(resp.status, 400);
        assert_eq!(resp.error.as_deref(), Some("invalid input"));

        let resp = ApiResponse::not_found("not found");
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_routing() {
        let mut api = ApiHandler::new();
        api.get(
            "/users",
            Box::new(|_| ApiResponse::ok(Value::String("user list".into()))),
        );
        api.post(
            "/users",
            Box::new(|_| ApiResponse::created(Value::String("user created".into()))),
        );

        assert_eq!(api.route_count(), 2);

        let resp = api.handle(&ApiRequest::get("/users"));
        assert_eq!(resp.status, 200);

        let resp = api.handle(&ApiRequest::post("/users", Value::Null));
        assert_eq!(resp.status, 201);

        let resp = api.handle(&ApiRequest::get("/missing"));
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn test_method_as_str() {
        assert_eq!(HttpMethod::Get.as_str(), "GET");
        assert_eq!(HttpMethod::Post.as_str(), "POST");
        assert_eq!(HttpMethod::Put.as_str(), "PUT");
        assert_eq!(HttpMethod::Delete.as_str(), "DELETE");
    }
}
