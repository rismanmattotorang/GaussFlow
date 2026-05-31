//! API module for GaussFlow WebUI
//! Contains shared API types and utilities for consistent API responses

use serde::{Deserialize, Serialize};

/// Standard API error response with structured error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    /// Error code for programmatic handling
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Optional additional error details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    /// Create a new API error with code and message
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    /// Add details to the error
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

/// Common error codes used throughout the API
pub mod error_codes {
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const INVALID_INPUT: &str = "INVALID_INPUT";
    pub const VALIDATION_ERROR: &str = "VALIDATION_ERROR";
    pub const CONFLICT: &str = "CONFLICT";
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
    pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
    pub const FORBIDDEN: &str = "FORBIDDEN";
    pub const RATE_LIMITED: &str = "RATE_LIMITED";
}

/// Pagination parameters for list endpoints
#[derive(Debug, Clone, Deserialize, Default)]
pub struct Pagination {
    /// Page number (0-indexed)
    pub page: Option<usize>,
    /// Number of items per page
    pub limit: Option<usize>,
    /// Field to sort by
    pub sort_by: Option<String>,
    /// Sort order (asc/desc)
    pub sort_order: Option<String>,
}

impl Pagination {
    /// Get the page number with default of 0
    pub fn page(&self) -> usize {
        self.page.unwrap_or(0)
    }

    /// Get the limit with default of 50, max of 100
    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(50).min(100)
    }

    /// Calculate the offset for database queries
    pub fn offset(&self) -> usize {
        self.page() * self.limit()
    }

    /// Check if sorting is ascending
    pub fn is_ascending(&self) -> bool {
        self.sort_order.as_deref().unwrap_or("desc") == "asc"
    }
}

/// Metadata for paginated responses
#[derive(Debug, Clone, Serialize)]
pub struct PaginationMeta {
    /// Current page number
    pub page: usize,
    /// Items per page
    pub limit: usize,
    /// Total number of items
    pub total: usize,
    /// Total number of pages
    pub total_pages: usize,
    /// Whether there are more pages
    pub has_more: bool,
}

impl PaginationMeta {
    /// Create pagination metadata from request and total count
    pub fn new(pagination: &Pagination, total: usize) -> Self {
        let page = pagination.page();
        let limit = pagination.limit();
        let total_pages = total.div_ceil(limit);

        Self {
            page,
            limit,
            total,
            total_pages,
            has_more: page + 1 < total_pages,
        }
    }
}

/// Paginated response wrapper
#[derive(Debug, Clone, Serialize)]
pub struct PaginatedResponse<T> {
    /// Whether the request was successful
    pub success: bool,
    /// The data items
    pub data: Vec<T>,
    /// Pagination metadata
    pub meta: PaginationMeta,
}

impl<T> PaginatedResponse<T> {
    /// Create a new paginated response
    pub fn new(data: Vec<T>, pagination: &Pagination, total: usize) -> Self {
        Self {
            success: true,
            data,
            meta: PaginationMeta::new(pagination, total),
        }
    }
}

/// Filter parameters for list endpoints
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ListFilter {
    /// Filter by status
    pub status: Option<String>,
    /// Search term
    pub search: Option<String>,
    /// Filter by tags (comma-separated)
    pub tags: Option<String>,
    /// Filter by workflow ID (for executions)
    pub workflow_id: Option<String>,
}

impl ListFilter {
    /// Get tags as a vector
    pub fn tags_vec(&self) -> Vec<String> {
        self.tags
            .as_ref()
            .map(|t| {
                t.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Health check response
#[derive(Debug, Clone, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub timestamp: String,
}

/// WebSocket message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage {
    /// Ping message for keepalive
    Ping,
    /// Pong response
    Pong { timestamp: String },
    /// Subscribe to events
    Subscribe { topic: String },
    /// Subscription confirmed
    Subscribed { topic: String },
    /// Request data refresh
    Refresh,
    /// Error message
    Error { code: String, message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagination_defaults() {
        let p = Pagination::default();
        assert_eq!(p.page(), 0);
        assert_eq!(p.limit(), 50);
        assert_eq!(p.offset(), 0);
    }

    #[test]
    fn test_pagination_limit_cap() {
        let p = Pagination {
            limit: Some(200),
            ..Default::default()
        };
        assert_eq!(p.limit(), 100);
    }

    #[test]
    fn test_api_error_display() {
        let err = ApiError::new("TEST_ERROR", "Test message");
        assert_eq!(format!("{}", err), "[TEST_ERROR] Test message");
    }
}
