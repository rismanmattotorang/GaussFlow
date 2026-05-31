//! # GaussFlow Security
//!
//! Building blocks for securing GaussFlow's API and data path:
//! - [`auth`] — JWT (HS256) minting/verification with the signing secret sourced from the
//!   environment and **never defaulted** (auth is "off-by-misconfiguration", never silently open).
//! - [`rbac`] — role-based authorization for API actions.
//! - [`audit`] — an append-only, **tamper-evident** (hash-chained) audit log.
//! - [`pii`] — redaction of likely-PII (emails) from JSON flowing through logs/storage.

pub mod audit;
pub mod auth;
pub mod pii;
pub mod rbac;

pub use audit::{AuditEvent, AuditLog};
pub use auth::{verify, AuthError, Claims};
pub use rbac::{authorize, Action, Role};
