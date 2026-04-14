//! DuckFlock Core — shared types, traits, and configuration.
//!
//! This crate defines the plugin interfaces that make DuckFlock extensible:
//! - [`AuthProvider`] — pluggable authentication
//! - [`AuditLogger`] — pluggable query audit logging
//! - [`CatalogSource`] — pluggable catalog discovery
//!
//! It also contains the configuration schema for `duckflock.yaml`.

pub mod audit;
pub mod auth;
pub mod catalog;
pub mod config;
pub mod error;
pub mod identity;
