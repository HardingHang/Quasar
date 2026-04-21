//! Quasar protocol adapters.
//!
//! This crate contains the HTTP handlers for the supported catalog
//! protocols:
//!
//! - `iceberg` — Iceberg REST Catalog adapter
//! - `lance` — Lance REST Namespace adapter

pub mod iceberg;
pub mod lance;
pub use quasar_core::*;
