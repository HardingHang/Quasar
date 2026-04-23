//! Quasar protocol adapters.
//!
//! This crate contains the HTTP handlers for the supported catalog
//! protocols:
//!
//! - `iceberg` — Iceberg REST Catalog adapter
//! - `lance` — Lance REST Namespace adapter

#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
pub use quasar_core::*;
