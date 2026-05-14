//! Quasar protocol adapters.
//!
//! This crate contains the HTTP handlers for the supported catalog
//! protocols:
//!
//! - `iceberg` — Iceberg REST Catalog adapter
//! - `lance` — Lance REST Namespace adapter
//! - `unified` — Unified REST API adapter

mod format;
#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
#[cfg(feature = "iceberg")]
pub mod object_store_util;
#[cfg(feature = "unified")]
pub mod unified;

pub use format::{AssetFormat, AssetType};
pub use quasar_core::*;
