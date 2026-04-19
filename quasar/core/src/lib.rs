//! Quasar core domain models and trait definitions.
//!
//! This crate contains the protocol-agnostic data structures and the
//! `CatalogStore` trait that separates the domain layer from the
//! persistence layer.

pub mod error;
pub mod models;
pub mod store;

pub use error::StoreError;
pub use models::{Asset, AssetCommitUpdate, AssetFormat, AssetVersion, Namespace};
pub use store::CatalogStore;
