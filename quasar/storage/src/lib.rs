//! Quasar PostgreSQL storage implementation.
//!
//! Provides `PgCatalogStore`, a PostgreSQL-backed implementation of the
//! `CatalogStore` trait defined in `quasar-core`.

pub mod schema;
pub mod store;

pub use quasar_core::*;
pub use store::PgCatalogStore;
