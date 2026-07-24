//! Quasar PostgreSQL storage implementation.
//!
//! Provides `PgCatalogStore`, a PostgreSQL-backed implementation of the
//! store traits defined in `quasar-core`, plus a lightweight embedded
//! migration runner (`migrations/`, DESIGN §8.4).

pub(crate) mod migrate;
pub(crate) mod queries;
pub mod store;

pub use quasar_core::*;
pub use store::PgCatalogStore;
