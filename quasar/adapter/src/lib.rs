//! Quasar protocol adapters.
//!
//! This crate contains the HTTP handlers for the supported catalog
//! protocols:
//!
//! - `iceberg` — Iceberg REST Catalog adapter
//! - `lance` — Lance REST Namespace adapter
//! - `unified` — Unified REST API adapter

#[cfg(feature = "iceberg")]
pub mod iceberg;
#[cfg(feature = "lance")]
pub mod lance;
#[cfg(feature = "iceberg")]
pub mod object_store_util;
#[cfg(feature = "unified")]
pub mod unified;
pub use quasar_core::*;

/// Phase 2 transitional binding. The V3 store trait surface adds a
/// `domain_name` parameter to most operations, but the V3 routing layer
/// that extracts the domain from the request path is Phase 3 work. Until
/// Phase 3 lands, every adapter call uses this single seeded domain (see
/// `schema/init.sql`).
// TODO(v3-phase3): replace `DEFAULT_DOMAIN` with a domain extracted from
// the request path (`{prefix}` for Iceberg, first segment of `{id}` for
// Lance, `{domain}` path segment for Unified).
#[allow(dead_code)]
pub(crate) const DEFAULT_DOMAIN: &str = "default";
