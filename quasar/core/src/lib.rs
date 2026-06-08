//! Quasar core domain models and trait definitions.
//!
//! This crate contains the protocol-agnostic data structures and the
//! `CatalogStore` trait that separates the domain layer from the
//! persistence layer.

pub mod error;
pub mod metrics;
pub mod models;
pub mod store;
pub mod validation;

pub use error::StoreError;
pub use metrics::{Counter, MetricsRegistry, MetricsState};
pub use models::{
    Asset, AssetVersion, AssetVersionWithTabular, AssetWithTabular, Domain, Namespace, PatchField,
    TabularAsset, TabularAssetVersion, View, ViewAsset, ViewIdentifier,
};
pub use store::{
    AssetStore, CasCommitStore, CatalogStore, DomainPatch, DomainStore, IcebergCatalogStore,
    IcebergMetricsStore, IcebergPurgeStore, IcebergRegisterStore, IcebergStagingStore,
    IcebergTransactionStore, IcebergViewStore, NamespaceStore, TabularStore, TabularVersionStore,
    UnifiedQueryStore, VersionStore,
};
pub use validation::validate_name;
