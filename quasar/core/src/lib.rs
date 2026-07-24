//! Quasar core domain models and trait definitions.
//!
//! This crate contains the protocol-agnostic data structures and the
//! store traits that separate the domain layer from the persistence
//! layer. It depends on no framework or storage implementation.

pub mod error;
pub mod models;
pub mod store;
pub mod validation;

pub use error::CatalogError;
pub use models::{
    Asset, AssetType, AssetVersion, AssetWithTabular, Domain, Format, Namespace, PatchField,
    TabularAsset, View, ViewAsset, ViewIdentifier,
};
pub use store::{
    AssetFilter, AssetPatch, AssetQuery, AssetStore, AssetTypeStore, CasCommitStore, CatalogStore,
    CreateAsset, CreateDomain, CreateNamespace, CreateVersion, DomainPatch, DomainStore,
    IcebergCatalogStore, IcebergMetricsStore, IcebergPurgeStore, IcebergRegisterStore,
    IcebergStagingStore, IcebergTableCommit, IcebergTransactionStore, IcebergViewStore,
    NamespacePatch, NamespaceStore, RegisterAssetType, RegisterFormat, TabularStore, TagStore,
    UnifiedQueryStore, VersionStore,
};
pub use validation::{validate_name, validate_namespace_path};
