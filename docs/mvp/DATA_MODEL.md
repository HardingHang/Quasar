# Quasar 数据模型设计

## 设计原则

### 1. 格式无关的核心抽象

核心实体（Namespace、Asset）不绑定任何具体数据格式。格式差异通过 Namespace 级的 `format` 标识字段和 Asset 级的 `properties` 扩展字段消化，而非为每种格式单独建立实体或表结构。

### 2. 指针而非内容

Catalog 只存储指向实际数据/元数据的指针（Iceberg 的 `metadata_location`，Lance 的 `location` / `manifest_path`），不存储实际数据文件内容。这确保了 Catalog 服务的轻量级和高性能。

### 3. 扁平化层级

采用 `Namespace → Asset` 的两层结构，而非业界常见的多层嵌套模型。MVP 阶段限定单级 Namespace，但数据库层面已预留多级扩展能力。

### 4. 自包含存储

所有元数据直接持久化在 PostgreSQL 中，不依赖外部元数据系统（如 HMS、另一个 Iceberg REST Catalog）。协议适配器直接操作本地存储，避免 connector 路由带来的额外网络跳转和复杂度。

### 5. 并发安全

Iceberg 的乐观并发控制通过数据库级别的条件更新实现；Lance 的版本唯一性约束防止重复注册。两种机制都利用 PostgreSQL 的原子性保证，无需引入外部协调服务。

### 6. 预留扩展位

所有 JSONB 字段和 `format` 枚举为未来新增资产类型预留空间，不修改表结构即可支持新格式。

---

## 与业界方案的对比

### 对比维度：层级设计

| 方案 | 层级结构 | 最小路径 |
|------|---------|----------|
| **Apache Gravitino** | `Metalake → Catalog → Schema → Table` | `metalake.catalog.schema.table` |
| **Unity Catalog** | `Catalog → Schema → Table` | `catalog.schema.table` |
| **Hive Metastore** | `Database → Table` | `database.table` |
| **Quasar** | `Namespace → Asset` | `namespace.asset` |

Gravitino 需要 `Metalake` 做租户边界、`Catalog` 做后端路由，因为它要同时连接 HMS、MySQL、Iceberg REST、文件系统等异构后端。`Catalog` 存储连接底层系统所需的配置（如 HMS URI、JDBC 连接串、Iceberg REST endpoint），并通过 connector 将请求路由到对应的后端。

Quasar 不需要这些抽象层，因为：
- **没有异构后端**。所有元数据直接存在 PostgreSQL 中，不存在"通过 connector 路由到外部系统"的场景。
- **协议即入口**。Iceberg REST 和 Lance REST 的请求路径天然区分了格式（`/iceberg/` vs `/lance/`），无需额外的 `Catalog` 层来标识后端类型。
- **配置去中心化**。Warehouse 路径、存储凭证等配置通过全局环境变量或 Namespace 级 `properties` 管理，不需要在 Catalog 层集中绑定。

这带来的直接收益是路径更短、查询更简单、部署更轻量。

### 对比维度：格式划分策略

| 方案 | 格式标识位置 | 同名表处理方式 |
|------|-------------|---------------|
| **Gravitino** | `Catalog.type`（一个 Catalog 只绑定一种格式） | `prod_hive.users` 和 `prod_iceberg.users` 分布在不同 Catalog 下 |
| **Quasar** | `Namespace.format` 字段（一个 Namespace 只绑定一种格式） | Iceberg 的 `prod.users` 和 Lance 的 `prod.users` 在各自格式的 Namespace 下，通过 `format` 隔离 |

Gravitino 按 Catalog 划分格式的优势在于配置集中（一个 Catalog 的 S3 凭证、warehouse 路径配一次即可），且权限可以粗粒度地按 Catalog 控制。但这增加了层级深度，同一业务的数据可能分散在不同 Catalog 中。

Quasar 将 `format` 放在 Namespace 层的考量：
- **与协议规范对齐**。Iceberg REST Catalog Spec 和 Lance REST Namespace 规范中的 "Catalog" 概念就是整个服务实例本身，不是 Namespace 和 Table 之间的一层。Quasar 遵循这种扁平语义。
- **协议天然隔离**。`/iceberg/` 和 `/lance/` 路径前缀天然将两种格式的 Namespace 空间分开，各适配器在查询时自动注入 `format` 条件，无需 Asset 层再冗余一个 `format` 字段。
- **减少认知负担**。用户只需要理解 Namespace（类似数据库）和 Asset（类似表），不需要理解 Catalog 是什么、该选哪个 Catalog。

代价是配置粒度更细（每个 Namespace 可能需单独配置 storage_options），且不能像 Gravitino 那样"一键禁止访问所有 Iceberg 表"。这些代价在 Quasar 的定位（简洁、高性能、单团队/单项目使用）下是可接受的。

### 对比维度：存储架构

| 方案 | 存储方式 | 元数据流向 |
|------|---------|-----------|
| **Gravitino** | 代理模式。`Table` 对象存储指向底层系统的指针，真实元数据在 HMS/Iceberg REST/JDBC 中 | `Client → Gravitino → Connector → 底层 Catalog → 返回` |
| **Lakekeeper** | 自包含。所有 Iceberg 元数据直接存在 PostgreSQL 中 | `Client → Lakekeeper → PostgreSQL → 返回` |
| **Quasar** | 自包含。所有格式的元数据直接存在 PostgreSQL 中 | `Client → Quasar → PostgreSQL → 返回` |

Quasar 选择与 Lakekeeper 类似的自包含存储路径，而非 Gravitino 的联邦代理模式。原因在于：
- **性能优先**。少一层网络跳转（没有 connector 到外部系统的 RPC），元数据查询延迟更低。
- **部署简洁**。单二进制 + PostgreSQL 即可运行，不需要额外部署 HMS、Gravitino Server、多个 connector。
- **直接实现协议**。Iceberg REST 的 requirements 校验直接在 PostgreSQL 事务中完成，不 delegating 给底层系统，语义更可控。

代价是失去了 Gravitino 的"联邦统一治理"能力——Quasar 不能同时连接已有的 HMS 和 Iceberg Catalog 做统一管理。这是定位取舍：Quasar 是独立的元数据服务，不是元数据联邦控制面。

---

## 实体关系

```
Namespace (1) ──────< (N) Asset (1) ──────< (N) AssetVersion

- 一个 Namespace 下有多张 Asset（表）
- 一张 Asset（Lance 格式）有多个版本记录
- Iceberg 格式的 Asset 不创建 AssetVersion 记录（版本信息在 metadata.json 中自描述）
- Namespace 仅允许在为空（无下属 Asset）时删除（ON DELETE RESTRICT）
- Asset 删除时级联删除其下所有 AssetVersion 记录
- Asset 的格式由所属 Namespace 的 format 决定，Asset 自身不重复存储 format
```

---

## 核心实体

### Namespace

资产的逻辑分组容器。MVP 阶段限定为单级，但数据库层面预留多级扩展能力。

```rust
pub struct Namespace {
    pub id: Uuid,
    pub name: String,           // Namespace 名称，如 "prod"
    pub format: AssetFormat,    // iceberg | lance
    pub properties: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub enum AssetFormat {
    #[serde(rename = "iceberg")]
    Iceberg,
    #[serde(rename = "lance")]
    Lance,
}
```

**设计要点**：
- `format` 字段确保 Iceberg 和 Lance 的 Namespace 空间逻辑隔离。即使名称相同（如都叫 `prod`），因 `format` 不同而视为两个独立 Namespace。这与 Iceberg REST Spec 和 Lance REST Namespace 规范的路径前缀（`/iceberg/` vs `/lance/`）语义一致。
- `format` 仅在 Namespace 层定义，Asset 继承所属 Namespace 的 format，不冗余存储。这消除了 Asset 的 format 与 Namespace 的 format 不一致的风险。
- `properties` 存储 Namespace 级别的配置属性，如 `comment`、`location` 前缀等。不同格式可定义各自的属性约定。

---

### Asset

统一的资产实体，代表一张表（Iceberg Table 或 Lance Table）。

```rust
pub struct Asset {
    pub id: Uuid,
    pub namespace_id: Uuid,     // 外键 → namespaces.id

    pub name: String,           // 表名，如 "users"

    // 表的数据基础路径（两种格式语义一致）：
    // - Iceberg: 表的 warehouse 路径（s3://bucket/warehouse/prod/users/）
    // - Lance: 数据集的基础路径（s3://bucket/warehouse/prod/users.lance/）
    pub location: String,

    // Iceberg 专用：当前生效的 metadata.json 文件 URI
    // CAS 校验的直接比较目标（commit 时断言此值未变）
    // Lance 表此字段为 NULL
    pub metadata_location: Option<String>,

    // Schema 快照（格式相关的 JSON），可选缓存以加速 describe 查询
    // - Iceberg: 当前 schema 的序列化表示
    // - Lance: Arrow schema 的 JSON 表示
    pub schema_snapshot: Option<serde_json::Value>,

    // 格式特有的扩展属性
    pub properties: HashMap<String, String>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**设计要点**：
- **`location` 和 `metadata_location` 分离**。在 Iceberg 规范中，`location` 是表的数据基础路径（引擎在此路径下写入数据文件），`metadata-location` 是当前生效的 metadata.json 指针。两者是不同的概念：`location` 在表生命周期内通常不变，`metadata_location` 在每次 commit 后更新。将它们合并到一个字段中会导致 Iceberg 的 `LoadTableResponse` 无法正确构造。Lance 只使用 `location`，`metadata_location` 为 NULL。
- **不再需要 `metadata_etag`**。Iceberg CAS 的校验目标就是 `metadata_location` 本身——客户端在 commit 时断言"当前的 metadata_location 还是我之前读到的那个值"。直接用 `metadata_location` 做条件更新即可，无需额外引入 etag。
- **Asset 不存储 `format`**。格式由所属 Namespace 决定。查询时通过 `JOIN namespaces` 或由适配器提前解析 `namespace_id` 来确定格式。这避免了 Asset 与 Namespace 之间 format 不一致的数据完整性风险。
- `id` 同时作为 Iceberg 的 `table-uuid` 使用。在 CreateTable 时，Quasar 生成此 UUID 并写入返回的 TableMetadata 中。Iceberg 的 `assert-table-uuid` requirement 直接校验此字段。
- `schema_snapshot` 为可选缓存字段。MVP 阶段客户端在 `describe` 时若需要 schema，优先从此字段返回；若为空，可提示客户端从实际数据文件读取。
- `properties` 是格式扩展的载体。Iceberg 可存放 `previous_metadata_location`、`current_snapshot_id` 等；Lance 可存放 `storage_options` 等。

---

### AssetVersion

Lance 格式专用的版本记录。Iceberg 不写入此表。

```rust
pub struct AssetVersion {
    pub id: Uuid,
    pub asset_id: Uuid,         // 外键 → assets.id
    pub version: i32,           // 版本号，从 1 开始递增
    pub manifest_path: String,  // manifest 文件的完整 URI
    pub naming_scheme: String,  // 如 "V2"
    pub created_at: DateTime<Utc>,
}
```

**设计要点**：
- 仅 Lance 格式使用此表。Iceberg 的版本历史自包含在 metadata.json 链中，无需 Catalog 额外记录。这种"按格式差异化存储"的策略避免了对 Iceberg 做不必要的版本表维护，同时满足 Lance REST Namespace 规范对显式版本注册的要求。
- **版本管理语义**：Lance 客户端写入数据文件后，生成 manifest 文件并写入对象存储的 staging 路径，然后调用 `CreateTableVersion` 将 `(version, manifest_path, naming_scheme)` 注册到 Catalog。Catalog 只存储映射关系，不扫描 Lance 数据文件，不验证 `manifest_path` 指向的文件是否存在。这与 Iceberg 的 "metadata-location 指针" 设计哲学一致。
- `UNIQUE(asset_id, version)` 约束确保版本号唯一。客户端并发注册同一版本时，后提交者触发数据库唯一性冲突，映射为 `409 Conflict`（`TableVersionAlreadyExists`）。
- **当前版本号不单独存储**。Lance Asset 的当前版本通过查询 `SELECT MAX(version) FROM asset_versions WHERE asset_id = $1` 实时获取，不在 `assets.properties` 中冗余存储 `current_version`，避免两处数据不一致的风险。

---

## PostgreSQL 数据库 Schema

### 完整 DDL

```sql
-- 扩展：UUID 生成
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================
-- namespaces: 资产命名空间
-- ============================================
CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    format TEXT NOT NULL CHECK (format IN ('iceberg', 'lance')),
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (name, format)
);

COMMENT ON COLUMN namespaces.format IS
    '资产格式标识，当前支持 iceberg / lance，未来可扩展';

-- ============================================
-- assets: 统一资产实体（表）
-- ============================================
CREATE TABLE assets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    metadata_location TEXT,             -- Iceberg 专用：当前 metadata.json URI
    schema_snapshot JSONB,              -- 可选缓存的 Schema 快照
    properties JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (namespace_id, name)
);

CREATE INDEX idx_assets_namespace ON assets(namespace_id);

COMMENT ON COLUMN assets.location IS
    '表的数据基础路径。Iceberg: warehouse 路径；Lance: 数据集根目录';
COMMENT ON COLUMN assets.metadata_location IS
    'Iceberg 专用：当前生效的 metadata.json URI，CAS 校验的比较目标。Lance 始终为 NULL';

-- ============================================
-- asset_versions: Lance 版本记录
-- ============================================
CREATE TABLE asset_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    manifest_path TEXT NOT NULL,
    naming_scheme TEXT NOT NULL DEFAULT 'V2',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (asset_id, version)
);

CREATE INDEX idx_asset_versions_asset ON asset_versions(asset_id);

COMMENT ON TABLE asset_versions IS
    '仅 Lance 格式使用。客户端写入数据后将 manifest 路径注册到此表。Iceberg 版本历史自包含在 metadata.json 链中。';
```

**DDL 变更说明（相对原版）**：

| 变更项 | 原版 | 修改后 | 理由 |
|--------|------|--------|------|
| assets 外键约束 | `ON DELETE CASCADE` | `ON DELETE RESTRICT` | Iceberg 和 Lance 规范均要求仅允许删除空 Namespace |
| assets.format | 存在 | **移除** | 格式由 Namespace 决定，消除冗余和不一致风险 |
| assets.metadata_etag | 存在 | **移除** | CAS 直接基于 `metadata_location` 比较，无需额外字段 |
| assets.metadata_location | 不存在 | **新增** | 区分 Iceberg 的 `location`（数据路径）和 `metadata-location`（元数据指针） |
| assets 唯一约束 | `UNIQUE(namespace_id, name, format)` | `UNIQUE(namespace_id, name)` | format 已由 namespace 决定，无需参与唯一约束 |
| idx_assets_format | 存在 | **移除** | 不再有 assets.format 列 |
| asset_versions 外键 | `ON DELETE CASCADE` | `ON DELETE CASCADE`（保持不变） | 删除 Asset 时应同步清理其版本记录 |

---

## CatalogStore Trait

核心存储抽象，定义领域层与持久化层的边界。

```rust
#[async_trait]
pub trait CatalogStore: Send + Sync {
    // ── Namespace ──────────────────────────────
    async fn create_namespace(&self, namespace: &Namespace) -> Result<Namespace, StoreError>;
    async fn get_namespace(&self, name: &str, format: AssetFormat) -> Result<Option<Namespace>, StoreError>;
    async fn list_namespaces(&self, format: AssetFormat, offset: i64, limit: i32) -> Result<Vec<Namespace>, StoreError>;
    async fn namespace_is_empty(&self, name: &str, format: AssetFormat) -> Result<bool, StoreError>;
    async fn delete_namespace(&self, name: &str, format: AssetFormat) -> Result<bool, StoreError>;

    // 增量更新 properties：removals 删除指定 key，updates 新增/覆盖指定 key-value
    // 对应 Iceberg REST 的 updateProperties 端点语义
    async fn update_namespace_properties(
        &self,
        name: &str,
        format: AssetFormat,
        removals: &[String],
        updates: &HashMap<String, String>,
    ) -> Result<Namespace, StoreError>;

    // ── Asset ──────────────────────────────────
    async fn create_asset(&self, asset: &Asset) -> Result<Asset, StoreError>;
    async fn get_asset(&self, namespace_id: Uuid, name: &str)
        -> Result<Option<Asset>, StoreError>;
    async fn list_assets(&self, namespace_id: Uuid, offset: i64, limit: i32)
        -> Result<Vec<Asset>, StoreError>;
    async fn update_asset(&self, asset: &Asset) -> Result<Asset, StoreError>;
    async fn delete_asset(&self, namespace_id: Uuid, name: &str)
        -> Result<bool, StoreError>;

    // ── Iceberg Commit (CAS) ───────────────────
    // Iceberg 适配器在内存中完成 TableMetadata 的更新、序列化、写入对象存储后，
    // 调用此方法原子更新数据库指针。CAS 条件始终是 metadata_location 未变。
    // 返回 true 表示成功，false 表示冲突（当前值已变）
    async fn commit_iceberg_table(
        &self,
        namespace_id: Uuid,
        name: &str,
        expected_metadata_location: &str,
        updates: &AssetCommitUpdate,
    ) -> Result<bool, StoreError>;

    // ── Lance Version ──────────────────────────
    async fn create_version(&self, version: &AssetVersion) -> Result<AssetVersion, StoreError>;
    async fn list_versions(&self, asset_id: Uuid, descending: bool, limit: i32)
        -> Result<Vec<AssetVersion>, StoreError>;
    async fn get_version(&self, asset_id: Uuid, version: i32)
        -> Result<Option<AssetVersion>, StoreError>;
    async fn get_latest_version(&self, asset_id: Uuid)
        -> Result<Option<AssetVersion>, StoreError>;
}

/// 一次 Iceberg commit 中可能变化的所有字段集合
pub struct AssetCommitUpdate {
    /// 新的 metadata.json URI（必填，每次 commit 都会产生新文件）
    pub new_metadata_location: String,

    /// 表的数据基础路径（仅当 set-location update 出现时有值）
    pub new_location: Option<String>,

    /// 更新后的 schema 缓存
    pub new_schema_snapshot: Option<serde_json::Value>,

    /// properties 增量变更：新增/覆盖
    pub property_updates: HashMap<String, String>,
    /// properties 增量变更：移除的 key 列表
    pub property_removals: Vec<String>,
}
```

**Trait 变更说明（相对原版）**：

| 变更项 | 原版 | 修改后 | 理由 |
|--------|------|--------|------|
| `update_namespace_properties` 签名 | 接收完整 `HashMap` | 接收 `removals` + `updates` | 支持增量更新，避免并发 read-modify-write 覆盖 |
| `update_asset_cas` | 基于 `metadata_etag` 比较 | 重命名为 `commit_iceberg_table`，基于 `metadata_location` 比较 | CAS 目标就是 metadata_location 本身 |
| Asset 查询方法 | 接收 `(namespace_name, asset_name, format)` | 接收 `(namespace_id, asset_name)` | format 已由 namespace_id 隐含，适配器先解析 namespace_id 再调用 |
| `namespace_is_empty` | 不存在 | **新增** | 删除 Namespace 前需检查是否为空 |
| `get_latest_version` | 不存在 | **新增** | DescribeTable 时获取当前最新版本，替代 properties 中冗余存储 current_version |

---

## 格式属性约定

### Iceberg Asset Properties

`assets.properties` JSONB 中 Iceberg 表建议存放的字段：

```json
{
  "previous_metadata_location": "s3://bucket/warehouse/db/table/metadata/00000.metadata.json",
  "current_snapshot_id": "1234567890123",
  "schema_id": "1"
}
```

注意：
- `metadata_location`（指向当前 metadata.json）存储在 `assets.metadata_location` 列中，不放入 `properties`。
- `location`（表的数据基础路径）存储在 `assets.location` 列中，不放入 `properties`。
- `assets.id` 即 Iceberg 规范中的 `table-uuid`。CreateTable 时由 Quasar 生成并写入返回的 TableMetadata。

### Lance Asset Properties

`assets.properties` JSONB 中 Lance 表建议存放的字段：

```json
{
  "storage_options.region": "us-east-1",
  "storage_options.endpoint": "https://s3.example.com"
}
```

注意：
- properties 遵循上游协议的 `Map<String, String>` 类型定义，不支持嵌套对象。需要结构化数据时使用点号分隔的 key 前缀。
- `storage_options` 本身不通过 properties 传递——在 Lance REST Namespace 规范中它是 DeclareTable/DescribeTable 响应中的独立字段，由适配器作为独立字段返回给客户端。
- Lance 的 `location`（数据集基础路径）存储在 `assets.location` 列中。
- 版本历史通过 `asset_versions` 表单独维护。
- 当前版本号不存入 properties，由 `get_latest_version` 实时查询 `asset_versions` 获取。

---

## 索引设计

| 表 | 索引 | 用途 |
|---|---|---|
| namespaces | `UNIQUE(name, format)` | 防止同名 Namespace 在同一格式下重复 |
| assets | `UNIQUE(namespace_id, name)` | 防止同 Namespace 下同名表（format 已由 namespace 决定） |
| assets | `idx_assets_namespace` | 加速 `list assets in namespace` 查询 |
| asset_versions | `UNIQUE(asset_id, version)` | 防止同一表的版本号重复（并发控制） |
| asset_versions | `idx_asset_versions_asset` | 加速 `list versions` 查询 |

---

## 并发控制实现

### Iceberg CAS

Iceberg 的 CAS 直接基于 `metadata_location` 做条件更新——客户端 commit 时断言"当前的 metadata_location 还是我之前 LoadTable 时读到的值"：

```sql
UPDATE assets
SET metadata_location = $new_metadata_location,
    location = COALESCE($new_location, location),
    schema_snapshot = COALESCE($new_schema_snapshot, schema_snapshot),
    properties = (properties - $property_removals::text[]) || $property_updates::jsonb,
    updated_at = NOW()
WHERE namespace_id = $namespace_id
  AND name = $table_name
  AND metadata_location = $expected_metadata_location;
```

应用层检查 `UPDATE` 影响的行数：
- 行数 = 1 → 成功
- 行数 = 0 → 冲突，返回 `409 Conflict`

不需要额外的 etag 或版本号字段。`metadata_location` 本身就是乐观锁的版本标识——每次成功的 commit 都会将它更新为新的 metadata.json URI，该 URI 全局唯一（包含 UUID 或递增编号）。

### Lance 版本唯一性

```sql
INSERT INTO asset_versions (asset_id, version, manifest_path, naming_scheme)
VALUES ($1, $2, $3, $4);
```

若违反 `UNIQUE(asset_id, version)` → 返回 `409 Conflict`（`TableVersionAlreadyExists`）

### Namespace 属性增量更新

```sql
UPDATE namespaces
SET properties = (properties - $removals::text[]) || $updates::jsonb,
    updated_at = NOW()
WHERE name = $name AND format = $format;
```

单条 SQL 原子完成移除和新增，无需 read-modify-write，避免并发更新互相覆盖。

---

## 扩展路径

| 未来需求 | 现有模型支持方式 |
|----------|----------------|
| 新增 Delta Lake 格式 | `namespaces.format` CHECK 约束增加 `delta`，`properties` 存放 Delta 特有属性 |
| 多级 Namespace | `namespaces` 表增加 `parent_id` 自引用字段，或改用 `ltree` 扩展 |
| AI 模型资产 | 新增 `model` 格式，`assets` 的 `location` 指向模型 artifact URI，`properties` 存放 framework、version 等 |
| 特征（Feature）资产 | 新增 `feature` 格式，`properties` 存放 feature group、source table 等元数据 |
| 软删除 | 增加 `deleted_at` 字段，查询时过滤 `WHERE deleted_at IS NULL` |
| 审计日志 | 新增 `audit_log` 表记录所有写入操作 |

---

## 修订记录

### V1.0

- 新增：6 条数据模型设计原则（格式无关、指针而非内容、扁平化层级、自包含存储、并发安全、预留扩展位）
- 新增：与业界方案的三维度对比（层级设计、格式划分策略、存储架构）
- 新增：实体关系定义 —— `Namespace(1) → Asset(N) → AssetVersion(N)`，含级联和删除约束规则
- 新增：`Namespace`、`Asset`、`AssetVersion` 核心实体的 Rust 结构定义及设计要点说明
- 新增：PostgreSQL 完整 DDL（3 张表：namespaces、assets、asset_versions，含索引、注释、约束）
- 新增：`CatalogStore` trait 定义（Namespace / Asset / Iceberg Commit / Lance Version 四类操作，共 15 个方法）
- 新增：`AssetCommitUpdate` 结构定义（Iceberg CAS 更新时可能变更的全部字段集合）
- 新增：Iceberg CAS SQL（基于 `metadata_location` 的条件更新）
- 新增：Lance 版本唯一性约束 SQL（`UNIQUE(asset_id, version)`）
- 新增：Namespace 属性增量更新 SQL（单条原子操作）
- 新增：格式属性约定（Iceberg Asset 和 Lance Asset 的 `properties` 字段建议存放内容）
- 新增：索引设计说明（5 个索引/约束的用途表）
- 新增：扩展路径表（6 种未来需求及现有模型的支持方式）

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。