# Quasar V3 Core Data Model 设计草案

> 本文档承载 V3 Core Data Model 的设计细节。需求边界、业务目标与问题闭环见 `docs/v3/V3_REQUIREMENTS.md`。

---

## 一、设计目标

V3 Data Core Model 的目标是建立长期稳定的元数据核心：

1. 支持 `Domain -> Namespace -> Asset` 三层组织结构。
2. 支持 Iceberg / Lance 表资产在同一 Domain 内按端点隔离访问。
3. 支持后续 Model / Fileset / Topic 等非表资产扩展。
4. 用数据库约束保护核心不变量，但避免无边界级联删除和过早索引。
5. 修复 V2 中 `asset_subtype` 语义混乱、版本字段混用、Trait 过度绑定表资产的问题。

---

## 二、约束与性能原则

### 2.1 应由数据库兜底的不变量

以下不变量必须由数据库约束或等价机制兜底，不能只依赖 handler 约定：

- Domain 名称全局唯一。
- 同一 Domain 内 Namespace 名称唯一。
- 同一 Namespace 内活动 Asset 名称唯一。
- Asset 必须属于一个 Namespace。
- 表资产扩展记录必须属于一个 `asset_type='table'` 的 Asset。
- AssetVersion 必须属于一个 Asset。
- 同一 Asset 下 `version_key` 唯一。
- 同一 Asset 下非空 `version_order` 唯一。
- 版本前驱必须指向同一 Asset 下的版本。

### 2.2 不使用无边界级联删除

V3 区分上层容器删除与明确的资产删除：

- `domains -> namespaces`：建议 `ON DELETE RESTRICT`。
- `namespaces -> assets`：建议 `ON DELETE RESTRICT`。
- `assets -> tabular_assets / asset_versions / asset_permissions`：允许 `ON DELETE CASCADE`，但只能由明确的 Asset 删除 API 触发。
- `asset_versions -> tabular_asset_versions`：允许 `ON DELETE CASCADE`。
- 版本前驱引用使用 `ON DELETE SET NULL`；跨 Asset 前驱引用由数据库触发器兜底。

这样可以避免一次误删 Domain 或 Namespace 导致大量资产和版本记录被数据库隐式删除。

### 2.3 索引策略

必要索引优先覆盖：

- 外键引用方列。
- 列表查询路径。
- 标准协议端点按格式过滤的表资产查询。
- latest-version 查询。

延后索引包括：

- JSONB GIN 索引。
- `created_at` / `updated_at` 等仅用于后台排序的索引。
- 预留字段索引。
- 权限表在 RBAC 未启用前的复杂组合索引。

---

## 三、逻辑模型

```
domains
  └── namespaces
        └── assets
              ├── tabular_assets
              ├── model_assets        # 后续版本引入
              ├── fileset_assets      # 后续版本引入
              └── asset_versions
                    ├── tabular_asset_versions
                    └── model_asset_versions  # 后续版本引入
```

权限、标签、审计、血缘等治理能力优先绑定 `assets.id`。

---

## 四、DDL 草案

### 4.1 注册表

V3 不建议继续使用硬编码 `CHECK (asset_type IN (...))` 或 `CHECK (format IN (...))`。更合适的方式是注册表：新增类型或格式时插入注册表记录，不需要改表结构。

```sql
CREATE TABLE asset_types (
    -- 资产大类名称，如 table / model / fileset / topic。
    name TEXT PRIMARY KEY,
    -- 资产类型说明，用于管理后台展示和运维识别。
    comment TEXT,
    -- 注册时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tabular_formats (
    -- 表资产格式名称，如 iceberg / lance / delta。
    name TEXT PRIMARY KEY,
    -- 表格式说明，用于管理后台展示和运维识别。
    comment TEXT,
    -- 是否支持由 Catalog Service 协调的 catalog-managed CAS commit。
    -- 例如 Iceberg 支持；当前 Lance 只支持外部写入后的版本注册，不支持 catalog-managed commit。
    supports_cas_commit BOOLEAN NOT NULL DEFAULT FALSE,
    -- 注册时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO asset_types (name, comment)
VALUES ('table', 'Tabular dataset')
ON CONFLICT DO NOTHING;

INSERT INTO tabular_formats (
    name,
    comment,
    supports_cas_commit
)
VALUES
    ('iceberg', 'Apache Iceberg table', TRUE),
    ('lance', 'Lance dataset', FALSE)
ON CONFLICT DO NOTHING;
```

### 4.2 domains

```sql
CREATE TABLE domains (
    -- Domain 唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Domain 名称，全局唯一；用于 API 路径、管理后台展示和权限边界识别。
    name TEXT NOT NULL UNIQUE,
    -- Domain 描述说明。
    comment TEXT,
    -- 业务自定义属性，如标签、成本中心、环境、负责人等。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 存储类型，如 s3 / minio / hdfs / local；用于选择 object_store 初始化逻辑。
    storage_type TEXT,
    -- 存储配置。不得明文存储 credential；应存 secret reference 或加密后的敏感值。
    storage_config JSONB NOT NULL DEFAULT '{}',
    -- 默认 warehouse 根路径；创建托管表时可基于该路径分配物理位置。
    warehouse TEXT,
    -- Domain 所有者标识，如团队、用户或服务账号。
    owner TEXT,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

设计说明：

- Domain 不绑定 `format` / `provider`。
- `storage_config` 不应存明文 credential；优先存 secret reference，或者存加密后的敏感值。
- `warehouse` 是默认物理路径分配根，不表达格式隔离。

### 4.3 namespaces

```sql
CREATE TABLE namespaces (
    -- Namespace 唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Domain。非空 Domain 不允许被删除。
    domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE RESTRICT,
    -- Namespace 名称；同一 Domain 内唯一。
    name TEXT NOT NULL,
    -- Namespace 描述说明。
    comment TEXT,
    -- 业务自定义属性，如标签、owner、默认配置等。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 保证同一 Domain 内 Namespace 名称唯一。
    UNIQUE(domain_id, name)
);

CREATE INDEX idx_namespaces_domain ON namespaces(domain_id);
```

设计说明：

- V3 只实现单层 Namespace。
- 不在 V3 DDL 中加入 `parent_id` 作为半成品预留。未来支持嵌套 Namespace 时，需要同时设计路径解析、同父唯一约束、跨 Domain parent 校验和协议映射。
- 非空 Domain 删除由 FK `RESTRICT` 阻止，上层 API 映射为冲突错误。

### 4.4 assets

```sql
CREATE TABLE assets (
    -- Asset 唯一标识，后续权限、审计、血缘等治理能力统一引用该 ID。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Namespace。非空 Namespace 不允许被删除。
    namespace_id UUID NOT NULL REFERENCES namespaces(id) ON DELETE RESTRICT,
    -- Asset 名称；同一 Namespace 内活动资产名唯一。
    name TEXT NOT NULL,
    -- 资产大类，如 table / model / fileset；引用资产类型注册表。
    asset_type TEXT NOT NULL REFERENCES asset_types(name),
    -- Asset 描述说明。
    comment TEXT,
    -- 业务自定义属性；只承载治理/管理面属性，不存格式内部状态。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 软删除时间。NULL 表示活动资产；非 NULL 表示已软删除。
    deleted_at TIMESTAMPTZ,
    -- 创建者标识，预留给审计或权限系统。
    created_by TEXT,
    -- 最后更新者标识，预留给审计或权限系统。
    updated_by TEXT,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_assets_active_name
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_assets_namespace_active
    ON assets(namespace_id, name)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_assets_type_active
    ON assets(asset_type)
    WHERE deleted_at IS NULL;
```

设计说明：

- `assets` 是统一身份层，不存表格式。
- 活动资产名在 Namespace 内唯一。如果 V3 不启用软删除，可以用普通 `UNIQUE(namespace_id, name)`；一旦保留 `deleted_at`，应使用部分唯一索引。
- 协议端点创建表时，若同名活动资产已存在，即使格式不同也返回冲突。
- 协议端点查询表时仍按表格式过滤，非匹配格式返回 not found 语义。

### 4.5 tabular_assets

```sql
CREATE TABLE tabular_assets (
    -- 关联的通用 Asset ID；一个表资产对应一个通用资产身份。
    asset_id UUID PRIMARY KEY REFERENCES assets(id) ON DELETE CASCADE,
    -- 表格式，如 iceberg / lance；引用表格式注册表。
    format TEXT NOT NULL REFERENCES tabular_formats(name),
    -- 表数据根路径或数据集根路径。
    location TEXT NOT NULL,
    -- 当前元数据位置。Iceberg 为当前 metadata.json；Lance 可为空。
    metadata_location TEXT,
    -- schema 快照缓存，用于管理面快速展示；格式内权威状态仍以协议语义为准。
    schema_snapshot JSONB,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 更新时间。
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tabular_assets_format_asset
    ON tabular_assets(format, asset_id);

CREATE OR REPLACE FUNCTION ensure_tabular_asset_type()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM assets
        WHERE id = NEW.asset_id
          AND asset_type = 'table'
    ) THEN
        RAISE EXCEPTION 'tabular asset % must reference an asset with asset_type=table', NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tabular_assets_asset_type
BEFORE INSERT OR UPDATE OF asset_id ON tabular_assets
FOR EACH ROW
EXECUTE FUNCTION ensure_tabular_asset_type();
```

设计说明：

- `format` 属于表资产扩展层。
- `metadata_location` 对 Iceberg 是当前 metadata.json 指针；对 Lance 可以为空，Lance 版本元数据记录在 `tabular_asset_versions`。
- `trg_tabular_assets_asset_type` 保证 `tabular_assets.asset_id` 对应的 `assets.asset_type = 'table'`。

### 4.6 asset_versions

```sql
CREATE TABLE asset_versions (
    -- 版本记录唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 所属 Asset；删除 Asset 时清理其版本记录。
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- 格式原生版本标识。同一 Asset 内唯一，如 Lance 的 "1" 或模型版本 "v1.2.0"。
    version_key TEXT NOT NULL,
    -- 可比较的版本顺序。支持自然递增版本的格式应写入；无稳定数字顺序时可为空。
    version_order BIGINT,
    -- 前驱版本记录 ID，用于表达版本链。
    previous_version_id UUID REFERENCES asset_versions(id) ON DELETE SET NULL,
    -- 版本说明，如提交摘要或人工备注。
    comment TEXT,
    -- 版本级自定义属性，不存表版本 metadata_location 等类型专有字段。
    properties JSONB NOT NULL DEFAULT '{}',
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 保证同一 Asset 下原生版本标识唯一。
    UNIQUE(asset_id, version_key)
);

CREATE UNIQUE INDEX uq_asset_versions_order
    ON asset_versions(asset_id, version_order)
    WHERE version_order IS NOT NULL;

CREATE INDEX idx_asset_versions_latest
    ON asset_versions(asset_id, version_order DESC)
    WHERE version_order IS NOT NULL;

CREATE INDEX idx_asset_versions_previous
    ON asset_versions(previous_version_id);

CREATE OR REPLACE FUNCTION ensure_previous_version_same_asset()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.previous_version_id IS NOT NULL
       AND NOT EXISTS (
           SELECT 1 FROM asset_versions
           WHERE id = NEW.previous_version_id
             AND asset_id = NEW.asset_id
       ) THEN
        RAISE EXCEPTION 'previous_version_id % must reference a version of the same asset %',
            NEW.previous_version_id, NEW.asset_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_asset_versions_previous_same_asset
BEFORE INSERT OR UPDATE OF asset_id, previous_version_id ON asset_versions
FOR EACH ROW
EXECUTE FUNCTION ensure_previous_version_same_asset();
```

设计说明：

- `version_key` 是原生版本标识，例如 Lance 的 `"1"`、模型版本的 `"v1.2.0"`、或其他格式的 hash/tag。
- `version_order` 是可比较顺序。Lance 写入 `version_order = version_id`；不具备稳定数字顺序的格式可以为空。
- latest 查询不得从 `version_key` 解析 fallback。没有 `version_order` 的格式必须提供格式内权威 latest 指针或返回无 latest。
- `trg_asset_versions_previous_same_asset` 保证 `previous_version_id` 必须指向同一 Asset 下版本；应用层仍需在同一事务内校验，用于返回更清晰的协议错误。

### 4.7 tabular_asset_versions

```sql
CREATE TABLE tabular_asset_versions (
    -- 关联的通用版本记录 ID；一个表版本对应一个通用版本身份。
    version_id UUID PRIMARY KEY REFERENCES asset_versions(id) ON DELETE CASCADE,
    -- 该表版本的元数据文件或 manifest 位置。
    metadata_location TEXT NOT NULL,
    -- 创建时间。
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

设计说明：

- 表版本的 metadata / manifest 位置放在表版本扩展层。
- Iceberg V3 初始仍可只用 `tabular_assets.metadata_location` 表达当前指针，不强制为每次 Iceberg commit 写入 `asset_versions`。
- Lance `create_version` 写入 `asset_versions` 与 `tabular_asset_versions`，且必须在同一数据库事务中完成。

### 4.8 asset_permissions（预留）

```sql
CREATE TABLE asset_permissions (
    -- 权限记录唯一标识。
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 授权目标 Asset；权限系统不感知资产类型和格式。
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- 权限主体，如 user / role / group 的标识符。
    subject TEXT NOT NULL,
    -- 权限动作，如 read / write / delete / admin。
    action TEXT NOT NULL,
    -- 授权人标识。
    granted_by TEXT NOT NULL,
    -- 授权时间。
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 过期时间；NULL 表示长期有效。
    expires_at TIMESTAMPTZ,
    -- 防止同一主体对同一资产重复授予同一动作。
    UNIQUE(asset_id, subject, action)
);

CREATE INDEX idx_asset_permissions_asset ON asset_permissions(asset_id);
CREATE INDEX idx_asset_permissions_subject ON asset_permissions(subject);
```

设计说明：

- 权限外键指向统一资产身份层。
- V3 不实现完整 RBAC 时，可不创建该表，或创建但不暴露 API。

---

## 五、Core Rust 模型建议

V3 Core 模型应减少硬编码枚举向存储层泄漏：

```rust
pub struct Domain {
    /// Domain 唯一标识。
    pub id: Uuid,
    /// Domain 名称，全局唯一。
    pub name: String,
    /// Domain 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性。
    pub properties: HashMap<String, String>,
    /// 存储类型，如 s3 / minio / hdfs / local。
    pub storage_type: Option<String>,
    /// 存储配置。使用 JSON 值承载嵌套配置和 secret reference，API 响应必须脱敏。
    pub storage_config: serde_json::Value,
    /// 默认 warehouse 根路径。
    pub warehouse: Option<String>,
    /// Domain 所有者标识。
    pub owner: Option<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct Namespace {
    /// Namespace 唯一标识。
    pub id: Uuid,
    /// 所属 Domain ID。
    pub domain_id: Uuid,
    /// Namespace 名称，同一 Domain 内唯一。
    pub name: String,
    /// Namespace 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性。
    pub properties: HashMap<String, String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct Asset {
    /// Asset 唯一标识，治理能力统一引用该 ID。
    pub id: Uuid,
    /// 所属 Namespace ID。
    pub namespace_id: Uuid,
    /// Asset 名称，同一 Namespace 内活动资产名唯一。
    pub name: String,
    /// 资产大类，如 table / model / fileset。
    pub asset_type: String,
    /// Asset 描述说明。
    pub comment: Option<String>,
    /// 业务自定义属性，不存格式内部状态。
    pub properties: HashMap<String, String>,
    /// 软删除时间；None 表示活动资产。
    pub deleted_at: Option<DateTime<Utc>>,
    /// 创建者标识。
    pub created_by: Option<String>,
    /// 最后更新者标识。
    pub updated_by: Option<String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct TabularAsset {
    /// 关联的通用 Asset ID。
    pub asset_id: Uuid,
    /// 表格式，如 iceberg / lance。
    pub format: String,
    /// 表数据根路径或数据集根路径。
    pub location: String,
    /// 当前元数据位置；Iceberg 为 metadata.json，Lance 可为空。
    pub metadata_location: Option<String>,
    /// schema 快照缓存。
    pub schema_snapshot: Option<serde_json::Value>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
    /// 更新时间。
    pub updated_at: DateTime<Utc>,
}

pub struct AssetVersion {
    /// 版本记录唯一标识。
    pub id: Uuid,
    /// 所属 Asset ID。
    pub asset_id: Uuid,
    /// 格式原生版本标识，同一 Asset 内唯一。
    pub version_key: String,
    /// 可比较版本顺序；无稳定数字顺序时为空。
    pub version_order: Option<i64>,
    /// 前驱版本记录 ID。
    pub previous_version_id: Option<Uuid>,
    /// 版本说明。
    pub comment: Option<String>,
    /// 版本级自定义属性。
    pub properties: HashMap<String, String>,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}

pub struct TabularAssetVersion {
    /// 关联的通用版本记录 ID。
    pub version_id: Uuid,
    /// 该表版本的元数据文件或 manifest 位置。
    pub metadata_location: String,
    /// 创建时间。
    pub created_at: DateTime<Utc>,
}
```

说明：

- 标准协议适配器可以继续使用 `AssetFormat::Iceberg` / `AssetFormat::Lance` 这类常量或枚举做路由分支。
- Core 存储模型不应要求所有未来格式都进入闭合枚举。
- 如果希望类型更安全，可用 newtype 包装 `String`，例如 `AssetTypeName(String)`、`TabularFormatName(String)`。

---

## 六、Store Trait 拆分建议

V3 存储 trait 建议按职责拆分：

| Trait | 职责 |
|------|------|
| `DomainStore` | Domain CRUD 与存储配置读取 |
| `NamespaceStore` | Namespace CRUD |
| `AssetStore` | 格式无关的 Asset 身份、属性、删除、重命名 |
| `TabularStore` | 表资产创建、加载、列表、格式过滤 |
| `VersionStore` | 通用版本记录与 latest 查询 |
| `TabularVersionStore` | 表版本 metadata / manifest 记录 |
| `CasCommitStore` | Iceberg CAS commit |
| `UnifiedQueryStore` | Unified API 聚合查询 |

`PgCatalogStore` 可以实现多个 trait。Adapter 层按协议需要依赖最小 trait 组合。

---

## 七、协议映射要点

### 7.1 Iceberg

- Domain 映射到 Iceberg REST Catalog 的 `{prefix}` 或服务端默认 Catalog，具体路径由 V3 API 设计文档确认。
- Namespace 查询必须带 Domain 上下文。
- Table 查询必须过滤 `asset_type = 'table'` 与 `tabular_assets.format = 'iceberg'`。
- CAS commit 更新 `tabular_assets.metadata_location` 与 `schema_snapshot`。

### 7.2 Lance

- Domain 编码为 Lance REST Namespace 官方 `{id}` 的第一段，例如 `prod$analytics$events`。
- Lance 适配器不得新增 `/domains/{domain}` 这类自定义协议路径。
- Table 查询必须过滤 `asset_type = 'table'` 与 `tabular_assets.format = 'lance'`。
- 官方 `POST /v1/table/{id}/version/create` 的本质是注册已存在版本，写入 `asset_versions.version_key = version_id::text`、`version_order = version_id` 和 `tabular_asset_versions.metadata_location`。

### 7.3 Unified API

- 单 Asset 操作不再要求 `format` 查询参数，因为活动资产名在 Namespace 内唯一。
- 列表接口仍可以支持 `asset_type`、`format`、`name` 等过滤条件。
- 响应中应返回资产类型；表资产还应返回表格式。

---

## 八、验收重点

V3 实现阶段至少覆盖以下测试：

- 非空 Domain 删除返回冲突，不删除 Namespace。
- 非空 Namespace 删除返回冲突，不删除 Asset。
- 删除 Asset 会清理对应表扩展和版本扩展记录。
- 同一 Namespace 下同名活动 Asset 重复创建返回冲突，即使格式不同。
- 软删除启用时，已软删除 Asset 不阻塞同名新资产创建。
- 非注册资产类型或表格式创建失败。
- Iceberg / Lance 标准协议列表互相不可见。
- Lance `version_key` 与 `version_order` 映射稳定，不存在字符串解析 fallback。
- 跨 Asset 的 `previous_version_id` 被拒绝。
- 外键引用方索引存在，基础列表查询和 latest-version 查询使用预期索引路径。

---

## 九、修订记录

### V1.0

- 新增 V3 Core Data Model 设计草案。
- 明确数据库约束与性能原则。
- 将 Domain / Namespace 删除语义调整为 `RESTRICT` 思路。
- 将版本模型调整为 `version_key` + `version_order` 的明确分工，而非单一整数版本号。
- 引入资产类型与表格式注册表方案，替代硬编码数据库 CHECK。

### V1.1

- 补充 DDL 草案中每个字段的含义、用途和边界说明。
- 补充 Core Rust 模型草案中每个字段的注释。

### V1.2

- 删除 `tabular_formats.supports_version_registration`，避免把显式版本注册能力混入核心格式注册表。
- 保留 `supports_cas_commit`，并明确其含义是是否支持 Catalog-managed CAS commit。

### V1.3

- 补充数据库触发器，确保表资产扩展只能引用 table Asset、版本前驱不能跨 Asset。
- 修复 Core Rust 模型与 DDL 的不一致：补充 `storage_config`、审计字段、扩展表时间字段和 `TabularAssetVersion`。
