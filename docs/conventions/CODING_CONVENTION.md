# Quasar Rust 编码规范

> 本规范约束 Quasar 项目中 Rust 代码的编写风格和质量要求。

---

## 基本原则

1. **优先可读性**：代码是给人类阅读的，机器只是顺便执行
2. **显式优于隐式**：类型转换、错误处理、生命周期标注尽量显式表达
3. **错误优先**：使用 `?` 传播错误，禁止 `unwrap()`/`expect()` 出现在生产代码路径
4. **文档即契约**：公共 API（pub struct、pub fn、pub trait）必须有文档注释

---

## 命名规范

| 类型 | 规范 | 示例 |
|------|------|------|
| 模块/文件 | snake_case | `namespace.rs`、`table_metadata.rs` |
| 结构体/枚举 | PascalCase | `CatalogStore`、`StoreError` |
| 枚举变体 | PascalCase | `NotFound`、`AlreadyExists` |
| 函数/方法 | snake_case | `create_namespace`、`commit_iceberg_table` |
| 常量 | SCREAMING_SNAKE_CASE | `MAX_PAGE_SIZE` |
| 类型参数 | PascalCase，单字母或大写缩写 | `T`、`Err`、`Store` |
| trait | PascalCase，描述能力 | `CatalogStore`、`IntoResponse` |

---

## 模块组织

每个 crate 的 `src/lib.rs` 只导出公共 API，具体实现放在子模块中。

**跨 crate 公共 API（core）使用显式导出**，避免通配符无意中暴露内部类型：

```rust
// core/src/lib.rs
pub mod models;
pub mod store;
pub mod error;

// 显式导出公共类型，不依赖通配符
pub use models::{Namespace, Asset, AssetVersion, AssetFormat, AssetCommitUpdate};
pub use store::CatalogStore;
pub use error::StoreError;
```

**crate 内部模块组织（adapter）允许通配符导出**，因为是内部聚合而非公共 API 契约：

```rust
// adapter/src/lance/mod.rs
mod namespace;
mod table;
mod version;
mod error;

pub use namespace::*;
pub use table::*;
pub use version::*;
```

---

## 错误处理

### 禁止使用 `unwrap()` 和 `expect()`

**唯一例外：** 单元测试和编译期常量（`const`/`static`）。

```rust
// ❌ 错误
let config = std::env::var("QUASAR_DATABASE_URL").unwrap();

// ✅ 正确
let config = std::env::var("QUASAR_DATABASE_URL")
    .map_err(|e| StoreError::InvalidInput(format!("Missing DATABASE_URL: {e}")))?;
```

### 错误转换

使用 `thiserror` 定义错误类型。错误转换在各自 crate 中实现，避免核心 crate 依赖具体实现细节：

```rust
// core/src/error.rs — core 不依赖任何数据库或框架 crate
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    #[error("Database error: {0}")]
    Database(String),
}
```

```rust
// storage/src/lib.rs — storage 层做转换，core 保持纯净
impl From<tokio_postgres::Error> for StoreError {
    fn from(e: tokio_postgres::Error) -> Self {
        StoreError::Database(e.to_string())
    }
}
```

---

## SQL 编写规范

- SQL 关键字大写：`SELECT`、`INSERT`、`WHERE`、`UPDATE`
- 使用参数化查询（`$1`、`$2`），严禁字符串拼接 SQL
- 多行 SQL 使用 `r#"..."#` 原始字符串，保持 SQL 自身的缩进可读性
- 每条 SQL 写在常量或函数开头，不散落在业务逻辑中间

```rust
// ✅
const SQL_CREATE_NAMESPACE: &str = r#"
    INSERT INTO namespaces (name, format, properties)
    VALUES ($1, $2, $3)
    RETURNING id, name, format, properties, created_at, updated_at
"#;

let row = client.query_one(SQL_CREATE_NAMESPACE, &[&name, &format, &props]).await?;

// ❌ 字符串拼接，存在注入风险
let row = client.query_one(
    &format!("INSERT INTO namespaces (name) VALUES ('{}')", name),
    &[],
).await?;
```

---

## unsafe 代码

生产代码中禁止使用 `unsafe`。如确有必要（如 FFI 调用），需在 PR 中说明理由并由至少一名团队成员 review。

---

## 异步代码

- 使用 `async fn` 定义异步函数，不要用 `async` 块包裹整个函数体
- `await` 点尽量垂直对齐，便于阅读调用链
- 避免在 `async fn` 中持有锁跨越 `.await` 点

```rust
// ✅
pub async fn get_namespace(
    &self,
    name: &str,
    format: AssetFormat,
) -> Result<Option<Namespace>, StoreError> {
    let client = self.pool.get().await?;
    let row = client
        .query_one("SELECT ...", &[...])
        .await?;
    Ok(row_to_namespace(row))
}
```

---

## 文档注释

### 公共 API 必须写文档

```rust
/// 创建一个新的 Namespace。
///
/// 如果同名同 format 的 Namespace 已存在，返回 `StoreError::AlreadyExists`。
pub async fn create_namespace(
    &self,
    namespace: &Namespace,
) -> Result<Namespace, StoreError>;
```

### 文档注释内容要求

- 第一行：简短描述（一句话）
- 空行后：详细说明、参数含义、返回值、错误情况
- 使用 Markdown 格式

---

## 格式化

- 使用 `rustfmt` 统一格式化，配置放在 `rustfmt.toml`
- 每行最大长度：100 字符
- 使用 4 空格缩进（不使用 tab）

```toml
# rustfmt.toml
max_width = 100
tab_spaces = 4
use_small_heuristics = "Default"
```

---

## Lint

CI 中启用 `clippy` 并配置为 deny warnings：

```bash
cargo clippy -- -D warnings
```

在 workspace `Cargo.toml` 中统一配置 lint 级别：

```toml
[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
```

这从编译器层面强制执行"禁止 unwrap/expect"的规则，而非仅靠人工 review。

---

## 测试

### 单元测试放在模块内

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_namespace_duplicate() {
        let store = setup_test_store().await;
        store.create_namespace(&ns("prod", AssetFormat::Lance)).await.unwrap();
        let err = store.create_namespace(&ns("prod", AssetFormat::Lance)).await.unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists(_)));
    }
}
```

### 集成测试放在 `tests/` 目录

```
quasar-server/
├── src/
└── tests/
    ├── lance_integration.rs
    └── iceberg_integration.rs
```

---

## 依赖管理

### Workspace 统一管理

共享依赖在 workspace 根 `Cargo.toml` 中统一声明版本，各 crate 引用时使用 `workspace = true`：

```toml
# quasar/Cargo.toml
[workspace.dependencies]
uuid = { version = "1", features = ["v4"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
tracing = "0.1"

# core/Cargo.toml
[dependencies]
uuid = { workspace = true }
serde = { workspace = true }

# storage/Cargo.toml
[dependencies]
core = { workspace = true }
tokio = { workspace = true }
tokio-postgres = "0.7"
```

### 一般规则

- 在 `Cargo.toml` 中使用精确版本或最小兼容版本
- 新增依赖需说明理由（在 PR 描述中）
- 定期运行 `cargo audit` 检查安全漏洞

---

## 日志与追踪

- 使用 `tracing` 而非 `log`
- 结构化字段：`info!(namespace = %name, format = ?fmt, "creating namespace")`
- 避免在热路径打印 DEBUG 级别日志

---

## 修订记录

### V1.1

- 修正：`StoreError` 示例改为 `Database(String)`，由 storage 层实现 `From<tokio_postgres::Error>`，避免 core crate 依赖数据库驱动
- 修正：core 的模块导出改为显式列举（`pub use models::{Namespace, ...}`），保留 adapter 内部模块使用通配符导出的说明
- 新增：SQL 编写规范（关键字大写、参数化查询、原始字符串）
- 新增：unsafe 代码禁止策略
- 新增：clippy lint 配置（`unwrap_used = "deny"`）
- 新增：Workspace 依赖统一管理（`workspace = true`）

### V1.0

- 新增：命名规范（8 种类型）
- 新增：模块组织规则
- 新增：错误处理规范（禁止 unwrap、使用 thiserror）
- 新增：异步代码规范
- 新增：文档注释要求
- 新增：格式化配置（rustfmt.toml）
- 新增：测试规范（单元测试 + 集成测试目录结构）
- 新增：依赖管理、日志与追踪规范

---

**后续修订规则：** 任何修改都在修订记录末尾追加新条目。版本号增长模式：V1.0 → V1.1（小修正）或 V2.0（重大结构调整），视修订范围自行决策。
