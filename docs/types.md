# RBDC 数据类型与表字段映射规则

> 本文是 rbdc 驱动类型映射的权威说明。修改任何 `driver.rs`、`src/types`、`TypeInfo`、`Value` 编码/解码行为时，必须同步更新本文和对应测试。
>
> 更新时间：2026-11-15

## 1. 强制规则

1. **数组和 Map 统一按 JSON 存储**
   - `rbs::Value::Array`、`rbs::Value::Map` 在 rbdc 中表示 JSON 数据，不表示数据库原生数组。
   - MySQL：`JSON`
   - PostgreSQL：`JSON`，绑定的是 JSON 参数，不是 PostgreSQL 原生数组
   - SQL Server：JSON 文本/二进制，字段使用 `VARCHAR(MAX)` 或 `NVARCHAR(MAX)`
   - SQLite/Turso：`TEXT`
   - DuckDB：`VARCHAR`
   - PostgreSQL 原生数组解码是兼容能力，不改变 `Value::Array` 的 JSON 绑定语义。

2. **同一 `rbs::Value` 变体在每个驱动中必须固定映射**
   - 禁止根据数值大小在 `i32/i64`、`INT/BIGINT`、`INT4/INT8/NUMERIC` 之间动态切换绑定类型。
   - 能无损承载就选择固定类型；不能承载就明确报错。
   - 表字段类型、参数 `TypeInfo`、编码格式必须互相一致。

3. **DDL 类型和协议绑定类型分开看**
   - `Driver::column_type(&Value)`：建表/字段类型推荐。
   - `TypeInfo for Value`：数据库协议参数类型，可能与 DDL 名称不同。
   - 例如 MySQL `Enum/Set/Geometry/Bit` 的 DDL 类型和 Connector/C 可绑定的 wire type 不同。

4. **禁止静默错值**
   - 整数越界不得截断。
   - 非法时间、Decimal、UUID、BIT 不得变成 `0`、空字符串或 NULL。
   - 未知 `Ext` 不得默认绑定 NULL；应报错，或在驱动明确支持时按文本处理。

5. **扩展类型名必须稳定**
   - 使用 PascalCase，例如 `DateTime`，禁止 `Datetime`。
   - `Ext` 名称属于对外数据协议，重命名必须评估兼容性。
   - `TypeInfo` 相关枚举/结构必须支持 serde 往返；基于 `rbs::Value::Ext` 的枚举通常需要自定义 `Deserialize`。

## 2. 源码中的权威入口

| 内容 | 路径 |
|---|---|
| MySQL | `rbdc-mysql/src/driver.rs`、`rbdc-mysql/src/types/value.rs` |
| PostgreSQL | `rbdc-pg/src/driver.rs`、`rbdc-pg/src/types/value.rs`、`rbdc-pg/src/type_info.rs` |
| SQL Server | `rbdc-mssql/src/driver.rs`、`rbdc-mssql/src/encode.rs`、`rbdc-mssql/src/decode.rs` |
| SQLite | `rbdc-sqlite/src/driver.rs`、`rbdc-sqlite/src/type_info.rs`、`rbdc-sqlite/src/types/` |
| Turso/libSQL | `rbdc-turso/src/driver.rs`、`rbdc-turso/src/value.rs`、`rbdc-turso/src/types/` |
| DuckDB | `rbdc-duckdb/src/driver.rs`、`rbdc-duckdb/src/types/value.rs` |
| Value/serde | `rbs/src/value/mod.rs`、`rbs/src/value_serde/` |

## 3. 基础 `Value` 映射

下表是 DDL 建表推荐类型；参数绑定必须以对应 `TypeInfo` 和编码实现为准。

| `rbs::Value` | MySQL | PostgreSQL | SQL Server | SQLite | Turso | DuckDB |
|---|---|---|---|---|---|---|
| `Null` | `NULL` | `UNKNOWN` | `NULL` | `NULL` | `NULL` | `NULL` |
| `Bool` | `TINYINT` | `BOOL` | `BIT` | `BOOLEAN` | `INTEGER` | `BOOLEAN` |
| `I32` | `INT` | `INT4` | `INT` | `INTEGER` | `INTEGER` | `INTEGER` |
| `I64` | `BIGINT` | `INT8` | `BIGINT` | `INTEGER` | `INTEGER` | `BIGINT` |
| `U32` | `INT UNSIGNED` | `INT8` | `INT` | `INTEGER` | `INTEGER` | `UINTEGER` |
| `U64` | `BIGINT UNSIGNED` | `NUMERIC` | `BIGINT` | `INTEGER` | `INTEGER` | `UBIGINT` |
| `F32` | `FLOAT` | `FLOAT4` | `REAL` | `REAL` | `REAL` | `FLOAT` |
| `F64` | `DOUBLE` | `FLOAT8` | `FLOAT` | `REAL` | `REAL` | `DOUBLE` |
| `String` | `TEXT` | `VARCHAR` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |
| `Binary` | `BLOB` | `BYTEA` | `VARBINARY(MAX)` | `BLOB` | `BLOB` | `BLOB` |
| `Array` | `JSON` | `JSON` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |
| `Map` | `JSON` | `JSON` | `VARCHAR(MAX)` | `TEXT` | `TEXT` | `VARCHAR` |

### 3.1 无符号整数约束

| 驱动 | 规则 |
|---|---|
| MySQL | `U32 = INT UNSIGNED`，`U64 = BIGINT UNSIGNED`，完整范围可绑定 |
| PostgreSQL | 无 unsigned 类型：`U32 = INT8`，`U64 = NUMERIC` |
| SQL Server | `U32 -> i32`、`U64 -> i64`，越界直接报错 |
| SQLite/Turso | 仅 signed 64 位 INTEGER；`U64 > i64::MAX` 直接报错 |
| DuckDB | `U32 = UINTEGER`，`U64 = UBIGINT` |

禁止使用 `as i32/as i64` 做无检查转换。

## 4. 扩展类型语义

### 4.1 时间、时间戳、Decimal、UUID

| `Ext` | 语义 |
|---|---|
| `Date` | 日期字符串，例如 `2024-01-15` |
| `Time` | 时间字符串；驱动按数据库格式编码/解码 |
| `DateTime` | 带时区语义的完整日期时间或数据库 `DATETIME` 值 |
| `Timestamp` | **Unix epoch 毫秒数（`i64`）**，不是 SQL Server `timestamp/rowversion` |
| `Decimal` | 高精度十进制字符串 |
| `Uuid` | UUID 字符串，按当前驱动能力转文本或原生 UUID |
| `Json` | JSON 文本；`Value::Array/Map` 是首选结构化 JSON 入口 |
| `Jsonb` | PostgreSQL JSONB 扩展入口 |

- SQL Server `DateTimeOffset` 使用独立 `Ext("DateTimeOffset")`。
- PostgreSQL `DateTime` 固定映射为 `TIMESTAMPTZ`；`Timestamp` 映射为 `TIMESTAMP`。
- MySQL `Timestamp` 编码前转换为 UTC 日期时间分量；避免把本地时间直接写成 epoch 数值。

### 4.2 JSON 解码

- MySQL JSON 列解码为结构化 `Value`。
- PostgreSQL `JSON/JSONB` 解码为结构化 `Value`。
- SQLite 对 JSON 形状 TEXT 自动尝试解析。
- Turso 由 `json_detect` 控制，默认关闭。
- DuckDB VARCHAR 会尝试解析 JSON 形状文本。
- SQL Server JSON 是普通字符串列，不做自动 JSON 类型识别。

## 5. MySQL

### 5.1 DDL 与绑定类型

| `Ext` | DDL 类型 | 参数 wire 类型 | 说明 |
|---|---|---|---|
| `Uuid` | `TEXT` | `VarChar` | UUID 文本 |
| `Decimal` | `DECIMAL` | `NEWDECIMAL` | 严格解析 Decimal |
| `Year` | `YEAR` | `SHORT` 两字节 | 支持数值或数字字符串 |
| `Date` | `DATE` | `DATE` | MySQL binary date |
| `Time` | `TIME` | `TIME` | 支持微秒 |
| `Timestamp` | `TIMESTAMP` | `TIMESTAMP` | 输入 `i64` epoch millis |
| `DateTime` | `DATETIME` | `DATETIME` | 支持微秒 |
| `Json` | `JSON` | `JSON` | JSON 文本 |
| `Enum` | `TEXT` | `VarChar` | Connector/C 按字符串绑定 |
| `Set` | `TEXT` | `VarChar` | 逗号分隔字符串 |
| `Geometry` | `GEOMETRY` | `String` | 直接写入时 payload 必须是 `4 字节小端 SRID + WKB` |
| `Bit` | `BIT` | `BLOB` | 输入数值时转最小 big-endian 字节 |
| `Blob/TinyBlob/MediumBlob/LongBlob` | `BLOB` | `BLOB` | 二进制 |

### 5.2 空间类型

直接写入 GEOMETRY 字段：

```rust
Value::Ext("Geometry", Box::new(Value::Binary(srid_and_wkb)))
```

- `srid_and_wkb`：4 字节小端 SRID，后接 WKB。
- 使用 SQL 函数时，可传纯 WKB：`ST_GeomFromWKB(?)`。
- GEOMETRY 结果解码为 `Value::Ext("Geometry", Value::Binary(...))`。
- 不要使用 MySQL 不接受的 wire type `0xff GEOMETRY` 绑定参数。

### 5.3 BIT

- `Value::Ext("Bit", U64(n))`：编码为最短 big-endian 字节序列。
- 读取 `BIT` 结果：转换为 `Value::U64`。
- `BIT` 参数 wire type 使用 `BLOB`，不能直接使用 MySQL `BIT` 参数类型。

## 6. PostgreSQL

### 6.1 DDL 与参数类型

| `Ext` | PostgreSQL 类型 |
|---|---|
| `Uuid` | `UUID` |
| `Decimal` / `Numeric` | `NUMERIC` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Timestamp` | `TIMESTAMP` |
| `DateTime` | `TIMESTAMPTZ` |
| `Bool` | `BOOL` |
| `Bytea` | `BYTEA` |
| `Char` | `CHAR` |
| `Name` | `NAME` |
| `Int2` | `INT2` |
| `Int4` | `INT4` |
| `Int8` | `INT8` |
| `Text` | `TEXT` |
| `Oid` | `OID` |
| `Json` | `JSON` |
| `Jsonb` | `JSONB` |
| `Point/Lseg/Path/Box/Polygon/Line/Circle` | 对应几何类型 |
| `Cidr/Inet/Macaddr/Macaddr8` | 对应网络类型 |
| `Bit/Varbit` | 对应位串类型 |
| `Interval` | `INTERVAL` |
| `Timetz` | `TIMETZ` |
| `Int4Range/NumRange/TsRange/TstzRange/DateRange/Int8Range` | 对应 range 类型 |
| `Money` | `MONEY` |
| `Jsonpath` | `JSONPATH` |
| `Hstore` | 扩展类型 `hstore` |
| `TsVector` / `TsQuery` | 全文检索类型 |

### 6.2 数组规则

- `Value::Array` / `Value::Map` 固定按 JSON 参数绑定，通常对应 `JSON/JSONB`。
- PostgreSQL 原生数组只作为“数据库返回结果”兼容解码。
- 当前原生数组解码只支持一维数组，且下界必须为 1。
- 不要把 `Value::Array` 当作 PostgreSQL 原生数组参数。
- 类型层存在 `Vec<T>` 原生数组 codec，但动态 `Value` 绑定的正式语义仍是 JSON。

### 6.3 range、enum、domain、composite、hstore

- range 结果使用文本结果解码为 `Value::Ext("XxxRange", Value::String(...))`。
- 自定义 enum 解码为 `Value::String`。
- enum 数组解码为 `Value::Array`，元素为字符串。
- domain 通常按底层类型解码。
- composite 结果可能为 `Value::Ext("Custom", Value::String(...))`。
- 小型无类型自定义数组在无法解析类型 OID 时，会按通用 PostgreSQL 文本数组解析。
- `Hstore` 输出 `Value::Ext("Hstore", Value::Map(...))`；输入支持 Map、`key=>value` 字符串或 binary。

### 6.4 OID 内部规则

内部查询 `pg_type/pg_range/pg_enum` 必须使用：

```rust
Value::Ext("Oid", Box::new(Value::U32(oid)))
```

不能使用 `oid.0.into()`，因为 `U32` 的公开绑定规则是 `INT8`，与 PostgreSQL `oid` 比较会产生类型错误。

### 6.5 文本结果格式

以下类型会请求 PostgreSQL 文本结果，避免暴露不可解析协议二进制：

- `tsvector`
- `tsquery`
- range
- enum/composite
- hstore 及 hstore 数组
- range/custom 数组

普通标量和内置数组继续使用 binary 结果格式。

## 7. SQL Server

| `Ext` | DDL 类型 | 绑定/说明 |
|---|---|---|
| `Date` | `DATE` | `chrono::NaiveDate` |
| `DateTime` | `DATETIME2` | `chrono::NaiveDateTime` |
| `Time` | `TIME` | `chrono::NaiveTime` |
| `DateTimeOffset` | `DATETIMEOFFSET` | `chrono::DateTime<FixedOffset>` |
| `Decimal` | `DECIMAL` | `BigDecimal` |
| `Timestamp` | `BIGINT` | epoch millis，不是 ROWVERSION |
| `Uuid` | `UNIQUEIDENTIFIER` | 非法 UUID 报错 |
| `Json/Jsonb` | `NVARCHAR(MAX)` | JSON 文本 |
| `Xml` | `XML` | XML 文本 |
| `Geometry` | `GEOMETRY` | WKB 字节 |
| `Geography` | `GEOGRAPHY` | WKB 字节 |

### 空间类型绑定

SQL Server 推荐通过函数绑定 WKB：

```sql
geometry::STGeomFromWKB(@P1, 4326)
geography::STGeomFromWKB(@P1, 4326)
```

`Value::Ext("Geometry"/"Geography", Value::Binary(wkb))` 绑定为 `VARBINARY`。不要假定 SQL Server 内部 CLR 空间序列化格式与 WKB 相同。

## 8. SQLite

SQLite 使用动态存储类型；`column_type` 是声明类型建议，实际解码按存储类别进行。

| `Ext` | DDL 类型 | 存储/解码 |
|---|---|---|
| `Date` | `TEXT` | 文本 |
| `DateTime` | `TEXT` | 文本 |
| `Time` | `TEXT` | 文本 |
| `Timestamp` | `INTEGER` | epoch millis `i64` |
| `Decimal` | `NUMERIC` | 文本表示，解码为字符串 |
| `Json` | `TEXT` | JSON 文本 |
| `Uuid` | `TEXT` | UUID 文本 |

规则：

- `Array/Map` → JSON TEXT。
- 实际 INTEGER → `Value::I64`。
- 实际 REAL → `Value::F64`。
- 实际 TEXT → `Value::String`；JSON 形状文本会自动尝试解析为结构化 Value。
- 实际 BLOB → `Value::Binary`。
- `U64 > i64::MAX` 编码报错。
- 未知 `Ext` 不得绑定成 NULL。

## 9. Turso/libSQL

Turso 基于 libSQL，规则与 SQLite 接近，但 JSON 检测默认关闭。

| `Ext` | DDL 类型 | 存储/解码 |
|---|---|---|
| `Date/DateTime/Time` | `TEXT` | 文本 |
| `Timestamp` | `INTEGER` | epoch millis `i64` |
| `Decimal` | `TEXT` | 文本 |
| `Json` | `TEXT` | 原样 JSON 文本 |
| `Uuid` | `TEXT` | UUID 文本 |

规则：

- `Array/Map` → JSON TEXT。
- `Ext("Json", String)` → TEXT，不加额外引号。
- `Ext("Json", Binary)` → BLOB；普通 JSON 推荐 String 或 Array/Map。
- `json_detect=true` 时 JSON 形状 TEXT 解码为结构化 Value；默认关闭。
- `U64 > i64::MAX` 直接报错。
- 本地内存库和本地文件库均属于本地模式，不需要 Docker。

## 10. DuckDB

| `Ext` | DDL 类型 | 绑定/解码 |
|---|---|---|
| `Date` | `DATE` | `Value::Ext("Date", String)` |
| `DateTime` | `TIMESTAMP` | 根据参数类型绑定 timestamp/timestamptz |
| `Time` | `TIME` | 时间 |
| `Timestamp` | `BIGINT` | epoch millis |
| `Decimal` | `DECIMAL` | 十进制字符串 |
| `Json` | `VARCHAR` | JSON 文本 |
| `Uuid` | `UUID` | 32 位十六进制 UUID 转 `u128` |
| `Bit` | `BIT` | DuckDB padding bitstring 格式 |
| `Enum` | 数据库 enum | 按 enum dictionary index 绑定 |
| `HugeInt` | `HUGEINT` | `i128` |
| `UHugeInt` | `UHUGEINT` | `u128` |

规则：

- `Array/Map` → JSON VARCHAR。
- UUID 结果解码为 `Value::Ext("Uuid", String)`。
- BIT 结果解码为 `Value::Ext("Bit", String)`，例如 `"101"`。
- `TIMESTAMPTZ` 解码为 `Value::Ext("DateTime", String)`。
- `Ext("TimestampTz")` 是兼容入口，内部使用 `DateTime`。
- DuckDB 的 LIST/STRUCT/MAP/ARRAY/UNION 解码受 C API 版本影响；可能得到结构化 Value、字符串或 NULL，不能承诺跨版本完全一致。

## 11. 结果解码关键映射

| 数据库结果 | RBDC Value |
|---|---|
| MySQL unsigned TINY/SHORT/INT | `U32` |
| MySQL unsigned BIGINT | `U64` |
| MySQL BIT | `U64` |
| MySQL YEAR | `Ext("Year", String)` |
| PostgreSQL INT2 | `I32` |
| PostgreSQL INT4 | `I32` |
| PostgreSQL INT8 | `I64` |
| PostgreSQL OID | `Ext("Oid", U32)` |
| PostgreSQL JSON/JSONB | 结构化 Value |
| PostgreSQL range | `Ext("XxxRange", String)` |
| PostgreSQL enum | `String` |
| PostgreSQL hstore | `Ext("Hstore", Map)` |
| SQL Server UNIQUEIDENTIFIER | `Ext("Uuid", String)` |
| SQL Server XML | `Ext("Xml", String)` |
| SQL Server DATETIME/DATETIME2/DATETIMEOFFSET | `Ext("DateTime", String)` |
| SQLite/Turso INTEGER | `I64` |
| SQLite/Turso REAL | `F64` |
| SQLite/Turso TEXT | `String` 或 JSON 结构 |
| SQLite/Turso BLOB | `Binary` |

## 12. 修改类型映射的检查清单

1. 查数据库官方协议/驱动源码，确认 wire type、字节序、长度前缀和格式。
2. 先确定固定的 `Value -> SQL` 映射；禁止按数值大小改变目标 SQL 类型。
3. 同步修改：
   - `Driver::column_type`
   - `TypeInfo for Value`
   - `Encode`
   - `Decode`
   - `Ext` 分支
   - serde `Deserialize`（如需要）
4. 加测试：
   - 最小值、最大值、越界错误
   - NULL
   - 空值/空数组/空字符串
   - 实际数据库往返
   - `TypeInfo` serde roundtrip
5. Docker/本地真实数据库验证：
   - MySQL、PostgreSQL、SQL Server：Docker
   - SQLite、Turso、DuckDB：本地库直接测试
6. 更新 `docs/types.md`。
7. 修改受影响 crate 的 `Cargo.toml` 第三位版本号。
8. 全量运行 `cargo test --workspace -- --test-threads=1`。
9. 临时 `example` 测试代码必须回滚后再提交。

## 13. 不可重复的历史错误

- 不得把 `Datetime` 作为规范名称，统一使用 `DateTime`。
- 不得让同一 `U32/U64` 根据值大小在 `INT4/INT8/NUMERIC` 间漂移。
- 不得将 `u64` 越界静默降级为字符串。
- 不得把非法 Decimal/Year/UUID/Timestamp 静默写成 0/空串/NULL。
- 不得让未知 `Ext` 静默绑定 NULL。
- 不得用 `oid.0.into()` 处理 PostgreSQL OID 参数。
- 不得把 `Value::Array/Map` 改为数据库原生数组绑定；它们必须保持 JSON 语义。
- 不得把 MySQL GEOMETRY 参数声明为不可绑定的 wire type。
- 不得把 SQL Server WKB 当作内部 CLR 空间序列化格式。
- 不得只改绑定而漏掉解码、DDL、serde 或测试。
