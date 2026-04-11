# rbdc-mssql

rbdc-mssql 是一个基于 [tiberius](https://github.com/prisma/tiberius) 的 SQL Server 数据库驱动，为 rbdc 框架提供 Microsoft SQL Server 连接支持。

## 特性

- 支持多种连接字符串格式
- 基于 tiberius 的高性能异步连接
- 完整的 SQL Server 数据类型支持
- 连接池支持
- 零拷贝序列化/反序列化

## 支持的连接字符串格式

rbdc-mssql 现在支持以下四种连接字符串格式：

### 1. JDBC 格式 (原有支持)
```
jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;
```

### 2. mssql:// URL 格式 (新增)
```
mssql://SA:TestPass!123456@localhost:1433/master
```

### 3. sqlserver:// URL 格式 (新增)
```
sqlserver://SA:TestPass!123456@localhost:1433/master
```

### 4. ADO.NET 格式 (原有支持)
```
Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;
```

## 使用示例

```rust
use rbdc::pool::ConnectionManager;
use rbdc_mssql::MssqlDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 使用任意支持的连接字符串格式
    let uri = "mssql://SA:TestPass!123456@localhost:1433/master";

    // 创建连接管理器
    let manager = ConnectionManager::new(MssqlDriver {}, uri)?;

    // 使用连接池
    let pool = rbdc_pool_fast::FastPool::new(manager)?;
    let mut conn = pool.get().await?;

    // 执行查询
    let result = conn.exec_decode("SELECT 1 as test", vec![]).await?;
    println!("查询结果: {:?}", result);

    Ok(())
}
```

## URL 格式说明

URL 格式的连接字符串遵循标准的 URL 结构：

```
scheme://[username[:password]@]host[:port][/database][?parameters]
```

- **scheme**: `mssql` 或 `sqlserver`
- **username**: 数据库用户名
- **password**: 数据库密码（可选）
- **host**: 服务器主机名或 IP 地址
- **port**: 端口号（默认 1433）
- **database**: 数据库名称（可选）

### 特殊字符处理

URL 格式会自动处理用户名和密码中的特殊字符（URL 编码/解码）。

## RBDC 架构

* 数据库驱动抽象层
* 支持零拷贝序列化/反序列化

数据流：Database -> bytes -> rbs::Value -> Struct(User Define)
反向流：Struct(User Define) -> rbs::ValueRef -> ref clone() -> Database

### 如何定义自定义驱动？
需要实现相关 trait 并加载驱动：
* impl trait rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder};

## 依赖

- [tiberius](https://github.com/prisma/tiberius) - 底层 SQL Server 客户端
- [url](https://github.com/servo/rust-url) - URL 解析
- [percent-encoding](https://github.com/servo/rust-url/tree/master/percent_encoding) - URL 编码处理

## 许可证

本项目采用与 rbdc 相同的许可证。
