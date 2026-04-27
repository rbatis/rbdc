# rbdc-mysql

MySQL database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Features

- High-performance async connection based on native MySQL protocol
- Full MySQL data type support
- Connection pooling support
- Zero-copy serialization/deserialization
- TLS support (rustls and native-tls)

## Supported Connection String Formats

### 1. Standard URL format
```
mysql://user:password@host:port/database
```

### 2. JDBC-style format
```
jdbc:mysql://host:port/database?user=user&password=password
```

## Usage

```rust
use rbdc::pool::ConnectionManager;
use rbdc_mysql::MysqlDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connection string
    let uri = "mysql://root:password@localhost:3306/test";

    // Create connection manager
    let manager = ConnectionManager::new(MysqlDriver {}, uri)?;

    // Use connection pool
    let pool = rbdc_pool_fast::FastPool::new(manager)?;
    let mut conn = pool.get().await?;

    // Execute query
    let result = conn.exec_decode("SELECT 1 as test", vec![]).await?;
    println!("Result: {:?}", result);

    Ok(())
}
```

## RBDC Architecture

- Database driver abstraction layer
- Zero-copy serialization/deserialization

Data flow: Database -> bytes -> rbs::Value -> Struct(User Define)
Reverse: Struct(User Define) -> rbs::ValueRef -> ref clone() -> Database

### How to Define a Custom Driver?

Implement the following traits and load the driver:
* `impl trait rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder}`

## Dependencies

- [mysql_async](https://github.com/nature320/mysql-async) - Async MySQL database driver
- [url](https://github.com/servo/rust-url) - URL parsing
- [percent-encoding](https://github.com/servo/rust-url/tree/master/percent_encoding) - URL encoding

## License

This project is licensed under the same license as rbdc.
