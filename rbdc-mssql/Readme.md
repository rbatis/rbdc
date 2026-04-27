# rbdc-mssql

Microsoft SQL Server database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer, based on [tiberius](https://github.com/prisma/tiberius).

## Features

- Multiple connection string format support
- High-performance async connection based on tiberius
- Full SQL Server data type support
- Connection pooling support
- Zero-copy serialization/deserialization

## Supported Connection String Formats

### 1. JDBC format
```
jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;
```

### 2. mssql:// URL format
```
mssql://SA:TestPass!123456@localhost:1433/master
```

### 3. sqlserver:// URL format
```
sqlserver://SA:TestPass!123456@localhost:1433/master
```

### 4. ADO.NET format
```
Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;
```

## Usage

```rust
use rbdc::pool::ConnectionManager;
use rbdc_mssql::MssqlDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use any supported connection string format
    let uri = "mssql://SA:TestPass!123456@localhost:1433/master";

    // Create connection manager
    let manager = ConnectionManager::new(MssqlDriver {}, uri)?;

    // Use connection pool
    let pool = rbdc_pool_fast::FastPool::new(manager)?;
    let mut conn = pool.get().await?;

    // Execute query
    let result = conn.exec_decode("SELECT 1 as test", vec![]).await?;
    println!("Result: {:?}", result);

    Ok(())
}
```

## URL Format说明

URL format connection strings follow the standard URL structure:

```
scheme://[username[:password]@]host[:port][/database][?parameters]
```

- **scheme**: `mssql` or `sqlserver`
- **username**: Database username
- **password**: Database password (optional)
- **host**: Server hostname or IP address
- **port**: Port number (default 1433)
- **database**: Database name (optional)

### Special Character Handling

URL format automatically handles special characters in username and password (URL encoding/decoding).

## RBDC Architecture

- Database driver abstraction layer
- Zero-copy serialization/deserialization

Data flow: Database -> bytes -> rbs::Value -> Struct(User Define)
Reverse: Struct(User Define) -> rbs::ValueRef -> ref clone() -> Database

### How to Define a Custom Driver?

Implement the following traits and load the driver:
* `impl trait rbdc::db::{Driver, MetaData, Row, Connection, ConnectOptions, Placeholder}`

## Dependencies

- [tiberius](https://github.com/prisma/tiberius) - SQL Server client
- [url](https://github.com/servo/rust-url) - URL parsing
- [percent-encoding](https://github.com/servo/rust-url/tree/master/percent_encoding) - URL encoding

## License

This project is licensed under the same license as rbdc.
