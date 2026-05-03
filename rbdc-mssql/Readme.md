# rbdc-mssql

Microsoft SQL Server database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Basic Driver Usage

Full example: [example/src/mssql.rs](../example/src/mssql.rs)

```rust
use rbdc::Error;
use rbdc::pool::Pool;
use rbdc_mssql::MssqlDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Supported formats:
    // let uri = "jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;";
    // let uri = "sqlserver://SA:TestPass!123456@localhost:1433/master";
    // let uri = "Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;";
    let uri = "mssql://SA:TestPass!123456@localhost:1433/master";
    let pool = FastPool::new_url(MssqlDriver {}, uri)?;
    let mut conn = pool.get().await?;
    let v = conn.exec_decode("SELECT DB_NAME() AS CurrentDatabase", vec![]).await?;
    println!("{}", v);
    // if need decode use `let result:Vec<Table> = rbs::from_value(v)?;`
    Ok(())
}
```

## Usage with rbatis ORM

```rust
use rbatis::RBatis;
use rbatis::Error;

#[tokio::main]
pub async fn main() -> Result<(), Error> {
    let rb = RBatis::new();
    rb.init(rbdc_mssql::MssqlDriver {}, "mssql://SA:TestPass!123456@localhost:1433/master")?;
    Ok(())
}
```

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

## License

This project is licensed under the same license as rbdc.
