# rbdc-mysql

MySQL database driver for the [rbdc](https://github.com/rbatis/rbatis) database abstraction layer.

## Basic Driver Usage

Full example: [example/src/mysql.rs](../example/src/mysql.rs)

```rust
use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_mysql::MysqlDriver;
use rbdc_pool_fast::FastPool;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let url = "mysql://root:123456@localhost:3306/test";
    let pool = FastPool::new_url(MysqlDriver {}, url)?;
    let mut conn = pool.get().await?;
    let v = conn.exec_decode("SHOW TABLES;", vec![]).await?;
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
    rb.init(rbdc_mysql::MysqlDriver {}, "mysql://root:123456@localhost:3306/test")?;
    Ok(())
}
```

## Supported Connection String Formats

### 1. Standard URL format
```
mysql://user:password@host:port/database
```

### 2. JDBC-style format
```
jdbc:mysql://host:port/database?user=user&password=password
```

## License

This project is licensed under the same license as rbdc.
