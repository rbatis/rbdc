use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_duckdb::DuckDbDriver;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Create a connection pool for DuckDB
    // DuckDB supports in-memory database (":memory:") or file-based database
    let pool = FastPool::new_url(DuckDbDriver {}, "duckdb://target/duckdb.db")?;

    let mut conn = pool.get().await?;

    // Create a table
    conn.exec(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        vec![],
    )
    .await?;

    // Insert some data
    conn.exec(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        vec![rbs::Value::I32(1), rbs::Value::String("Alice".to_string()), rbs::Value::String("alice@example.com".to_string())],
    )
    .await?;

    conn.exec(
        "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
        vec![rbs::Value::I32(2), rbs::Value::String("Bob".to_string()), rbs::Value::String("bob@example.com".to_string())],
    )
    .await?;

    // Query data
    let v = conn
        .exec_decode("SELECT * FROM users", vec![])
        .await?;
    println!("Users: {}", v);

    // Query with parameters
    let v = conn
        .exec_decode("SELECT * FROM users WHERE id = ?", vec![rbs::Value::I32(1)])
        .await?;
    println!("User with id=1: {}", v);

    // For file-based database, use:
    // let pool = FastPool::new_url(DuckDbDriver {}, "duckdb:///path/to/database.db")?;

    Ok(())
}
