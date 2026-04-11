use rbdc::Error;
use rbdc::db::Connection;
use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;
use rbdc_turso::TursoDriver;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // In-memory database example (local, no network)
    // let pool = FastPool::new_url(TursoDriver {}, "turso://:memory:")?;

    // Local file database example
    let pool = FastPool::new_url(TursoDriver {}, "turso://target/turso.db")?;

    // Remote Turso database example (requires TURSO_URL and TURSO_TOKEN environment variables)
    // let url = std::env::var("TURSO_URL").unwrap_or_else(|_| "libsql://your-db.turso.io".to_string());
    // let token = std::env::var("TURSO_TOKEN").unwrap_or_default();
    // let pool = FastPool::new_url(
    //     TursoDriver {},
    //     &format!("turso://?url={}&token={}", url, token)
    // )?;

    pool.set_conn_max_lifetime(Some(Duration::from_secs(10)))
        .await;
    let mut conn = pool.get().await?;

    // Create test table
    conn.exec(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)",
        vec![],
    )
    .await?;

    // Insert data
    conn.exec("INSERT INTO users (name) VALUES (?)", vec!["Alice".into()])
        .await?;
    conn.exec("INSERT INTO users (name) VALUES (?)", vec!["Bob".into()])
        .await?;

    // Query data
    let v = conn.exec_decode("SELECT * FROM users", vec![]).await?;
    println!("Query result: {}", v);

    Ok(())
}
