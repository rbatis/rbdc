//! Tests for exec_rows() streaming interface with SQLite
//! Processing rows one by one without collecting all results into memory first.

use rbs::Value;
use rbdc::db::Connection;
use rbdc_sqlite::{SqliteConnectOptions, SqliteConnection};

async fn create_conn() -> SqliteConnection {
    SqliteConnectOptions::new()
        .filename(":memory:")
        .connect()
        .await
        .expect("failed to create in-memory SQLite connection")
}

async fn setup(conn: &mut SqliteConnection) {
    conn.exec(
        "CREATE TABLE IF NOT EXISTS test_stream (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)",
        vec![],
    )
    .await
    .expect("create table");
    conn.exec("INSERT OR IGNORE INTO test_stream VALUES (1, 'Alice', 100)", vec![])
        .await
        .expect("insert 1");
    conn.exec("INSERT OR IGNORE INTO test_stream VALUES (2, 'Bob', 85)", vec![])
        .await
        .expect("insert 2");
    conn.exec("INSERT OR IGNORE INTO test_stream VALUES (3, 'Charlie', 92)", vec![])
        .await
        .expect("insert 3");
}

#[tokio::test]
async fn test_stream_empty_result() {
    let mut conn = create_conn().await;
    setup(&mut conn).await;

    let mut stream = conn
        .exec_rows("SELECT * FROM test_stream WHERE 1 = 0", vec![])
        .await
        .expect("stream");

    use futures_util::StreamExt;
    let item = stream.next().await;
    assert!(item.is_none(), "no rows expected");
}

#[tokio::test]
async fn test_stream_multiple_rows() {
    let mut conn = create_conn().await;
    setup(&mut conn).await;

    let mut stream = conn
        .exec_rows("SELECT * FROM test_stream ORDER BY id", vec![])
        .await
        .expect("stream");

    use futures_util::StreamExt;
    let mut count = 0;
    while let Some(row) = stream.next().await {
        let row = row.expect("row");
        let md = row.meta_data();
        if count == 0 {
            assert_eq!(md.column_name(0), "id");
            assert_eq!(md.column_name(1), "name");
            assert_eq!(md.column_name(2), "score");
        }
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_stream_with_params() {
    let mut conn = create_conn().await;
    setup(&mut conn).await;

    let mut stream = conn
        .exec_rows(
            "SELECT name, score FROM test_stream WHERE score >= ?",
            vec![Value::I64(90)],
        )
        .await
        .expect("stream");

    use futures_util::StreamExt;
    let mut count = 0;
    while let Some(row) = stream.next().await {
        let _row = row.expect("row");
        count += 1;
    }
    assert_eq!(count, 2, "Alice (100) and Charlie (92) should match");
}

#[tokio::test]
async fn test_stream_exec_decode() {
    let mut conn = create_conn().await;
    setup(&mut conn).await;

    let value = conn
        .exec_decode("SELECT id, name FROM test_stream ORDER BY id", vec![])
        .await
        .expect("exec_decode");

    use rbs::Value;
    match value {
        Value::Array(arr) => {
            assert_eq!(arr.len(), 3);
        }
        _ => panic!("expected Array"),
    }
}

#[tokio::test]
async fn test_stream_consume_twice() {
    let mut conn = create_conn().await;
    setup(&mut conn).await;

    // First stream - drop before second to release borrow
    let ids = {
        let mut s1 = conn
            .exec_rows("SELECT id FROM test_stream ORDER BY id", vec![])
            .await
            .expect("stream");

        use futures_util::StreamExt;
        let mut ids: Vec<i32> = Vec::new();
        while let Some(row) = s1.next().await {
            let mut row = row.expect("row");
            if let Ok(Value::I64(id)) = row.get(0) {
                ids.push(id as i32);
            }
        }
        ids
    };
    assert_eq!(ids, vec![1, 2, 3]);

    // Second stream
    let mut s2 = conn
        .exec_rows("SELECT id FROM test_stream ORDER BY id", vec![])
        .await
        .expect("stream");

    use futures_util::StreamExt;
    let mut ids2: Vec<i32> = Vec::new();
    while let Some(row) = s2.next().await {
        let mut row = row.expect("row");
        if let Ok(Value::I64(id)) = row.get(0) {
            ids2.push(id as i32);
        }
    }
    assert_eq!(ids2, vec![1, 2, 3]);
}
