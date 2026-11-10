//! Coverage for connection options, URI parsing, enums and the driver.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rbdc::db::{ConnectOptions, Connection, Driver, Placeholder};
use rbdc_sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteDriver, SqliteJournalMode, SqliteLockingMode,
    SqliteSynchronous,
};
use rbs::Value;

static SEQ: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let mut names = vec![self.path.clone()];
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut extra = self.path.clone();
            extra.set_file_name(format!(
                "{}{}",
                self.path.file_name().unwrap().to_string_lossy(),
                suffix
            ));
            names.push(extra);
        }
        for name in names {
            let _ = std::fs::remove_file(name);
        }
    }
}

fn temp_db_path() -> TempDb {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    TempDb {
        path: std::env::temp_dir().join(format!(
            "rbdc-sqlite-options-{}-{}.db",
            std::process::id(),
            n
        )),
    }
}

#[tokio::test]
async fn test_default_options_and_all_setters() {
    let options = SqliteConnectOptions::default();
    // Exercise the derived Debug implementation.
    let debug = format!("{:?}", options);
    assert!(debug.contains("SqliteConnectOptions"));

    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .foreign_keys(true)
        .shared_cache(false)
        .journal_mode(SqliteJournalMode::Memory)
        .locking_mode(SqliteLockingMode::Normal)
        .read_only(false)
        .create_if_missing(true)
        .statement_cache_capacity(32)
        .busy_timeout(Duration::from_millis(250))
        .synchronous(SqliteSynchronous::Normal)
        .auto_vacuum(SqliteAutoVacuum::None)
        .page_size(8192)
        .pragma("cache_size", "-2000")
        .serialized(false)
        .thread_name(|id| format!("opts-worker-{}", id))
        .command_buffer_size(10)
        .row_buffer_size(10);

    let mut conn = options.connect().await.expect("connect");
    conn.exec("CREATE TABLE t (id INTEGER)", vec![]).await.unwrap();
    let size = conn
        .exec_decode("PRAGMA page_size", vec![])
        .await
        .unwrap();
    assert_eq!(size.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_every_enum_variant_is_applied() {
    // Connecting with each variant exercises `as_str` for all enum values.
    let journal_modes = [
        SqliteJournalMode::Delete,
        SqliteJournalMode::Truncate,
        SqliteJournalMode::Persist,
        SqliteJournalMode::Memory,
        SqliteJournalMode::Wal,
        SqliteJournalMode::Off,
    ];
    for mode in journal_modes {
        let mut conn = SqliteConnectOptions::new()
            .filename(":memory:")
            .journal_mode(mode)
            .connect()
            .await
            .unwrap_or_else(|e| panic!("journal mode {:?} failed: {}", mode, e));
        conn.exec("SELECT 1", vec![]).await.unwrap();
    }

    for mode in [
        SqliteLockingMode::Normal,
        SqliteLockingMode::Exclusive,
    ] {
        let mut conn = SqliteConnectOptions::new()
            .filename(":memory:")
            .locking_mode(mode)
            .connect()
            .await
            .unwrap_or_else(|e| panic!("locking mode {:?} failed: {}", mode, e));
        conn.exec("SELECT 1", vec![]).await.unwrap();
    }

    for mode in [
        SqliteSynchronous::Off,
        SqliteSynchronous::Normal,
        SqliteSynchronous::Full,
        SqliteSynchronous::Extra,
    ] {
        let mut conn = SqliteConnectOptions::new()
            .filename(":memory:")
            .synchronous(mode)
            .connect()
            .await
            .unwrap_or_else(|e| panic!("synchronous {:?} failed: {}", mode, e));
        conn.exec("SELECT 1", vec![]).await.unwrap();
    }

    for mode in [
        SqliteAutoVacuum::None,
        SqliteAutoVacuum::Full,
        SqliteAutoVacuum::Incremental,
    ] {
        let mut conn = SqliteConnectOptions::new()
            .filename(":memory:")
            .auto_vacuum(mode)
            .connect()
            .await
            .unwrap_or_else(|e| panic!("auto_vacuum {:?} failed: {}", mode, e));
        conn.exec("SELECT 1", vec![]).await.unwrap();
    }
}

#[test]
fn test_enum_from_str_and_defaults() {
    use SqliteAutoVacuum::*;
    assert_eq!(SqliteAutoVacuum::default(), None);
    assert_eq!(SqliteAutoVacuum::from_str("none").unwrap(), None);
    assert_eq!(SqliteAutoVacuum::from_str("FULL").unwrap(), Full);
    assert_eq!(SqliteAutoVacuum::from_str("incremental").unwrap(), Incremental);
    assert!(SqliteAutoVacuum::from_str("bogus").is_err());

    assert_eq!(SqliteJournalMode::default(), SqliteJournalMode::Wal);
    assert_eq!(
        SqliteJournalMode::from_str("delete").unwrap(),
        SqliteJournalMode::Delete
    );
    assert_eq!(
        SqliteJournalMode::from_str("TRUNCATE").unwrap(),
        SqliteJournalMode::Truncate
    );
    assert_eq!(
        SqliteJournalMode::from_str("persist").unwrap(),
        SqliteJournalMode::Persist
    );
    assert_eq!(
        SqliteJournalMode::from_str("memory").unwrap(),
        SqliteJournalMode::Memory
    );
    assert_eq!(
        SqliteJournalMode::from_str("wal").unwrap(),
        SqliteJournalMode::Wal
    );
    assert_eq!(
        SqliteJournalMode::from_str("off").unwrap(),
        SqliteJournalMode::Off
    );
    assert!(SqliteJournalMode::from_str("bogus").is_err());

    assert_eq!(SqliteLockingMode::default(), SqliteLockingMode::Normal);
    assert_eq!(
        SqliteLockingMode::from_str("normal").unwrap(),
        SqliteLockingMode::Normal
    );
    assert_eq!(
        SqliteLockingMode::from_str("EXCLUSIVE").unwrap(),
        SqliteLockingMode::Exclusive
    );
    assert!(SqliteLockingMode::from_str("bogus").is_err());

    assert_eq!(SqliteSynchronous::default(), SqliteSynchronous::Full);
    assert_eq!(SqliteSynchronous::from_str("off").unwrap(), SqliteSynchronous::Off);
    assert_eq!(
        SqliteSynchronous::from_str("normal").unwrap(),
        SqliteSynchronous::Normal
    );
    assert_eq!(SqliteSynchronous::from_str("full").unwrap(), SqliteSynchronous::Full);
    assert_eq!(
        SqliteSynchronous::from_str("extra").unwrap(),
        SqliteSynchronous::Extra
    );
    assert!(SqliteSynchronous::from_str("bogus").is_err());

    // Debug impls of the enums.
    assert!(format!("{:?}", SqliteJournalMode::Wal).contains("Wal"));
    assert!(format!("{:?}", SqliteAutoVacuum::Full).contains("Full"));
}

#[tokio::test]
async fn test_uri_parsing_variants() {
    // `sqlite::memory:` and `sqlite://:memory:` both select an in-memory database.
    for uri in ["sqlite::memory:", "sqlite://:memory:", "sqlite://?mode=memory"] {
        let mut conn = uri
            .parse::<SqliteConnectOptions>()
            .unwrap()
            .connect()
            .await
            .unwrap_or_else(|e| panic!("{}: {}", uri, e));
        conn.exec("CREATE TABLE t (id INTEGER)", vec![]).await.unwrap();
    }

    // mode=rw, mode=rwc, cache private/shared, immutable flags.
    for uri in [
        "sqlite://a.db?mode=rw",
        "sqlite://a.db?mode=rwc",
        "sqlite://a.db?cache=shared",
        "sqlite://a.db?cache=private",
        "sqlite://a.db?immutable=true",
        "sqlite://a.db?immutable=1",
        "sqlite://a.db?immutable=false",
        "sqlite://a.db?immutable=0",
    ] {
        let opts = uri
            .parse::<SqliteConnectOptions>()
            .unwrap_or_else(|e| panic!("{}: {}", uri, e));
        // Do not actually connect to `a.db`; just verify parsing produced options.
        assert!(format!("{:?}", opts).contains("a.db"));
    }

    let opts = "sqlite://a.db?mode=ro".parse::<SqliteConnectOptions>().unwrap();
    assert!(format!("{:?}", opts).contains("read_only: true"));

    let opts = "sqlite://a.db?mode=memory".parse::<SqliteConnectOptions>().unwrap();
    assert!(format!("{:?}", opts).contains("in_memory: true"));

    // Percent decoding of the database path.
    let opts = "sqlite://a%20b.db".parse::<SqliteConnectOptions>().unwrap();
    assert!(format!("{:?}", opts).contains("a b.db"));

    // Trailing parameters after `?` are parsed as key/value pairs.
    let opts = "sqlite://host%3Fname.db?cache=shared"
        .parse::<SqliteConnectOptions>()
        .unwrap();
    assert!(format!("{:?}", opts).contains("host?name.db"));
}

#[test]
fn test_uri_parse_errors() {
    assert!("sqlite://a.db?mode=bogus"
        .parse::<SqliteConnectOptions>()
        .is_err());
    assert!("sqlite://a.db?cache=bogus"
        .parse::<SqliteConnectOptions>()
        .is_err());
    assert!("sqlite://a.db?immutable=bogus"
        .parse::<SqliteConnectOptions>()
        .is_err());
    assert!("sqlite://a.db?unknown=value"
        .parse::<SqliteConnectOptions>()
        .is_err());
    // Invalid UTF-8 after percent decoding.
    assert!("sqlite://%FF".parse::<SqliteConnectOptions>().is_err());
}

#[test]
fn test_set_uri_and_connect_options_trait() {
    let mut opts = SqliteConnectOptions::new();
    ConnectOptions::set_uri(&mut opts, "sqlite::memory:").unwrap();
    assert!(format!("{:?}", opts).contains("in_memory: true"));

    let err = ConnectOptions::set_uri(&mut opts, "sqlite://a.db?mode=bogus").unwrap_err();
    assert!(!err.to_string().is_empty());

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let opts = SqliteConnectOptions::new().filename(":memory:");
        let mut conn = ConnectOptions::connect(&opts).await.unwrap();
        conn.exec("SELECT 1", vec![]).await.unwrap();
    });
}

#[test]
fn test_deserialize_options() {
    let json = r#"{
        "filename": ":memory:",
        "in_memory": true,
        "read_only": false,
        "create_if_missing": true,
        "shared_cache": true,
        "statement_cache_capacity": 42,
        "busy_timeout": {"secs": 3, "nanos": 0},
        "immutable": false,
        "command_channel_size": 12,
        "row_channel_size": 13,
        "serialized": true
    }"#;
    let opts: SqliteConnectOptions = serde_json::from_str(json).expect("deserialize options");
    let debug = format!("{:?}", opts);
    assert!(debug.contains("in_memory: true"));
    assert!(debug.contains("shared_cache: true"));
    assert!(debug.contains("statement_cache_capacity: 42"));
    assert!(debug.contains("serialized: true"));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut conn = opts.connect().await.expect("connect deserialized options");
        conn.exec("SELECT 1", vec![]).await.unwrap();
    });
}

#[tokio::test]
async fn test_establish_error_paths() {
    // Filenames containing nul bytes are rejected before opening.
    let err = SqliteConnectOptions::new()
        .filename("bad\0name.db")
        .connect()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nul bytes"));

    // Busy timeout that overflows i32 milliseconds is rejected.
    let err = SqliteConnectOptions::new()
        .filename(":memory:")
        .busy_timeout(Duration::from_millis(i32::MAX as u64 + 1))
        .connect()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("too big"));

    // Read-only connection to a file that does not exist fails.
    let missing = temp_db_path();
    let err = SqliteConnectOptions::new()
        .filename(missing.path())
        .read_only(true)
        .connect()
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());

    // Similarly, refusing to create a missing database fails.
    let err = SqliteConnectOptions::new()
        .filename(missing.path())
        .create_if_missing(false)
        .connect()
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
async fn test_file_database_read_only_and_immutable() {
    let db = temp_db_path();

    // Create and populate a real file database.
    {
        let mut conn = SqliteConnectOptions::new()
            .filename(db.path())
            .connect()
            .await
            .expect("create file db");
        conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", vec![])
            .await
            .unwrap();
        conn.exec("INSERT INTO t (v) VALUES ('hello')", vec![])
            .await
            .unwrap();
        conn.close().await.unwrap();
    }
    assert!(db.path().exists());

    // Re-open read-only through a URI.
    let uri = format!("sqlite://{}?mode=ro", db.path().display());
    let mut conn = uri
        .parse::<SqliteConnectOptions>()
        .unwrap()
        .connect()
        .await
        .expect("open read-only");
    let decoded = conn.exec_decode("SELECT v FROM t", vec![]).await.unwrap();
    assert_eq!(
        decoded.as_array().unwrap()[0].as_map().unwrap()["v"],
        Value::String("hello".into())
    );
    let err = conn.exec("INSERT INTO t (v) VALUES ('nope')", vec![]).await.unwrap_err();
    assert!(!err.to_string().is_empty());
    conn.close().await.unwrap();

    // Immutable connections skip locking and can still read.
    let mut conn = SqliteConnectOptions::new()
        .filename(db.path())
        .immutable(true)
        .read_only(true)
        .connect()
        .await
        .expect("open immutable");
    let decoded = conn.exec_decode("SELECT COUNT(*) AS c FROM t", vec![]).await.unwrap();
    assert_eq!(
        decoded.as_array().unwrap()[0].as_map().unwrap()["c"],
        Value::I64(1)
    );
    conn.close().await.unwrap();

    // The special `key` pragma is emitted before all other pragmas. Plain
    // SQLite ignores unknown pragmas, so this still connects.
    let mut conn = SqliteConnectOptions::new()
        .filename(":memory:")
        .pragma("key", "password")
        .connect()
        .await
        .expect("connect with key pragma");
    conn.exec("SELECT 1", vec![]).await.unwrap();
}

#[tokio::test]
async fn test_driver() {
    let driver = SqliteDriver {};

    assert_eq!(driver.name(), "sqlite");
    assert_eq!(driver.exchange("SELECT ?"), "SELECT ?");
    let default = driver.default_option();
    assert!(default.downcast_ref::<SqliteConnectOptions>().is_some());

    let mut conn = driver.connect("sqlite::memory:").await.expect("driver connect");
    assert_eq!(
        conn.exec("SELECT 1", vec![]).await.unwrap().rows_affected,
        0
    );
    conn.close().await.unwrap();

    let err = driver
        .connect("sqlite://a.db?mode=bogus")
        .await
        .err()
        .expect("invalid URI must fail");
    assert!(!err.to_string().is_empty());

    let opts = SqliteConnectOptions::new().filename(":memory:");
    let mut conn = driver.connect_opt(&opts).await.expect("connect_opt");
    conn.exec("SELECT 1", vec![]).await.unwrap();
}

#[test]
fn test_driver_column_type() {
    let driver = SqliteDriver {};

    let cases = vec![
        (Value::Null, "NULL"),
        (Value::Bool(true), "BOOLEAN"),
        (Value::I32(1), "INTEGER"),
        (Value::I64(1), "INTEGER"),
        (Value::U32(1), "INTEGER"),
        (Value::U64(1), "INTEGER"),
        (Value::F32(1.0), "REAL"),
        (Value::F64(1.0), "REAL"),
        (Value::String("x".into()), "TEXT"),
        (Value::Binary(vec![1]), "BLOB"),
        (Value::Array(vec![Value::I64(1)]), "TEXT"),
        (
            Value::Map(rbs::value::map::ValueMap::new()),
            "TEXT",
        ),
        (
            Value::Ext("Date", Box::new(Value::String("2020-01-01".into()))),
            "TEXT",
        ),
        (
            Value::Ext("DateTime", Box::new(Value::String("2020-01-01".into()))),
            "TEXT",
        ),
        (
            Value::Ext("Time", Box::new(Value::String("12:00".into()))),
            "TEXT",
        ),
        (
            Value::Ext("Timestamp", Box::new(Value::I64(1))),
            "INTEGER",
        ),
        (
            Value::Ext("Decimal", Box::new(Value::String("1.0".into()))),
            "NUMERIC",
        ),
        (
            Value::Ext("Json", Box::new(Value::String("{}".into()))),
            "BLOB",
        ),
        (
            Value::Ext("Uuid", Box::new(Value::String("abc".into()))),
            "TEXT",
        ),
        (Value::Ext("Unknown", Box::new(Value::Null)), "NULL"),
    ];

    for (value, expected) in cases {
        assert_eq!(driver.column_type(&value), expected, "value: {:?}", value);
    }
}

#[tokio::test]
async fn test_invalid_pragma_query_errors() {
    // The generated init statement is executed with `sqlite3_exec`; invalid
    // trailing SQL takes the error path.
    let err = SqliteConnectOptions::new()
        .filename(":memory:")
        .pragma("bad_pragma", "1; NOT SQL")
        .connect()
        .await
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn test_non_utf8_filename_is_rejected() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let name = OsStr::from_bytes(b"invalid-\xff.db");
    let err = SqliteConnectOptions::new()
        .filename(std::path::Path::new(name))
        .connect()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("valid UTF-8"));
}
