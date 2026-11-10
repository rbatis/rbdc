//! Offline coverage for connection options, URI parsing, SSL modes and the
//! driver facade.

use std::str::FromStr;

use rbdc::db::{ConnectOptions, Driver, Placeholder};
use rbdc_mysql::driver::MysqlDriver;
use rbdc_mysql::options::{MySqlConnectOptions, MySqlSslMode};
use rbs::Value;

// ---------------------------------------------------------------------------
// SSL mode
// ---------------------------------------------------------------------------

#[test]
fn test_ssl_mode_from_str() {
    assert!(matches!(
        MySqlSslMode::from_str("disabled").unwrap(),
        MySqlSslMode::Disabled
    ));
    assert!(matches!(
        MySqlSslMode::from_str("preferred").unwrap(),
        MySqlSslMode::Preferred
    ));
    assert!(matches!(
        MySqlSslMode::from_str("required").unwrap(),
        MySqlSslMode::Required
    ));
    assert!(matches!(
        MySqlSslMode::from_str("verify_ca").unwrap(),
        MySqlSslMode::VerifyCa
    ));
    assert!(matches!(
        MySqlSslMode::from_str("verify_identity").unwrap(),
        MySqlSslMode::VerifyIdentity
    ));
    // Parsing is case insensitive.
    assert!(matches!(
        MySqlSslMode::from_str("REQUIRED").unwrap(),
        MySqlSslMode::Required
    ));
    assert!(MySqlSslMode::from_str("verify-ca").is_err());
    assert!(MySqlSslMode::from_str("bogus").is_err());
}

#[test]
fn test_ssl_mode_default_and_serde() {
    assert!(matches!(MySqlSslMode::default(), MySqlSslMode::Disabled));

    for mode in [
        MySqlSslMode::Disabled,
        MySqlSslMode::Preferred,
        MySqlSslMode::Required,
        MySqlSslMode::VerifyCa,
        MySqlSslMode::VerifyIdentity,
    ] {
        let json = serde_json::to_string(&mode).unwrap();
        let back: MySqlSslMode = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{mode:?}"), format!("{back:?}"));
        // Copy/Clone stay usable.
        let copied = mode;
        assert_eq!(format!("{:?}", copied), json.trim_matches('"'));
    }
}

// ---------------------------------------------------------------------------
// Options builder
// ---------------------------------------------------------------------------

#[test]
fn test_options_defaults() {
    let options = MySqlConnectOptions::new();
    let dbg = format!("{options:?}");
    assert!(dbg.contains("host: \"localhost\""));
    assert!(dbg.contains("port: 3306"));
    assert!(dbg.contains("socket: None"));
    assert!(dbg.contains("username: \"root\""));
    assert!(dbg.contains("password: None"));
    assert!(dbg.contains("database: None"));
    assert!(dbg.contains("charset: \"utf8mb4\""));
    assert!(dbg.contains("collation: None"));
    assert!(dbg.contains("ssl_mode: Disabled"));
    assert!(dbg.contains("ssl_ca: None"));
    assert!(dbg.contains("statement_cache_capacity: 100"));

    // `Default` delegates to `new`.
    assert_eq!(format!("{:?}", MySqlConnectOptions::default()), dbg);
}

#[test]
fn test_options_builder_setters() {
    let options = MySqlConnectOptions::new()
        .host("db.example.com")
        .port(3307)
        .socket("/tmp/mysql.sock")
        .username("alice")
        .password("s3cret")
        .database("app")
        .ssl_mode(MySqlSslMode::VerifyIdentity)
        .ssl_ca("/etc/ssl/ca.pem")
        .statement_cache_capacity(7)
        .charset("latin1")
        .collation("latin1_bin")
        .offset_sec(3600);

    let dbg = format!("{options:?}");
    assert!(dbg.contains("host: \"db.example.com\""));
    assert!(dbg.contains("port: 3307"));
    assert!(dbg.contains("socket: Some(\"/tmp/mysql.sock\")"));
    assert!(dbg.contains("username: \"alice\""));
    assert!(dbg.contains("password: Some(\"s3cret\")"));
    assert!(dbg.contains("database: Some(\"app\")"));
    assert!(dbg.contains("ssl_mode: VerifyIdentity"));
    assert!(dbg.contains("File(\"/etc/ssl/ca.pem\")"));
    assert!(dbg.contains("statement_cache_capacity: 7"));
    assert!(dbg.contains("charset: \"latin1\""));
    assert!(dbg.contains("collation: Some(\"latin1_bin\")"));
    assert!(dbg.contains("offset_sec: 3600"));
}

#[test]
fn test_ssl_ca_from_pem() {
    let options = MySqlConnectOptions::new().ssl_ca_from_pem(vec![1, 2, 3]);
    let dbg = format!("{options:?}");
    assert!(dbg.contains("Inline([1, 2, 3])"));
}

// ---------------------------------------------------------------------------
// URI parsing
// ---------------------------------------------------------------------------

#[test]
fn test_parse_full_uri() {
    let uri = "mysql://alice:s3cr%40t@db.example.com:3307/app\
               ?ssl-mode=required&ssl-ca=/tmp/ca.pem&charset=latin1\
               &collation=latin1_bin&statement-cache-capacity=25\
               &socket=/tmp/mysql.sock&unknown=ignored";
    let options = MySqlConnectOptions::from_str(uri).unwrap();
    let dbg = format!("{options:?}");

    assert!(dbg.contains("host: \"db.example.com\""));
    assert!(dbg.contains("port: 3307"));
    assert!(dbg.contains("username: \"alice\""));
    // Percent-encoded characters in the password are decoded.
    assert!(dbg.contains("password: Some(\"s3cr@t\")"));
    assert!(dbg.contains("database: Some(\"app\")"));
    assert!(dbg.contains("ssl_mode: Required"));
    assert!(dbg.contains("File(\"/tmp/ca.pem\")"));
    assert!(dbg.contains("charset: \"latin1\""));
    assert!(dbg.contains("collation: Some(\"latin1_bin\")"));
    assert!(dbg.contains("statement_cache_capacity: 25"));
    assert!(dbg.contains("socket: Some(\"/tmp/mysql.sock\")"));
}

#[test]
fn test_parse_minimal_uri() {
    let options = MySqlConnectOptions::from_str("mysql://localhost").unwrap();
    let dbg = format!("{options:?}");
    assert!(dbg.contains("host: \"localhost\""));
    // Missing username keeps the default.
    assert!(dbg.contains("username: \"root\""));

    let options = MySqlConnectOptions::from_str("mysql:///db").unwrap();
    let dbg = format!("{options:?}");
    assert!(dbg.contains("host: \"localhost\""));
    assert!(dbg.contains("database: Some(\"db\")"));
}

#[test]
fn test_parse_percent_encoded_username() {
    let options = MySqlConnectOptions::from_str("mysql://user%40host@example.com/db").unwrap();
    assert!(format!("{options:?}").contains("username: \"user@host\""));
}

#[test]
fn test_parse_invalid_inputs() {
    // Not a URL at all.
    assert!(MySqlConnectOptions::from_str("not a url").is_err());
    // Unknown ssl mode.
    assert!(MySqlConnectOptions::from_str("mysql://h/db?ssl-mode=bogus").is_err());
    // Invalid statement cache capacity.
    assert!(
        MySqlConnectOptions::from_str("mysql://h/db?statement-cache-capacity=abc").is_err()
    );
    // Invalid UTF-8 percent escapes in the username.
    assert!(MySqlConnectOptions::from_str("mysql://%FF@h/db").is_err());
}

// ---------------------------------------------------------------------------
// ConnectOptions
// ---------------------------------------------------------------------------

#[test]
fn test_set_uri() {
    let mut options = MySqlConnectOptions::new();
    options.set_uri("mysql://bob:pw@example.org:1234/schema").unwrap();
    let dbg = format!("{options:?}");
    assert!(dbg.contains("host: \"example.org\""));
    assert!(dbg.contains("port: 1234"));
    assert!(dbg.contains("username: \"bob\""));
    assert!(dbg.contains("password: Some(\"pw\")"));
    assert!(dbg.contains("database: Some(\"schema\")"));

    assert!(options.set_uri("definitely not a url").is_err());
}

// ---------------------------------------------------------------------------
// Driver facade
// ---------------------------------------------------------------------------

#[test]
fn test_driver_name_and_exchange() {
    let driver = MysqlDriver {};
    assert_eq!(driver.name(), "mysql");
    assert_eq!(driver.exchange("SELECT ?"), "SELECT ?");
}

#[test]
fn test_driver_default_option() {
    let driver = MysqlDriver {};
    let option = driver.default_option();
    let parsed = option.downcast_ref::<MySqlConnectOptions>();
    assert!(parsed.is_some());
}

#[test]
fn test_driver_column_type() {
    let driver = MysqlDriver {};
    let cases = vec![
        (Value::Null, "NULL"),
        (Value::Bool(true), "TINYINT"),
        (Value::I32(1), "INT"),
        (Value::I64(1), "BIGINT"),
        (Value::U32(1), "INT"),
        (Value::U64(1), "BIGINT"),
        (Value::F32(1.0), "FLOAT"),
        (Value::F64(1.0), "DOUBLE"),
        (Value::String("x".into()), "TEXT"),
        (Value::Binary(vec![]), "BLOB"),
        (Value::Array(vec![]), "JSON"),
        (serde_json::from_str::<Value>("{}").unwrap(), "JSON"),
        (Value::Ext("Uuid", Box::new(Value::Null)), "TEXT"),
        (Value::Ext("Decimal", Box::new(Value::Null)), "DECIMAL"),
        (Value::Ext("Year", Box::new(Value::Null)), "YEAR"),
        (Value::Ext("Date", Box::new(Value::Null)), "DATE"),
        (Value::Ext("Time", Box::new(Value::Null)), "TIME"),
        (Value::Ext("Timestamp", Box::new(Value::Null)), "TIMESTAMP"),
        (Value::Ext("DateTime", Box::new(Value::Null)), "DATETIME"),
        (Value::Ext("Json", Box::new(Value::Null)), "JSON"),
        (Value::Ext("Enum", Box::new(Value::Null)), "TEXT"),
        (Value::Ext("Set", Box::new(Value::Null)), "TEXT"),
        (Value::Ext("Other", Box::new(Value::Null)), "NULL"),
    ];
    for (value, expected) in cases {
        assert_eq!(driver.column_type(&value), expected, "{value:?}");
    }
}

#[tokio::test]
async fn test_driver_connect_rejects_invalid_uri() {
    let driver = MysqlDriver {};
    // Parsing fails before any socket is opened.
    assert!(driver.connect("not a mysql uri").await.is_err());
}
