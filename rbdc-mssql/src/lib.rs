#![allow(mismatched_lifetime_syntaxes)]

pub extern crate tiberius;

pub mod decode;
pub mod driver;
pub mod encode;

pub use crate::driver::MssqlDriver;
pub use crate::driver::MssqlDriver as Driver;

use crate::decode::Decode;
use crate::encode::Encode;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use rbdc::db::{ConnectOptions, Connection, ExecResult, MetaData, Placeholder, Row};
use rbdc::Error;
use rbs::Value;
use std::sync::Arc;
use tiberius::{AuthMethod, Client, Column, ColumnData, Config, EncryptionLevel, Query};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use url::Url;
use percent_encoding::percent_decode_str;

pub struct MssqlConnection {
    inner: Option<Client<Compat<TcpStream>>>,
}

impl MssqlConnection {
    /// let cfg = Config::from_jdbc_string(url).map_err(|e| Error::from(e.to_owned()))?;
    pub async fn establish(cfg: &Config) -> Result<Self, Error> {
        // let cfg = Config::from_jdbc_string(url).map_err(|e| Error::from(e.to_owned()))?;
        let tcp = TcpStream::connect(cfg.get_addr())
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        tcp.set_nodelay(true)?;
        let c = Client::connect(cfg.clone(), tcp.compat_write())
            .await
            .map_err(|e| Error::from(e.to_string()))?;
        Ok(Self { inner: Some(c) })
    }
}

#[derive(Debug)]
pub struct MssqlConnectOptions(pub Config);

impl ConnectOptions for MssqlConnectOptions {
    fn connect(&self) -> BoxFuture<Result<Box<dyn Connection>, Error>> {
        Box::pin(async move {
            let v = MssqlConnection::establish(&self.0)
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(Box::new(v) as Box<dyn Connection>)
        })
    }

    fn set_uri(&mut self, url: &str) -> Result<(), Error> {
        if url.contains("jdbc") {
            let mut config = Config::from_jdbc_string(url).map_err(|e| Error::from(e.to_string()))?;
            config.trust_cert();
            *self = MssqlConnectOptions(config);
        } else if url.starts_with("mssql://") || url.starts_with("sqlserver://") {
            let mut config = parse_url_connection_string(url)?;
            config.trust_cert();
            *self = MssqlConnectOptions(config);
        } else {
            let mut config = Config::from_ado_string(url).map_err(|e| Error::from(e.to_string()))?;
            config.trust_cert();
            *self = MssqlConnectOptions(config);
        }
        Ok(())
    }
}

/// Parse URL format connection string (mssql:// or sqlserver://)
/// Format: mssql://user:password@host:port/database?param1=value1&param2=value2
/// Or: sqlserver://user:password@host:port/database?param1=value1&param2=value2
///
/// Supported query parameters:
/// - instance: SQL Server instance name
/// - application_name: Application name
/// - encrypt: Encryption level (true/false/DANGER_PLAINTEXT)
/// - trust_cert: Whether to trust server certificate (true/false)
/// - readonly: Read-only mode (true/false)
fn parse_url_connection_string(url: &str) -> Result<Config, Error> {
    let parsed_url = Url::parse(url).map_err(|e| Error::from(e.to_string()))?;

    let mut config = Config::new();

    // Set host
    if let Some(host) = parsed_url.host_str() {
        config.host(host.to_string());
    }

    // Set port
    if let Some(port) = parsed_url.port() {
        config.port(port);
    }

    // Set username and password
    let username = parsed_url.username();
    if !username.is_empty() {
        let decoded_username = percent_decode_str(username)
            .decode_utf8()
            .map_err(|e| Error::from(e.to_string()))?;

        if let Some(password) = parsed_url.password() {
            let decoded_password = percent_decode_str(password)
                .decode_utf8()
                .map_err(|e| Error::from(e.to_string()))?;
            config.authentication(AuthMethod::sql_server(&decoded_username, &decoded_password));
        } else {
            config.authentication(AuthMethod::sql_server(&decoded_username, ""));
        }
    }

    // Set database
    let path = parsed_url.path().trim_start_matches('/');
    if !path.is_empty() {
        config.database(path);
    }

    // Parse query parameters
    for (key, value) in parsed_url.query_pairs() {
        match key.to_lowercase().as_str() {
            "instance" | "instance_name" => {
                config.instance_name(&*value);
            }
            "application_name" | "applicationname" => {
                config.application_name(&*value);
            }
            "encrypt" | "encryption" => {
                match value.to_lowercase().as_str() {
                    "true" | "yes" => {
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        config.encryption(EncryptionLevel::Required);
                    }
                    "false" | "no" => {
                        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                        config.encryption(EncryptionLevel::Off);
                    }
                    "danger_plaintext" => {
                        config.encryption(EncryptionLevel::NotSupported);
                    }
                    _ => {
                        return Err(Error::from(format!("Invalid encryption value: {}", value)));
                    }
                }
            }
            "trust_cert" | "trustservercertificate" => {
                match value.to_lowercase().as_str() {
                    "true" | "yes" => {
                        config.trust_cert();
                    }
                    "false" | "no" => {
                        // Default behavior, no special handling needed
                    }
                    _ => {
                        return Err(Error::from(format!("Invalid trust_cert value: {}", value)));
                    }
                }
            }
            "readonly" | "applicationintent" => {
                match value.to_lowercase().as_str() {
                    "true" | "yes" | "readonly" => {
                        config.readonly(true);
                    }
                    "false" | "no" | "readwrite" => {
                        config.readonly(false);
                    }
                    _ => {
                        return Err(Error::from(format!("Invalid readonly value: {}", value)));
                    }
                }
            }
            _ => {
                // Ignore unknown parameters
            }
        }
    }

    Ok(config)
}

#[derive(Debug)]
pub struct MssqlRow {
    pub columns: Arc<Vec<Column>>,
    pub datas: Vec<ColumnData<'static>>,
}

#[derive(Debug)]
pub struct MssqlMetaData(pub Arc<Vec<Column>>);

impl MetaData for MssqlMetaData {
    fn column_len(&self) -> usize {
        self.0.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.0[i].name().to_string()
    }

    fn column_type(&self, i: usize) -> String {
        format!("{:?}", self.0[i].column_type())
    }
}

impl Row for MssqlRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(MssqlMetaData(self.columns.clone()))
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        Value::decode(&self.datas[i])
    }
}

impl Connection for MssqlConnection {
    fn get_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<Result<Vec<Box<dyn Row>>, Error>> {
        let sql = MssqlDriver {}.exchange(sql);
        Box::pin(async move {
            let mut q = Query::new(sql);
            for x in params {
                x.encode(&mut q)?;
            }
            let v = q
                .query(
                    self.inner
                        .as_mut()
                        .ok_or_else(|| Error::from("MssqlConnection is close"))?,
                )
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            let mut results = Vec::with_capacity(v.size_hint().0);
            let s = v
                .into_results()
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            for item in s {
                for r in item {
                    let mut columns = Vec::with_capacity(r.columns().len());
                    let mut row = MssqlRow {
                        columns: Arc::new(vec![]),
                        datas: Vec::with_capacity(r.columns().len()),
                    };
                    for x in r.columns() {
                        columns.push(x.clone());
                    }
                    row.columns = Arc::new(columns);
                    for x in r {
                        row.datas.push(x);
                    }
                    results.push(Box::new(row) as Box<dyn Row>);
                }
            }
            Ok(results)
        })
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<Result<ExecResult, Error>> {
        let sql = MssqlDriver {}.exchange(sql);
        Box::pin(async move {
            let mut q = Query::new(sql);
            for x in params {
                x.encode(&mut q)?;
            }
            let v = q
                .execute(
                    self.inner
                        .as_mut()
                        .ok_or_else(|| Error::from("MssqlConnection is close"))?,
                )
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(ExecResult {
                rows_affected: {
                    let mut rows_affected = 0;
                    for x in v.rows_affected() {
                        rows_affected += x.clone();
                    }
                    rows_affected
                },
                last_insert_id: Value::Null,
            })
        })
    }

    fn close(&mut self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async move {
            //inner must be Option,so we can take owner and call close(self) method.
            if let Some(v) = self.inner.take() {
                v.close().await.map_err(|e| Error::from(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<Result<(), rbdc::Error>> {
        //TODO While 'select 1' can temporarily solve the problem of checking that the connection is valid, it looks ugly.Better replace it with something better way
        Box::pin(async move {
            self.inner
                .as_mut()
                .ok_or_else(|| Error::from("MssqlConnection is close"))?
                .query("select 1", &[])
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        })
    }

    fn begin(&mut self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async move {
            self.inner
                .as_mut()
                .ok_or_else(|| Error::from("MssqlConnection is close"))?
                .simple_query("begin tran")
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        })
    }

    fn commit(&mut self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async move {
            self.inner
                .as_mut()
                .ok_or_else(|| Error::from("MssqlConnection is close"))?
                .simple_query("commit")
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        })
    }

    fn rollback(&mut self) -> BoxFuture<Result<(), Error>> {
        Box::pin(async move {
            self.inner
                .as_mut()
                .ok_or_else(|| Error::from("MssqlConnection is close"))?
                .simple_query("rollback")
                .await
                .map_err(|e| Error::from(e.to_string()))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{MssqlConnectOptions, parse_url_connection_string};
    use rbdc::db::{ConnectOptions};
    use tiberius::Config;

    #[test]
    fn test_datetime() {}

    #[test]
    fn test_connection_string_parsing() {
        // 测试 JDBC 格式
        let jdbc_uri = "jdbc:sqlserver://localhost:1433;User=SA;Password={TestPass!123456};Database=master;";
        let mut options = MssqlConnectOptions(Config::new());
        let result = options.set_uri(jdbc_uri);
        assert!(result.is_ok(), "JDBC format should be supported");

        // 测试 mssql:// 格式
        let mssql_uri = "mssql://SA:TestPass!123456@localhost:1433/master";
        let mut options = MssqlConnectOptions(Config::new());
        let result = options.set_uri(mssql_uri);
        assert!(result.is_ok(), "mssql:// format should be supported: {:?}", result);

        // 测试 sqlserver:// 格式
        let sqlserver_uri = "sqlserver://SA:TestPass!123456@localhost:1433/master";
        let mut options = MssqlConnectOptions(Config::new());
        let result = options.set_uri(sqlserver_uri);
        assert!(result.is_ok(), "sqlserver:// format should be supported: {:?}", result);

        // 测试 ADO 格式
        let ado_uri = "Server=localhost,1433;User Id=SA;Password=TestPass!123456;Database=master;";
        let mut options = MssqlConnectOptions(Config::new());
        let result = options.set_uri(ado_uri);
        assert!(result.is_ok(), "ADO format should be supported");
    }

    #[test]
    fn test_url_parsing_details() {
        // 测试详细的 URL 解析
        let config = parse_url_connection_string("mssql://testuser:testpass@example.com:1433/testdb").unwrap();
        assert_eq!(config.get_addr(), "example.com:1433");

        // 测试没有密码的情况
        let config = parse_url_connection_string("mssql://testuser@localhost:1433/testdb").unwrap();
        assert_eq!(config.get_addr(), "localhost:1433");

        // 测试没有数据库的情况
        let config = parse_url_connection_string("mssql://testuser:testpass@localhost:1433").unwrap();
        assert_eq!(config.get_addr(), "localhost:1433");

        // 测试默认端口
        let config = parse_url_connection_string("mssql://testuser:testpass@localhost/testdb").unwrap();
        assert_eq!(config.get_addr(), "localhost:1433");
    }

    #[test]
    fn test_url_query_parameters() {
        // 测试带查询参数的 URL
        let config = parse_url_connection_string(
            "mssql://testuser:testpass@localhost:1433/testdb?instance=SQLEXPRESS&application_name=MyApp&encrypt=true&trust_cert=true&readonly=true"
        ).unwrap();
        assert_eq!(config.get_addr(), "localhost:1433");

        // 测试部分查询参数
        let config = parse_url_connection_string(
            "sqlserver://user:pass@server:1433/db?application_name=TestApp&encrypt=false"
        ).unwrap();
        assert_eq!(config.get_addr(), "server:1433");

        // 测试无效的加密值应该返回错误
        let result = parse_url_connection_string(
            "mssql://user:pass@localhost/db?encrypt=invalid"
        );
        assert!(result.is_err());

        // 测试无效的 trust_cert 值应该返回错误
        let result = parse_url_connection_string(
            "mssql://user:pass@localhost/db?trust_cert=invalid"
        );
        assert!(result.is_err());
    }
}
