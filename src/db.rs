use crate::Error;
use futures_core::future::BoxFuture;
use rbs::Value;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};

/// Represents database driver that can be shared between threads, and can therefore implement
/// a connection pool
pub trait Driver: Debug + Sync + Send {
    fn name(&self) -> &str;
    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>>;

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn Connection>, Error>>;

    /// make an default option
    fn default_option(&self) -> Box<dyn ConnectOptions>;
}

impl Driver for Box<dyn Driver> {
    fn name(&self) -> &str {
        self.deref().name()
    }

    fn connect(&self, url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
        self.deref().connect(url)
    }

    fn connect_opt<'a>(
        &'a self,
        opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn Connection>, Error>> {
        self.deref().connect_opt(opt)
    }

    fn default_option(&self) -> Box<dyn ConnectOptions> {
        self.deref().default_option()
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
pub struct ExecResult {
    pub rows_affected: u64,
    /// If some databases do not support last_insert_id, the default value is Null
    pub last_insert_id: Value,
}

impl Display for ExecResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        struct DisplayBox<'a> {
            inner: &'a Value,
        }
        impl<'a> Debug for DisplayBox<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.inner, f)
            }
        }
        f.debug_map()
            .key(&"rows_affected")
            .value(&self.rows_affected)
            .key(&"last_insert_id")
            .value(&DisplayBox {
                inner: &self.last_insert_id,
            })
            .finish()
    }
}

impl From<(u64, Value)> for ExecResult {
    fn from(value: (u64, Value)) -> Self {
        Self {
            rows_affected: value.0,
            last_insert_id: value.1,
        }
    }
}

/// Represents a connection to a database
pub trait Connection: Send + Sync {
    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<Vec<Box<dyn Row>>, Error>>;

    /// Execute a query that is expected to return a result set, such as a `SELECT` statement.
    /// you can use `let result:Vec<Table>=rbs::from_value(v)?;` to decode this result.
    /// return csv format Value [['column'],['value']].
    fn exec_decode(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<Value, Error>> {
        let v = self.exec_rows(sql, params);
        Box::pin(async move {
            let v = v.await?;
            let mut rows = Vec::with_capacity(v.len() + 1);
            for (row_idx, mut x) in v.into_iter().enumerate() {
                let md = x.meta_data();
                let mut row = Vec::with_capacity(md.column_len());
                for mut i in 0..md.column_len() {
                    i = md.column_len() - i - 1;
                    row.push(x.get(i).unwrap_or(Value::Null));
                }
                if row_idx == 0 {
                    let columns: Vec<Value> = (0..md.column_len())
                        .map(|mut i| {
                            i = md.column_len() - i - 1;
                            Value::String(md.column_name(i))
                        })
                        .collect();
                    rows.push(Value::Array(columns));
                }
                rows.push(Value::Array(row));
            }
            Ok(Value::Array(rows))
        })
    }

    /// Execute a query that is expected to update some rows.
    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<ExecResult, Error>>;

    /// ping
    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>>;

    /// close connection
    /// Normally conn is dropped when the link is dropped,
    /// but it is recommended to actively close this function so that the database does not report errors.
    /// If &mut self is not satisfied close, when you need mut self,
    /// It is recommended to use Option<DataBaseConnection>
    /// and then call take to take ownership and then if let Some(v) = self.inner.take() {v.lose ().await; }
    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>>;

    /// an translation impl begin
    fn begin(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async {
            _ = self.exec("begin", vec![]).await?;
            Ok(())
        })
    }

    /// an translation impl commit
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async {
            _ = self.exec("commit", vec![]).await?;
            Ok(())
        })
    }

    /// an translation impl rollback
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async {
            _ = self.exec("rollback", vec![]).await?;
            Ok(())
        })
    }
}

impl Connection for Box<dyn Connection> {
    fn exec_rows(
        &mut self,
        sql: &str,
        params: Vec<Value>,
    ) -> BoxFuture<'_, Result<Vec<Box<dyn Row>>, Error>> {
        self.deref_mut().exec_rows(sql, params)
    }

    fn exec_decode(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<Value, Error>> {
        self.deref_mut().exec_decode(sql, params)
    }

    fn exec(&mut self, sql: &str, params: Vec<Value>) -> BoxFuture<'_, Result<ExecResult, Error>> {
        self.deref_mut().exec(sql, params)
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.deref_mut().ping()
    }

    fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.deref_mut().close()
    }

    fn begin(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.deref_mut().begin()
    }
    fn rollback(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.deref_mut().rollback()
    }
    fn commit(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.deref_mut().commit()
    }
}

/// Result set from executing a query against a statement
pub trait Row: 'static + Send + Debug {
    /// get meta data about this result set
    fn meta_data(&self) -> Box<dyn MetaData>;

    /// get Value from index
    fn get(&mut self, i: usize) -> Result<Value, Error>;
}

/// Meta data for result set
pub trait MetaData: Debug {
    fn column_len(&self) -> usize;
    fn column_name(&self, i: usize) -> String;
    fn column_type(&self, i: usize) -> String;
}

/// connect option
pub trait ConnectOptions: Any + Send + Sync + Debug + 'static {
    /// Establish a new database connection with the options specified by `self`.
    fn connect(&self) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>>;

    ///set option
    ///
    /// for exmample:
    ///
    ///```rust
    /// use std::any::Any;
    /// pub struct SqliteConnectOptions{
    ///   pub immutable:bool,
    /// };
    ///  impl SqliteConnectOptions{
    ///             pub fn new()->Self{
    ///                 Self{
    ///                     immutable: false,
    ///                 }
    ///             }
    ///             fn set(&mut self, arg: Box<dyn Any>){
    ///             }
    ///         }
    ///
    /// let mut d = SqliteConnectOptions{immutable:false};
    ///         d.set(Box::new({
    ///             let mut new = SqliteConnectOptions::new();
    ///             new.immutable=true;
    ///             new
    ///         }));
    /// ```
    ///
    #[inline]
    fn set(&mut self, arg: Box<dyn Any>)
    where
        Self: Sized,
    {
        *self = *arg.downcast().expect(
            "ConnectOptions::set: type mismatch - expected the same type that implements ConnectOptions",
        );
    }

    ///set option from uri
    fn set_uri(&mut self, uri: &str) -> Result<(), Error>;
}

/// database driver ConnectOptions
impl dyn ConnectOptions {
    pub fn downcast_ref<E: ConnectOptions>(&self) -> Option<&E> {
        <dyn Any>::downcast_ref::<E>(self)
    }

    pub fn downcast_ref_mut<E: ConnectOptions>(&mut self) -> Option<&mut E> {
        <dyn Any>::downcast_mut::<E>(self)
    }
}

/// make all database drivers support dialect '?'
/// you can use util package to impl this
/// for example:
/// ```rust
/// use rbdc::db::Placeholder;
/// pub struct MyPgDriver{}
/// impl Placeholder for MyPgDriver{
///     fn exchange(&self, sql: &str) -> String {
///         rbdc::impl_exchange("$",1,sql)
///     }
/// }
/// ```
///
/// for example: postgres driver
/// ```log
///  "select * from  table where name = ?"
/// ```
/// to
/// ```log
/// "select * from  table where name =  $1"
pub trait Placeholder {
    fn exchange(&self, sql: &str) -> String;
}


use rbs::value::map::ValueMap;

/// 将 CSV 格式 Value 转换为 [{k:v}] 格式的 Map 数组
/// CSV 格式: [[col1,col2],[val1,val2],[val3,val4]]
/// 转换后: [{"col1": val1, "col2": val2}, {"col1": val3, "col2": val4}]
pub trait IntoMaps {
    fn into_maps(self) -> Result<Value, Error>;
}

impl IntoMaps for Value {
    fn into_maps(self) -> Result<Value, Error> {
        let Value::Array(rows) = self else {
            return Err(Error::from("into_maps: expected Array"));
        };
        if rows.is_empty() {
            return Ok(Value::Array(vec![]));
        }
        let Value::Array(columns) = &rows[0] else {
            return Err(Error::from("into_maps: first row must be column array"));
        };
        let data_rows = &rows[1..];
        let result: Vec<Value> = data_rows
            .iter()
            .map(|row| {
                let Value::Array(values) = row else {
                    return Ok::<Value, Error>(Value::Map(ValueMap::new()));
                };
                let mut map = ValueMap::with_capacity(columns.len());
                for (k, v) in columns.iter().zip(values.iter()) {
                    let key = match k {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    map.insert(key.into(), v.clone());
                }
                Ok(Value::Map(map))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Value::Array(result))
    }
}