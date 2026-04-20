use crate::Error;
use crate::db::Row;
use rbs::Value;
use std::fmt::{Debug, Formatter};

/// Iterator for scanning query results row by row.
/// More memory efficient than loading all rows into Value Array.
///
/// # Example
///
/// ```
/// use rbdc::db::Connection;
/// use rbdc::util::Scan;
/// use rbdc::Error;
///
/// # async fn example(conn: &mut dyn Connection) -> Result<(), Error> {
/// let rows = conn.exec_rows("SELECT * FROM activity", vec![]).await?;
/// let scan = Scan::new(rows);
///
/// // Collect all rows into a Vec of struct
/// #[derive(serde::Deserialize)]
/// struct Activity {
///     id: Option<String>,
///     name: Option<String>,
/// }
/// let activities: Vec<Activity> = scan.collect()?;
/// # Ok(())
/// # }
/// ```
pub struct Scan {
    rows: Vec<Box<dyn Row>>,
    current: usize,
}

impl Debug for Scan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scan")
            .field("remaining", &(self.rows.len() - self.current))
            .finish()
    }
}

impl Iterator for Scan {
    type Item = Result<Value, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.rows.len() {
            return None;
        }
        let row = &mut self.rows[self.current];
        let md = row.meta_data();
        let col_len = md.column_len();
        let mut values = Vec::with_capacity(col_len);
        for i in 0..col_len {
            values.push(row.get(i).unwrap_or(Value::Null));
        }
        self.current += 1;
        Some(Ok(Value::Array(values)))
    }
}

impl Scan {
    /// Create a new Scan from rows.
    pub fn new(rows: Vec<Box<dyn Row>>) -> Self {
        Self { rows, current: 0 }
    }

    /// Collect all rows into a Vec of type T.
    /// Each row is converted from Value::Array to T using rbs::from_value.
    pub fn collect<T: serde::de::DeserializeOwned>(mut self) -> Result<Vec<T>, Error> {
        let mut result = Vec::new();
        while let Some(item) = self.next() {
            let value = item?;
            let t: T = rbs::from_value(value)?;
            result.push(t);
        }
        Ok(result)
    }
}
