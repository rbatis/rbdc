use futures_util::stream;
use rbdc::db::{MetaData, Row};
use rbdc::util::Scan;
use rbdc::Error;
use rbs::Value;
use std::fmt::Debug;

struct MockMetaData {
    columns: Vec<String>,
}

impl MockMetaData {
    fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }
}

impl Debug for MockMetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockMetaData").finish()
    }
}

impl MetaData for MockMetaData {
    fn column_len(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, i: usize) -> String {
        self.columns.get(i).cloned().unwrap_or_default()
    }

    fn column_type(&self, _i: usize) -> String {
        String::new()
    }
}

struct MockRow {
    columns: Vec<String>,
    values: Vec<Value>,
}

impl MockRow {
    fn new(columns: Vec<String>, values: Vec<Value>) -> Self {
        Self { columns, values }
    }
}

impl Debug for MockRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockRow").finish()
    }
}

impl Row for MockRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(MockMetaData::new(self.columns.clone()))
    }

    fn get(&mut self, i: usize) -> Result<Value, Error> {
        self.values.get(i).cloned().ok_or_else(|| Error::from("index out of bounds"))
    }
}

#[tokio::test]
async fn test_scan_stream_collect() {
    let rows: Vec<Box<dyn Row>> = vec![
        Box::new(MockRow::new(
            vec!["id".to_string(), "name".to_string()],
            vec![Value::I64(1), Value::String("a".into())],
        )),
        Box::new(MockRow::new(
            vec!["id".to_string(), "name".to_string()],
            vec![Value::I64(2), Value::String("b".into())],
        )),
    ];
    let stream = stream::iter(rows.into_iter().map(Ok::<Box<dyn Row>, Error>));
    let scan = Scan::new(stream);

    let values: Vec<Value> = scan.collect::<Value>().await.unwrap();
    assert_eq!(values.len(), 2);
    match &values[0] {
        Value::Map(_map) => {
            // Map contains key-value pairs
            assert!(true);
        }
        _ => panic!("expected Map"),
    }
}

#[tokio::test]
async fn test_scan_empty() {
    let rows: Vec<Box<dyn Row>> = vec![];
    let stream = stream::iter(rows.into_iter().map(Ok::<Box<dyn Row>, Error>));
    let scan = Scan::new(stream);

    let values: Vec<Value> = scan.collect::<Value>().await.unwrap();
    assert!(values.is_empty());
}

#[test]
fn test_scan_debug() {
    let rows: Vec<Box<dyn Row>> = vec![Box::new(MockRow::new(
        vec!["id".to_string()],
        vec![Value::I64(1)],
    ))];
    let stream = stream::iter(rows.into_iter().map(Ok::<Box<dyn Row>, Error>));
    let scan = Scan::new(stream);
    let debug_str = format!("{:?}", scan);
    assert!(debug_str.contains("Scan"));
}
