# rbs

* rbs is rbatis's impl serde serialize trait crates.
* The rbs serialization framework is used to serialize parameters and deserialize sql result sets, and provides the value structure as py_ Sql and html_ The intermediate object representation of the expression in sql.

## use example
```rust
use std::collections::HashMap;
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct A {
    pub name: String,
}

fn main(){
    let a = A {
        name: "sss".to_string(),
    };
    let value = rbs::value(a).unwrap();
    println!("value: {}",value);
    let a: A = rbs::from_value(value).unwrap();
    println!("a:{:?}", a);
    
    //macro
    let val = value! {
            "name": "Alice",
            "age": 30,
            "city": "New York"
        };
    println!("val: {}",val);
}
```