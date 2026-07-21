/// value macro
///
/// value! map
///```rust
/// let v=  rbs::value! {"1":"1",};
///```
/// value! expr
///```rust
/// let arg="1";
/// let v =  rbs::value!(arg);
///```
/// 
/// JSON example:
/// ```ignore
/// let v = rbs::value! {
///     "id": 1, 
///     "user": {
///         "name": "Alice",
///         "address": {
///             "city": "Beijing",
///             "street": {
///                 "number": 123
///             }
///         }
///     }
/// };
/// ```
#[macro_export]
macro_rules! value {
    // Handle empty object case
    ({}) => {
        $crate::Value::Map($crate::value::map::ValueMap::new())
    };
    
    // Handle empty input
    () => {
        $crate::Value::Map($crate::value::map::ValueMap::new())
    };
    
    // Handle nested objects with brace syntax {"key": {nested object}}
    // This is a general rule for handling objects with nested objects
    // Note: This rewrites the handling method, focusing on the internal block {}
    {$($k:tt: $v:tt),* $(,)*} => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                $crate::value!(@map_entry map $k $v);
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Handle object form with parentheses: value!({k:v})
    ({$($k:tt: $v:tt),* $(,)*}) => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                $crate::value!(@map_entry map $k $v);
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Handle key-value pairs: value!(k:v)
    ($($k:tt: $v:expr),* $(,)?) => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                map.insert($crate::value!($k), $crate::value!($v));
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Internal helper rule: handle key-value pairs in a map
    (@map_entry $map:ident $k:tt {$($ik:tt: $iv:tt),* $(,)*}) => {
        // Process nested object
        let inner_map = $crate::value!({$($ik: $iv),*});
        $map.insert($crate::value!($k), inner_map);
    };
    
    // Handle regular key-value pairs
    (@map_entry $map:ident $k:tt $v:tt) => {
        $map.insert($crate::value!($k), $crate::value!($v));
    };
    
    // Handle single expression
    ($arg:expr) => {
        $crate::value($arg).unwrap_or_default()
    };
    
    // Array syntax: value![a, b, c]
    [$($v:expr),* $(,)*] => {
        {
            // Use value function directly to handle arrays, avoiding recursive expansion
            $crate::value(vec![$($crate::value($v).unwrap_or_default()),*]).unwrap_or_default()
        }
    };
}



#[deprecated(note = "please use value!")]
#[macro_export]
macro_rules! to_value {
    // Handle empty object case
    ({}) => {
        $crate::Value::Map($crate::value::map::ValueMap::new())
    };
    
    // Handle empty input
    () => {
        $crate::Value::Map($crate::value::map::ValueMap::new())
    };
    
    // Handle nested objects with brace syntax {"key": {nested object}}
    // This is a general rule for handling objects with nested objects
    // Note: This rewrites the handling method, focusing on the internal block {}
    {$($k:tt: $v:tt),* $(,)*} => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                $crate::value!(@map_entry map $k $v);
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Handle object form with parentheses: value!({k:v})
    ({$($k:tt: $v:tt),* $(,)*}) => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                $crate::value!(@map_entry map $k $v);
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Handle key-value pairs: value!(k:v)
    ($($k:tt: $v:expr),* $(,)?) => {
        {
            let mut map = $crate::value::map::ValueMap::new();
            $(
                map.insert($crate::value!($k), $crate::value!($v));
            )*
            $crate::Value::Map(map)
        }
    };
    
    // Internal helper rule: handle key-value pairs in a map
    (@map_entry $map:ident $k:tt {$($ik:tt: $iv:tt),* $(,)*}) => {
        // Process nested object
        let inner_map = $crate::value!({$($ik: $iv),*});
        $map.insert($crate::value!($k), inner_map);
    };
    
    // Handle regular key-value pairs
    (@map_entry $map:ident $k:tt $v:tt) => {
        $map.insert($crate::value!($k), $crate::value!($v));
    };
    
    // Handle single expression
    ($arg:expr) => {
        $crate::value($arg).unwrap_or_default()
    };
    
    // Array syntax: value![a, b, c]
    [$($v:expr),* $(,)*] => {
        {
            // Use value function directly to handle arrays, avoiding recursive expansion
            $crate::value(vec![$($crate::value($v).unwrap_or_default()),*]).unwrap_or_default()
        }
    };
}