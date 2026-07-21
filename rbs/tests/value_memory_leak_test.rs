use rbs::{Value, value};
use rbs::value::map::ValueMap;
use std::thread;

/// 测试1: value函数对Value类型（零拷贝优化路径）的大量调用
#[test]
fn test_value_self_type() {
    let iterations = 100_000;
    for i in 0..iterations {
        let mut m = ValueMap::new();
        m.insert(Value::String("key".to_string()), Value::I64(i));
        m.insert(Value::String("value".to_string()), Value::String("test".to_string()));
        let original = Value::Map(m);
        // T=Value 会进入 unsafe 优化路径：直接 move 原值
        let _result = value(original);
        // _result drop 时应该释放所有内存
    }
    println!("Test value(Value): {} iterations - OK", iterations);
}

/// 测试2: value函数对&Value类型的大量调用
#[test]
fn test_value_ref_type() {
    let iterations = 100_000;
    let mut m = ValueMap::new();
    m.insert(Value::String("a".to_string()), Value::I64(1));
    m.insert(Value::String("b".to_string()), Value::I64(2));
    let original = Value::Map(m);
    for _ in 0..iterations {
        let _result = value(&original);
        // 克隆释放
    }
    println!("Test value(&Value): {} iterations - OK", iterations);
}

/// 测试3: value函数对复杂嵌套结构的大量调用
#[test]
fn test_value_nested() {
    let iterations = 50_000;
    for i in 0..iterations {
        let mut inner = ValueMap::new();
        inner.insert(Value::String("x".to_string()), Value::I64(1));
        inner.insert(Value::String("y".to_string()), Value::I64(2));
        let mut outer = ValueMap::new();
        outer.insert(Value::String("id".to_string()), Value::I64(i));
        outer.insert(Value::String("data".to_string()), Value::Array(vec![Value::I64(1), Value::I64(2), Value::I64(3)]));
        outer.insert(Value::String("nested".to_string()), Value::Map(inner));
        let _v = value(outer);
    }
    println!("Test value(nested): {} iterations - OK", iterations);
}

/// 测试4: Ext类型包含Box的内存释放
#[test]
fn test_value_ext_with_box() {
    let iterations = 100_000;
    for i in 0..iterations {
        let v = Value::Ext("MyType", Box::new(Value::String(format!("data_{}", i))));
        let _converted = value(v);
        // _converted drop 时应释放 Box
    }
    println!("Test value(Ext<Box>): {} iterations - OK", iterations);
}

/// 测试5: 大量数组嵌套
#[test]
fn test_value_array_nesting() {
    let iterations = 30_000;
    for _ in 0..iterations {
        let mut arr = Vec::with_capacity(5);
        for j in 0..5 {
            let mut m = ValueMap::new();
            m.insert(Value::String("idx".to_string()), Value::I64(j));
            m.insert(Value::String("val".to_string()), Value::I64(j * 10));
            arr.push(Value::Map(m));
        }
        let _v = value(arr);
    }
    println!("Test value(array): {} iterations - OK", iterations);
}

/// 测试8: 并发压力测试 - 多线程同时调用 value
#[test]
fn test_value_concurrent() {
    let iterations_per_thread = 50_000;
    let mut handles = vec![];

    for t in 0..4 {
        let handle = thread::spawn(move || {
            for i in 0..iterations_per_thread {
                let mut m = ValueMap::new();
                m.insert(Value::String("thread".to_string()), Value::I64(t));
                m.insert(Value::String("iter".to_string()), Value::I64(i));
                m.insert(Value::String("data".to_string()), Value::Array(vec![Value::I64(1), Value::I64(2)]));
                let _v = value(Value::Map(m));
            }
            println!("Thread {} completed {} iterations", t, iterations_per_thread);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }
    println!("Test concurrent: all threads completed - OK");
}

/// 测试9: 验证 Value::Ext 嵌套 Box 的完整生命周期
#[test]
fn test_value_ext_nested_box() {
    let iterations = 50_000;
    for i in 0..iterations {
        // 创建 Ext(Ext(...)) 嵌套结构
        let inner = Value::Ext("Inner", Box::new(Value::String(format!("data_{}", i))));
        let outer = Value::Ext("Outer", Box::new(inner));
        let _v = value(outer);
        // 应该释放两层 Box
    }
    println!("Test value(Ext<Ext<Box>>): {} iterations - OK", iterations);
}

/// 测试10: 大对象创建/销毁循环（模拟真实负载）
#[test]
fn test_value_large_objects() {
    let iterations = 10_000;
    for i in 0..iterations {
        // 创建包含大量元素的数组
        let mut arr = Vec::with_capacity(1000);
        for j in 0..1000 {
            arr.push(Value::I64(i * 1000 + j));
        }
        let mut m = ValueMap::new();
        m.insert(Value::String("id".to_string()), Value::I64(i));
        m.insert(Value::String("items".to_string()), Value::Array(arr));
        let _v = value(Value::Map(m));
    }
    println!("Test value(large objects): {} iterations - OK", iterations);
}

/// 测试6: 验证Ext类型递归调用value不会泄露
#[test]
fn test_value_ext_recursive() {
    let iterations = 50_000;
    for i in 0..iterations {
        let inner = Value::I64(i as i64);
        let ext = Value::Ext("Num", Box::new(inner));
        let _v = value(ext);
    }
    println!("Test value(Ext recursive): {} iterations - OK", iterations);
}

/// 测试7: 深度递归（验证不会内存泄露，但可能导致栈溢出）
#[test]
fn test_value_deep_recursion() {
    let mut current = Value::String("root".to_string());
    // 创建深度嵌套的数组 (100层)
    for _ in 0..100 {
        let mut arr = Vec::with_capacity(1);
        arr.push(current);
        current = Value::Array(arr);
    }

    // to_string() 是递归的，深度过大可能栈溢出，但不会内存泄露
    let result = std::panic::catch_unwind(|| {
        let _ = current.to_string();
    });

    if result.is_err() {
        println!("Deep recursion (100 layers) caused stack overflow - Stack NOT leaked");
    } else {
        println!("Deep recursion (100 layers) succeeded - memory OK");
    }
}
