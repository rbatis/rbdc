#![feature(test)]
extern crate test;

use test::Bencher;

// Old implementation (before optimization)
fn impl_exchange_old(start_str: &str, start_num: usize, sql: &str) -> String {
    let mut sql = sql.to_string();
    let mut sql_bytes = sql.as_bytes();
    let mut placeholder_idx = start_num;
    let mut index = 0;

    while index < sql_bytes.len() {
        let x = sql_bytes[index];

        if x == '\\' as u8 && index + 1 < sql_bytes.len() && sql_bytes[index + 1] == '?' as u8 {
            sql.remove(index);
            sql_bytes = sql.as_bytes();
            index += 1;
        } else if x == '?' as u8 {
            sql.remove(index);
            let mut i = 0;
            for c in start_str.chars() {
                sql.insert(index + i, c);
                i += 1;
            }
            sql.insert_str(
                index + start_str.len(),
                itoa::Buffer::new().format(placeholder_idx),
            );
            placeholder_idx += 1;
            sql_bytes = sql.as_bytes();
        } else {
            index += 1;
        }
    }
    sql
}

// New implementation (optimized)
fn impl_exchange_new(start_str: &str, start_num: usize, sql: &str) -> String {
    let mut result = String::with_capacity(sql.len() * 2);
    let mut placeholder_idx = start_num;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek() == Some(&'?') {
            chars.next();
            result.push('?');
        } else if c == '?' {
            result.push_str(start_str);
            result.push_str(itoa::Buffer::new().format(placeholder_idx));
            placeholder_idx += 1;
        } else {
            result.push(c);
        }
    }
    result
}

//test bench_exchange_old ... bench:         270.05 ns/iter (+/- 5.88)
#[bench]
fn bench_exchange_old(b: &mut Bencher) {
    let sql = "INSERT INTO users (id, name, email, age, city, country) VALUES (?, ?, ?, ?, ?, ?)";
    b.iter(|| impl_exchange_old("$", 1, sql));
}

//test bench_exchange_new ... bench:         208.89 ns/iter (+/- 4.95)
#[bench]
fn bench_exchange_new(b: &mut Bencher) {
    let sql = "INSERT INTO users (id, name, email, age, city, country) VALUES (?, ?, ?, ?, ?, ?)";
    b.iter(|| impl_exchange_new("$", 1, sql));
}
