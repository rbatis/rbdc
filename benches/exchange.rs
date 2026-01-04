#![feature(test)]
extern crate test;

use test::Bencher;
use rbdc::util::impl_exchange;

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

#[bench]
fn bench_exchange_old(b: &mut Bencher) {
    let sql = "INSERT INTO users (id, name, email, age, city, country) VALUES (?, ?, ?, ?, ?, ?)";
    b.iter(|| impl_exchange_old("$", 1, sql));
}

#[bench]
fn bench_exchange_new(b: &mut Bencher) {
    let sql = "INSERT INTO users (id, name, email, age, city, country) VALUES (?, ?, ?, ?, ?, ?)";
    b.iter(|| impl_exchange("$", 1, sql));
}
