/// impl exchange - optimized single pass without pre-scanning
pub fn impl_exchange(start_str: &str, start_num: usize, sql: &str) -> String {
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
