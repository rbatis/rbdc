#[derive(Debug, Default)]
pub struct MySqlQueryResult {
    pub(super) rows_affected: u64,
    pub(super) last_insert_id: u64,
}

impl MySqlQueryResult {
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<MySqlQueryResult> for MySqlQueryResult {
    fn extend<T: IntoIterator<Item = MySqlQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
            self.last_insert_id = elem.last_insert_id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_getters() {
        let r = MySqlQueryResult::default();
        assert_eq!(r.rows_affected(), 0);
        assert_eq!(r.last_insert_id(), 0);
    }

    #[test]
    fn test_extend_accumulates_rows_and_overwrites_last_id() {
        let mut r = MySqlQueryResult::default();
        r.extend([
            MySqlQueryResult {
                rows_affected: 2,
                last_insert_id: 10,
            },
            MySqlQueryResult {
                rows_affected: 3,
                last_insert_id: 12,
            },
        ]);
        assert_eq!(r.rows_affected(), 5);
        assert_eq!(r.last_insert_id(), 12);

        // Extending with an empty iterator leaves the totals untouched.
        r.extend(std::iter::empty());
        assert_eq!(r.rows_affected(), 5);
        assert_eq!(r.last_insert_id(), 12);
    }
}
