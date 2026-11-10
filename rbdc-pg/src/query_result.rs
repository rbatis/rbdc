#[derive(Debug, Default)]
pub struct PgQueryResult {
    pub(super) rows_affected: u64,
}

impl PgQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

impl Extend<PgQueryResult> for PgQueryResult {
    fn extend<T: IntoIterator<Item = PgQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.rows_affected += elem.rows_affected;
        }
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn rows_affected_and_extend() {
        let result = PgQueryResult::default();
        assert_eq!(result.rows_affected(), 0);

        let mut result = PgQueryResult { rows_affected: 2 };
        result.extend(vec![
            PgQueryResult { rows_affected: 3 },
            PgQueryResult { rows_affected: 4 },
        ]);
        assert_eq!(result.rows_affected(), 9);
    }
}
