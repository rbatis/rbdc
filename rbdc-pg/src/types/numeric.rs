use crate::arguments::PgArgumentBuffer;
use bytes::Buf;
use rbdc::Error;

/// Represents a `NUMERIC` value in the **Postgres** wire protocol.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PgNumeric {
    /// Equivalent to the `'NaN'` value in Postgres. The result of, e.g. `1 / 0`.
    NotANumber,

    /// A populated `NUMERIC` value.
    ///
    /// A description of these fields can be found here (although the type being described is the
    /// version for in-memory calculations, the field names are the same):
    /// https://github.com/postgres/postgres/blob/bcd1c3630095e48bc3b1eb0fc8e8c8a7c851eba1/src/backend/utils/adt/numeric.c#L224-L269
    Number {
        /// The sign of the value: positive (also set for 0 and -0), or negative.
        sign: PgNumericSign,

        /// The digits of the number in base-10000 with the most significant digit first
        /// (big-endian).
        ///
        /// The length of this vector must not overflow `i16` for the binary protocol.
        ///
        /// *Note*: the `Encode` implementation will panic if any digit is `>= 10000`.
        digits: Vec<i16>,

        /// The scaling factor of the number, such that the value will be interpreted as
        ///
        /// ```text
        ///   digits[0] * 10,000 ^ weight
        /// + digits[1] * 10,000 ^ (weight - 1)
        /// ...
        /// + digits[N] * 10,000 ^ (weight - N) where N = digits.len() - 1
        /// ```
        /// May be negative.
        weight: i16,

        /// How many _decimal_ (base-10) digits following the decimal point to consider in
        /// arithmetic regardless of how many actually follow the decimal point as determined by
        /// `weight`--the comment in the Postgres code linked above recommends using this only for
        /// ignoring unnecessary trailing zeroes (as trimming nonzero digits means reducing the
        /// precision of the value).
        ///
        /// Must be `>= 0`.
        scale: i16,
    },
}

// https://github.com/postgres/postgres/blob/bcd1c3630095e48bc3b1eb0fc8e8c8a7c851eba1/src/backend/utils/adt/numeric.c#L167-L170
const SIGN_POS: u16 = 0x0000;
const SIGN_NEG: u16 = 0x4000;
const SIGN_NAN: u16 = 0xC000; // overflows i16 (C equivalent truncates from integer literal)

/// Possible sign values for [PgNumeric].
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum PgNumericSign {
    Positive = SIGN_POS,
    Negative = SIGN_NEG,
}

impl PgNumericSign {
    fn try_from_u16(val: u16) -> Result<Self, Error> {
        match val {
            SIGN_POS => Ok(PgNumericSign::Positive),
            SIGN_NEG => Ok(PgNumericSign::Negative),

            SIGN_NAN => unreachable!("sign value for NaN passed to PgNumericSign"),

            _ => Err(format!("invalid value for PgNumericSign: {:#04X}", val).into()),
        }
    }
}

impl PgNumeric {
    pub(crate) fn decode(mut buf: &[u8]) -> Result<Self, Error> {
        // https://github.com/postgres/postgres/blob/bcd1c3630095e48bc3b1eb0fc8e8c8a7c851eba1/src/backend/utils/adt/numeric.c#L874
        let num_digits = buf.get_u16();
        let weight = buf.get_i16();
        let sign = buf.get_u16();
        let scale = buf.get_i16();

        if sign == SIGN_NAN {
            Ok(PgNumeric::NotANumber)
        } else {
            let digits: Vec<_> = (0..num_digits).map(|_| buf.get_i16()).collect::<_>();

            Ok(PgNumeric::Number {
                sign: PgNumericSign::try_from_u16(sign)?,
                scale,
                weight,
                digits,
            })
        }
    }

    /// ### Panics
    ///
    /// * If `digits.len()` overflows `i16`
    /// * If any element in `digits` is greater than or equal to 10000
    pub(crate) fn encode(&self, buf: &mut PgArgumentBuffer) {
        match *self {
            PgNumeric::Number {
                ref digits,
                sign,
                scale,
                weight,
            } => {
                let digits_len: i16 = digits
                    .len()
                    .try_into()
                    .expect("PgNumeric.digits.len() should not overflow i16");

                buf.extend(&digits_len.to_be_bytes());
                buf.extend(&weight.to_be_bytes());
                buf.extend(&(sign as i16).to_be_bytes());
                buf.extend(&scale.to_be_bytes());

                for digit in digits {
                    debug_assert!(*digit < 10000, "PgNumeric digits must be in base-10000");

                    buf.extend(&digit.to_be_bytes());
                }
            }

            PgNumeric::NotANumber => {
                buf.extend(&0_i16.to_be_bytes());
                buf.extend(&0_i16.to_be_bytes());
                buf.extend(&SIGN_NAN.to_be_bytes());
                buf.extend(&0_i16.to_be_bytes());
            }
        }
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use num_bigint::BigInt;

    #[test]
    fn decode_number_nan_and_invalid_sign() {
        let mut payload = Vec::new();
        payload.extend(2u16.to_be_bytes());
        payload.extend(1i16.to_be_bytes());
        payload.extend(SIGN_NEG.to_be_bytes());
        payload.extend(3i16.to_be_bytes());
        payload.extend(1i16.to_be_bytes());
        payload.extend(2345i16.to_be_bytes());
        let decoded = PgNumeric::decode(&payload).unwrap();
        assert_eq!(
            decoded,
            PgNumeric::Number {
                sign: PgNumericSign::Negative,
                weight: 1,
                scale: 3,
                digits: vec![1, 2345],
            }
        );

        let mut nan = Vec::new();
        nan.extend(0u16.to_be_bytes());
        nan.extend(0i16.to_be_bytes());
        nan.extend(SIGN_NAN.to_be_bytes());
        nan.extend(0i16.to_be_bytes());
        assert_eq!(PgNumeric::decode(&nan).unwrap(), PgNumeric::NotANumber);

        let mut bad = Vec::new();
        bad.extend(0u16.to_be_bytes());
        bad.extend(0i16.to_be_bytes());
        bad.extend(0x1234u16.to_be_bytes());
        bad.extend(0i16.to_be_bytes());
        assert!(PgNumeric::decode(&bad).is_err());
    }

    #[test]
    fn encode_number_and_nan() {
        let number = PgNumeric::Number {
            sign: PgNumericSign::Positive,
            weight: 1,
            scale: 2,
            digits: vec![1, 2],
        };
        let mut buf = PgArgumentBuffer::default();
        number.encode(&mut buf);
        assert_eq!(
            buf.to_vec(),
            vec![
                0, 2, // digits
                0, 1, // weight
                0, 0, // sign positive
                0, 2, // scale
                0, 1, // digit 1
                0, 2, // digit 2
            ]
        );

        let mut buf = PgArgumentBuffer::default();
        PgNumeric::NotANumber.encode(&mut buf);
        assert_eq!(
            buf.to_vec(),
            vec![0, 0, 0, 0, 0xc0, 0, 0, 0]
        );
    }

    #[test]
    fn sign_conversion() {
        assert_eq!(PgNumericSign::try_from_u16(SIGN_POS).unwrap(), PgNumericSign::Positive);
        assert_eq!(PgNumericSign::try_from_u16(SIGN_NEG).unwrap(), PgNumericSign::Negative);
        assert!(PgNumericSign::try_from_u16(0x1234).is_err());
    }

    #[test]
    #[should_panic(expected = "sign value for NaN")]
    fn sign_nan_is_unreachable() {
        let _ = PgNumericSign::try_from_u16(SIGN_NAN);
    }

    #[test]
    fn numeric_to_bigdecimal() {
        // NaN is rejected
        assert!(BigDecimal::try_from(PgNumeric::NotANumber).is_err());

        // empty digit array means zero
        let zero = BigDecimal::try_from(PgNumeric::Number {
            sign: PgNumericSign::Positive,
            digits: vec![],
            weight: 0,
            scale: 0,
        })
        .unwrap();
        assert_eq!(zero, BigDecimal::from(0u64));

        // regular value, positive and negative
        let positive = BigDecimal::try_from(PgNumeric::Number {
            sign: PgNumericSign::Positive,
            digits: vec![1, 2345],
            weight: 1,
            scale: 4,
        })
        .unwrap();
        assert_eq!(positive.to_string(), "12345");

        let negative = BigDecimal::try_from(PgNumeric::Number {
            sign: PgNumericSign::Negative,
            digits: vec![1234],
            weight: 0,
            scale: 0,
        })
        .unwrap();
        assert_eq!(negative.to_string(), "-1234");
    }

    #[test]
    fn bigdecimal_to_numeric_edge_cases() {
        // offset larger than the digit buffer
        let value = BigDecimal::new(BigInt::from(1), -1);
        let numeric = PgNumeric::try_from(&value).unwrap();
        assert_eq!(
            numeric,
            PgNumeric::Number {
                sign: PgNumericSign::Positive,
                digits: vec![10],
                weight: 0,
                scale: 0,
            }
        );

        // huge scale overflows i16
        let value = BigDecimal::new(BigInt::from(1), 40_000);
        assert!(PgNumeric::try_from(&value).is_err());

        // huge negative exponent overflows the weight
        let value = BigDecimal::new(BigInt::from(1), -200_000);
        assert!(PgNumeric::try_from(&value).is_err());

        // trailing zero base-10000 digits are trimmed
        let value: BigDecimal = "100000000".parse().unwrap();
        let numeric = PgNumeric::try_from(&value).unwrap();
        match numeric {
            PgNumeric::Number { digits, .. } => assert_ne!(digits.last(), Some(&0)),
            _ => panic!("expected number"),
        }
    }
}
