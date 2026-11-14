use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueFormat, PgValueRef};
use rbdc::Error;
use rbs::Value;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// PostgreSQL POINT type for geometric points
///
/// Represents a point in 2D space (x, y).
/// This implementation uses WKT (Well-Known Text) format for text representation.
///
/// # Examples
///
/// ```ignore
/// // Create a point at (116.4, 39.9) - Beijing coordinates
/// let point = Point { x: 116.4, y: 39.9 };
///
/// // WKT format: "POINT(116.4 39.9)"
/// ```
///
/// For more advanced GIS operations, consider using PostGIS extension directly
/// or the `geo-types` crate for parsing WKT/WKB formats.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Display for Point {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "POINT({} {})", self.x, self.y)
    }
}

impl From<Point> for Value {
    fn from(arg: Point) -> Self {
        rbs::Value::Ext(
            "Point",
            Box::new(rbs::Value::Array(vec![
                rbs::Value::F64(arg.x),
                rbs::Value::F64(arg.y),
            ])),
        )
    }
}

impl FromStr for Point {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let coords = if let Some(rest) = s.strip_prefix("POINT(") {
            rest.strip_suffix(')')
        } else {
            s.strip_prefix('(').and_then(|rest| rest.strip_suffix(')'))
        }
        .ok_or_else(|| {
            Error::from(format!(
                "Invalid POINT format: {s}. Expected '(x,y)' or 'POINT(x y)'"
            ))
        })?;

        let parts: Vec<&str> = coords
            .split(|c: char| c == ',' || c.is_whitespace())
            .filter(|part| !part.is_empty())
            .collect();
        if parts.len() != 2 {
            return Err(Error::from(format!(
                "Invalid POINT coords: {coords}. Expected 2 values."
            )));
        }

        let x = parts[0]
            .parse::<f64>()
            .map_err(|e| Error::from(format!("Invalid x coordinate: {e}")))?;
        let y = parts[1]
            .parse::<f64>()
            .map_err(|e| Error::from(format!("Invalid y coordinate: {e}")))?;
        Ok(Self { x, y })
    }
}

impl Decode for Point {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                let bytes = value.as_bytes()?;
                if bytes.len() != 16 {
                    return Err(Error::from(format!(
                        "Invalid PostgreSQL POINT binary length: {} (expected 16)",
                        bytes.len()
                    )));
                }
                Self {
                    x: f64::from_bits(u64::from_be_bytes(bytes[0..8].try_into().unwrap())),
                    y: f64::from_bits(u64::from_be_bytes(bytes[8..16].try_into().unwrap())),
                }
            }
            PgValueFormat::Text => Self::from_str(value.as_str()?)?,
        })
    }
}

impl Encode for Point {
    fn encode(self, buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        buf.extend_from_slice(&self.x.to_bits().to_be_bytes());
        buf.extend_from_slice(&self.y.to_bits().to_be_bytes());
        Ok(IsNull::No)
    }
}
