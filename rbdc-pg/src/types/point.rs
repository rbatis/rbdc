use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValueFormat, PgValueRef};
use rbdc::Error;
use rbs::Value;
use std::fmt::{Display, Formatter};

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
            "point",
            Box::new(rbs::Value::Ext(
                "point",
                Box::new(rbs::Value::Array(vec![
                    rbs::Value::F64(arg.x),
                    rbs::Value::F64(arg.y),
                ])),
            )),
        )
    }
}

impl Decode for Point {
    fn decode(value: PgValueRef) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // Binary format is WKB (Well-Known Binary)
                // For simplicity, we don't support direct binary parsing
                // Applications should use geo-types crate for proper WKB parsing
                // Or use TEXT format in PostgreSQL: ST_AsText(point_column)
                return Err(Error::from(
                    "POINT binary format (WKB) not supported. \
                     Use TEXT format: ST_AsText(point_column) or parse with geo-types crate.",
                ));
            }
            PgValueFormat::Text => {
                // Text format is WKT (Well-Known Text): "POINT(x y)"
                let s = value.as_str()?;

                // Parse WKT format: "POINT(x y)"
                let s = s.trim();
                if !s.starts_with("POINT(") || !s.ends_with(')') {
                    return Err(Error::from(format!(
                        "Invalid POINT format: {}. Expected 'POINT(x y)'",
                        s
                    )));
                }

                let coords = &s[6..s.len() - 1]; // Remove "POINT(" and ")"
                let parts: Vec<&str> = coords.split_whitespace().collect();

                if parts.len() != 2 {
                    return Err(Error::from(format!(
                        "Invalid POINT coords: {}. Expected 2 values.",
                        coords
                    )));
                }

                let x = parts[0]
                    .parse::<f64>()
                    .map_err(|e| Error::from(format!("Invalid x coordinate: {}", e)))?;
                let y = parts[1]
                    .parse::<f64>()
                    .map_err(|e| Error::from(format!("Invalid y coordinate: {}", e)))?;

                Self { x, y }
            }
        })
    }
}

impl Encode for Point {
    fn encode(self, _buf: &mut crate::arguments::PgArgumentBuffer) -> Result<IsNull, Error> {
        // For PostGIS POINT, use TEXT format in your query:
        // ST_GeomFromText('POINT(116.4 39.9)')
        Err(Error::from(
            "POINT encoding not supported. Use PostGIS ST_GeomFromText() or ST_MakePoint() in your query instead."
        ))
    }
}
