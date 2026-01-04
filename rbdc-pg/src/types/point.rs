use crate::types::decode::Decode;
use crate::types::encode::{Encode, IsNull};
use crate::value::{PgValue, PgValueFormat};
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
        rbs::Value::Ext("point", Box::new(rbs::Value::Ext(
            "point",
            Box::new(rbs::Value::Array(vec![
                rbs::Value::F64(arg.x),
                rbs::Value::F64(arg.y),
            ]))
        )))
    }
}

impl Decode for Point {
    fn decode(value: PgValue) -> Result<Self, Error> {
        Ok(match value.format() {
            PgValueFormat::Binary => {
                // Binary format is WKB (Well-Known Binary)
                // For simplicity, we don't support direct binary parsing
                // Applications should use geo-types crate for proper WKB parsing
                // Or use TEXT format in PostgreSQL: ST_AsText(point_column)
                return Err(Error::from(
                    "POINT binary format (WKB) not supported. \
                     Use TEXT format: ST_AsText(point_column) or parse with geo-types crate."
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

                let coords = &s[6..s.len()-1]; // Remove "POINT(" and ")"
                let parts: Vec<&str> = coords.split_whitespace().collect();

                if parts.len() != 2 {
                    return Err(Error::from(format!(
                        "Invalid POINT coords: {}. Expected 2 values.",
                        coords
                    )));
                }

                let x = parts[0].parse::<f64>()
                    .map_err(|e| Error::from(format!("Invalid x coordinate: {}", e)))?;
                let y = parts[1].parse::<f64>()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::decode::Decode;
    use crate::value::{PgValue, PgValueFormat};

    #[test]
    fn test_display() {
        let point = Point { x: 116.4, y: 39.9 };
        assert_eq!(format!("{}", point), "POINT(116.4 39.9)");
    }

    #[test]
    fn test_beijing_coords() {
        let point = Point { x: 116.4074, y: 39.9042 };
        let display = format!("{}", point);
        println!("Beijing: {}", display);
        assert!(display.contains("POINT("));
    }

    #[test]
    fn test_decode_text_valid() {
        let s = "POINT(116.4 39.9)";
        let result: Point = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(result.x, 116.4);
        assert_eq!(result.y, 39.9);
    }

    #[test]
    fn test_decode_text_negative_coords() {
        let s = "POINT(-74.0060 40.7128)";
        let result: Point = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        }).unwrap();
        assert_eq!(result.x, -74.0060);
        assert_eq!(result.y, 40.7128);
    }

    #[test]
    fn test_decode_text_invalid_format() {
        let s = "INVALID(116.4 39.9)";
        let result: Result<Point, _> = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_text_invalid_coords_count() {
        let s = "POINT(116.4)";
        let result: Result<Point, _> = Decode::decode(PgValue {
            value: Some(s.as_bytes().to_vec()),
            type_info: crate::type_info::PgTypeInfo::UNKNOWN,
            format: PgValueFormat::Text,
            timezone_sec: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_from_value() {
        let point = Point { x: 1.0, y: 2.0 };
        let value: rbs::Value = point.into();
        match value {
            rbs::Value::Ext(type_name, boxed) => {
                assert_eq!(type_name, "point");
                if let rbs::Value::Ext(inner_type, inner_boxed) = *boxed {
                    assert_eq!(inner_type, "point");
                    if let rbs::Value::Array(arr) = *inner_boxed {
                        assert_eq!(arr.len(), 2);
                        if let rbs::Value::F64(x) = &arr[0] {
                            assert_eq!(*x, 1.0);
                        } else {
                            panic!("Expected F64");
                        }
                        if let rbs::Value::F64(y) = &arr[1] {
                            assert_eq!(*y, 2.0);
                        } else {
                            panic!("Expected F64");
                        }
                    } else {
                        panic!("Expected Array");
                    }
                } else {
                    panic!("Expected inner Ext");
                }
            }
            _ => panic!("Expected Ext variant"),
        }
    }

    #[test]
    fn test_equality() {
        let p1 = Point { x: 1.0, y: 2.0 };
        let p2 = Point { x: 1.0, y: 2.0 };
        let p3 = Point { x: 2.0, y: 3.0 };
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_clone() {
        let p1 = Point { x: 1.0, y: 2.0 };
        let p2 = p1;
        assert_eq!(p1, p2);
    }
}
