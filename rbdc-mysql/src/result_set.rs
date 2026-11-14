use crate::protocol::text::{ColumnDefinition, ColumnFlags, ColumnType};
use rbdc::ext::ustr::UStr;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MySqlColumn {
    pub ordinal: usize,
    pub name: UStr,
    pub type_info: MySqlTypeInfo,
    // #[serde(skip)]
    // pub flags: Option<ColumnFlags>,
}

/// Type information for a MySql type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MySqlTypeInfo {
    pub r#type: ColumnType,
    pub flags: ColumnFlags,
    pub char_set: u16,
    // [max_size] for integer types, this is (M) in BIT(M) or TINYINT(M)
    // #[serde(default)]
    // pub max_size: Option<u32>,
}
impl MySqlTypeInfo {
    fn is_null(&self) -> bool {
        matches!(self.r#type, ColumnType::Null)
    }

    pub const fn binary(ty: ColumnType) -> Self {
        Self {
            r#type: ty,
            flags: ColumnFlags::BINARY,
            char_set: 63,
        }
    }

    #[doc(hidden)]
    pub const fn null() -> Self {
        Self {
            r#type: ColumnType::Null,
            flags: ColumnFlags::BINARY,
            char_set: 63,
        }
    }

    #[doc(hidden)]
    pub const fn __enum() -> Self {
        Self {
            r#type: ColumnType::Enum,
            flags: ColumnFlags::BINARY,
            char_set: 63,
        }
    }

    #[doc(hidden)]
    pub fn __type_feature_gate(&self) -> Option<&'static str> {
        match self.r#type {
            ColumnType::Date | ColumnType::Time | ColumnType::Timestamp | ColumnType::DateTime => {
                Some("time")
            }
            ColumnType::Json => Some("json"),
            ColumnType::NewDecimal => Some("bigdecimal"),
            _ => None,
        }
    }

    pub(crate) fn from_column(column: &ColumnDefinition) -> Self {
        Self {
            r#type: column.r#type,
            flags: column.flags,
            char_set: column.char_set,
        }
    }

    pub fn from_type(ty: ColumnType) -> Self {
        Self {
            r#type: ty,
            flags: ColumnFlags::empty(),
            char_set: 63,
        }
    }

    pub fn is_unsigned(&self) -> bool {
        self.flags.contains(ColumnFlags::UNSIGNED)
    }

    pub const fn unsigned(ty: ColumnType) -> Self {
        Self {
            r#type: ty,
            flags: ColumnFlags::UNSIGNED,
            char_set: 63,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::text::{ColumnDefinition, ColumnType};
    use rbdc::io::Decode;

    /// Build a minimal ColumnDefinition packet using the length-encoded encoding.
    fn definition_bytes(ty: ColumnType) -> Vec<u8> {
        use crate::io::MySqlBufMutExt;
        use bytes::BufMut;
        let mut b = Vec::new();
        b.put_bytes_lenenc(b"def".to_vec());
        b.put_bytes_lenenc(b"db".to_vec());
        b.put_bytes_lenenc(b"t".to_vec());
        b.put_bytes_lenenc(b"t".to_vec());
        b.put_bytes_lenenc(b"a".to_vec());
        b.put_bytes_lenenc(b"name".to_vec());
        b.put_uint_lenenc(12);
        b.put_u16_le(45);
        b.put_u32_le(255);
        b.put_u8(ty as u8);
        b.put_u16_le(1);
        b.put_u8(0);
        b
    }

    #[test]
    fn test_type_info_constructors() {
        let b = MySqlTypeInfo::binary(ColumnType::Blob);
        assert_eq!(b.r#type, ColumnType::Blob);
        assert_eq!(b.char_set, 63);

        let n = MySqlTypeInfo::null();
        assert_eq!(n.r#type, ColumnType::Null);
        assert!(n.is_null());

        let e = MySqlTypeInfo::__enum();
        assert_eq!(e.r#type, ColumnType::Enum);

        let t = MySqlTypeInfo::from_type(ColumnType::VarChar);
        assert_eq!(t.r#type, ColumnType::VarChar);
    }

    #[test]
    fn test_type_feature_gate() {
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::Date).__type_feature_gate(),
            Some("time")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::Time).__type_feature_gate(),
            Some("time")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::Timestamp).__type_feature_gate(),
            Some("time")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::DateTime).__type_feature_gate(),
            Some("time")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::Json).__type_feature_gate(),
            Some("json")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::NewDecimal).__type_feature_gate(),
            Some("bigdecimal")
        );
        assert_eq!(
            MySqlTypeInfo::from_type(ColumnType::Long).__type_feature_gate(),
            None
        );
    }

    #[test]
    fn test_from_column() {
        let def = ColumnDefinition::decode_with(
            definition_bytes(ColumnType::VarChar).into(),
            crate::protocol::Capabilities::empty(),
        )
        .unwrap();
        let info = MySqlTypeInfo::from_column(&def);
        assert_eq!(info.r#type, ColumnType::VarChar);
        assert_eq!(info.char_set, 45);
    }
}
