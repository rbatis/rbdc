use crate::connection::PgConnection;
use crate::type_info::PgTypeInfo;
use crate::types::encode::{Encode, IsNull};
use crate::types::TypeInfo;
use crate::value::PgValueFormat;
use rbdc::error::Error;
use rbdc::ext::ustr::UStr;
use rbs::Value;
use std::fmt::{self, Write};
use std::ops::{Deref, DerefMut};

// TODO: buf.patch(|| ...) is a poor name, can we think of a better name? Maybe `buf.lazy(||)` ?
// TODO: Extend the patch system to support dynamic lengths
//       Considerations:
//          - The prefixed-len offset needs to be back-tracked and updated
//          - message::Bind needs to take a &PgArguments and use a `write` method instead of
//            referencing a buffer directly
//          - The basic idea is that we write bytes for the buffer until we get somewhere
//            that has a patch, we then apply the patch which should write to &mut Vec<u8>,
//            backtrack and update the prefixed-len, then write until the next patch offset

#[derive(Default)]
pub struct PgArgumentBuffer {
    buffer: Vec<u8>,

    // Number of arguments
    count: usize,

    // Session timezone offset in seconds from UTC
    // Used for encoding DateTime to TIMESTAMPTZ
    pub(crate) timezone_sec: Option<i32>,

    // Whenever an `Encode` impl needs to defer some work until after we resolve parameter types
    // it can use `patch`.
    //
    // This currently is only setup to be useful if there is a *fixed-size* slot that needs to be
    // tweaked from the input type. However, that's the only use case we currently have.
    //
    patches: Vec<(
        usize, // offset
        usize, // argument index
        Box<dyn Fn(&mut [u8], &PgTypeInfo) + 'static + Send + Sync>,
    )>,

    // Whenever an `Encode` impl encounters a `PgTypeInfo` object that does not have an OID
    // It pushes a "hole" that must be patched later.
    //
    // The hole is a `usize` offset into the buffer with the type name that should be resolved
    // This is done for Records and Arrays as the OID is needed well before we are in an async
    // function and can just ask postgres.
    //
    type_holes: Vec<(usize, UStr)>, // Vec<{ offset, type_name }>
}

/// Implementation of [`Arguments`] for PostgreSQL.
#[derive(Default)]
pub struct PgArguments {
    // Types of each bind parameter
    pub(crate) types: Vec<PgTypeInfo>,

    // Wire format of each bind parameter
    pub(crate) formats: Vec<PgValueFormat>,

    // Buffer of encoded bind parameters
    pub(crate) buffer: PgArgumentBuffer,
}

impl PgArguments {
    pub fn add(&mut self, value: Value) -> Result<(), Error> {
        // encode the value into our buffer
        let type_info = self.buffer.encode(value)?;
        let format = match type_info.0 {
            crate::type_info::PgType::Tsvector | crate::type_info::PgType::Tsquery => {
                PgValueFormat::Text
            }
            _ => PgValueFormat::Binary,
        };
        self.types.push(type_info);
        self.formats.push(format);
        // increment the number of arguments we are tracking
        self.buffer.count += 1;
        Ok(())
    }

    //Apply patches
    //This should only go out and ask postgres if we have not seen the type name yet
    pub(crate) async fn apply_patches(
        &mut self,
        conn: &mut PgConnection,
        parameters: &[PgTypeInfo],
    ) -> Result<(), Error> {
        let PgArgumentBuffer {
            ref patches,
            ref type_holes,
            ref mut buffer,
            ..
        } = self.buffer;

        for (offset, ty, callback) in patches {
            let buf = &mut buffer[*offset..];
            let ty = &parameters[*ty];
            callback(buf, ty);
        }

        for (offset, name) in type_holes {
            let oid = conn.fetch_type_id_by_name(&*name).await?;
            buffer[*offset..(*offset + 4)].copy_from_slice(&oid.0.to_be_bytes());
        }

        Ok(())
    }
}

impl PgArguments {
    pub fn reserve(&mut self, additional: usize, size: usize) {
        self.types.reserve(additional);
        self.buffer.reserve(size);
    }

    pub fn format_placeholder<W: Write>(&self, writer: &mut W) -> fmt::Result {
        write!(writer, "${}", self.buffer.count)
    }
}

impl PgArgumentBuffer {
    pub fn encode(&mut self, value: Value) -> Result<PgTypeInfo, Error> {
        // reserve space to write the prefixed length of the value
        let offset = self.len();
        self.extend(&[0; 4]);

        let info = value.type_info();
        // encode the value into our buffer
        let is_null = value.encode(self)?;
        let len = if let IsNull::No = is_null {
            (self.len() - offset - 4) as i32
        } else {
            // Write a -1 to indicate NULL
            // NOTE: It is illegal for [encode] to write any data
            debug_assert_eq!(self.len(), offset + 4);
            -1_i32
        };

        // write the len to the beginning of the value
        self[offset..(offset + 4)].copy_from_slice(&len.to_be_bytes());
        return Ok(info);
    }

    // Adds a callback to be invoked later when we know the parameter type
    #[allow(dead_code)]
    pub(crate) fn patch<F>(&mut self, callback: F)
    where
        F: Fn(&mut [u8], &PgTypeInfo) + 'static + Send + Sync,
    {
        let offset = self.len();
        let index = self.count;

        self.patches.push((offset, index, Box::new(callback)));
    }

    // Extends the inner buffer by enough space to have an OID
    // Remembers where the OID goes and type name for the OID
    pub(crate) fn patch_type_by_name(&mut self, type_name: &UStr) {
        let offset = self.len();

        self.extend_from_slice(&0_u32.to_be_bytes());
        self.type_holes.push((offset, type_name.clone()));
    }
}

impl Deref for PgArgumentBuffer {
    type Target = Vec<u8>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl DerefMut for PgArgumentBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use crate::type_info::{PgType, PgTypeInfo};
    use rbdc::ext::ustr::UStr;

    #[test]
    fn add_reserve_and_placeholders() {
        let mut args = PgArguments::default();
        assert_eq!(args.types.len(), 0);
        args.reserve(4, 128);

        args.add(Value::I32(1)).unwrap();
        args.add(Value::String("two".into())).unwrap();
        args.add(Value::Null).unwrap();
        assert_eq!(args.types.len(), 3);
        assert_eq!(args.formats.len(), 3);
        assert!(args
            .formats
            .iter()
            .all(|f| *f == PgValueFormat::Binary));
        assert_eq!(args.types[0], PgTypeInfo(PgType::Int4));
        assert_eq!(args.types[1], PgTypeInfo(PgType::Varchar));
        assert_eq!(args.types[2], PgTypeInfo(PgType::Unknown));

        // count drives placeholders
        let mut out = String::new();
        args.format_placeholder(&mut out).unwrap();
        assert_eq!(out, "$3");

        // values are length prefixed; the NULL is -1
        assert_eq!(&args.buffer[0..4], &4i32.to_be_bytes());
        assert_eq!(&args.buffer[4..8], &1i32.to_be_bytes());
        let null_offset = 4 + 4 + 4 + 3;
        assert_eq!(&args.buffer[null_offset..null_offset + 4], &(-1i32).to_be_bytes());

        // Deref / DerefMut
        assert_eq!(args.buffer.len(), args.buffer.buffer.len());
        args.buffer.push(9);
        assert_eq!(*args.buffer.last().unwrap(), 9);
    }

    #[test]
    fn from_args_sets_timezone_and_encodes() {
        let args = PgArguments::from_args(vec![Value::I32(1), Value::Null], Some(3600)).unwrap();
        assert_eq!(args.types.len(), 2);
        assert_eq!(args.formats.len(), 2);
        // timezone is carried on the buffer (private field, accessible here)
        assert_eq!(args.buffer.timezone_sec, Some(3600));

        let empty = PgArguments::from_args(vec![], None).unwrap();
        assert!(empty.types.is_empty());
        assert!(empty.formats.is_empty());
    }

    #[test]
    fn text_search_parameters_use_text_format() {
        let args = PgArguments::from_args(
            vec![
                Value::Ext("TsVector", Box::new(Value::String("'a'".into()))),
                Value::Ext("TsQuery", Box::new(Value::String("a & b".into()))),
                Value::I32(1),
            ],
            None,
        )
        .unwrap();
        assert_eq!(
            args.formats,
            vec![
                PgValueFormat::Text,
                PgValueFormat::Text,
                PgValueFormat::Binary
            ]
        );
        assert_eq!(args.types[0], PgTypeInfo(PgType::Tsvector));
        assert_eq!(args.types[1], PgTypeInfo(PgType::Tsquery));
    }

    #[test]
    fn patch_and_type_holes_are_recorded() {
        let mut buf = PgArgumentBuffer::default();
        buf.patch(|_buf, _ty| {});
        assert_eq!(buf.patches.len(), 1);

        buf.patch_type_by_name(&UStr::from("my_type"));
        assert_eq!(buf.type_holes.len(), 1);
        assert_eq!(&*buf.type_holes[0].1, "my_type");
        // four zero bytes were reserved for the OID
        assert_eq!(&buf[buf.type_holes[0].0..buf.type_holes[0].0 + 4], &[0, 0, 0, 0]);
    }
}
