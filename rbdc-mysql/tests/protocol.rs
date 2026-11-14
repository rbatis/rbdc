//! Offline coverage for the MySQL wire protocol: packet framing, connection
//! phase packets, generic response packets, text and binary result-set rows,
//! column definitions and the COM_* command encoders.

use bytes::{Buf, Bytes};
use rbdc::io::{Decode, Encode};
use rbdc::ext::ustr::UStr;
use rbdc_mysql::io::{MySqlBufExt, MySqlBufMutExt};
use rbdc_mysql::protocol::connect::{
    AuthSwitchRequest, AuthSwitchResponse, Handshake, HandshakeResponse, SslRequest,
};
use rbdc_mysql::protocol::response::{EofPacket, ErrPacket, OkPacket, Status};
use rbdc_mysql::protocol::statement::{
    BinaryRow, Execute, Prepare, PrepareOk, StmtClose,
};
use rbdc_mysql::protocol::text::{
    ColumnDefinition, ColumnFlags, ColumnType, Ping, Query, Quit, TextRow,
};
use rbdc_mysql::protocol::{Capabilities, Packet, Row};
use rbdc_mysql::result_set::{MySqlColumn, MySqlTypeInfo};
use rbdc_mysql::stmt::MySqlArguments;
use rbs::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn column(ordinal: usize, ty: ColumnType) -> MySqlColumn {
    MySqlColumn {
        ordinal,
        name: UStr::new(&format!("c{ordinal}")),
        type_info: MySqlTypeInfo::from_type(ty),
    }
}

fn columns(types: &[ColumnType]) -> Vec<MySqlColumn> {
    types
        .iter()
        .enumerate()
        .map(|(i, ty)| column(i, *ty))
        .collect()
}

fn decode_text_row(bytes: Vec<u8>, cols: &[MySqlColumn]) -> TextRow {
    TextRow::decode_with(Bytes::from(bytes), cols).unwrap()
}

fn decode_binary_row(bytes: Vec<u8>, cols: &[MySqlColumn]) -> BinaryRow {
    BinaryRow::decode_with(Bytes::from(bytes), cols).unwrap()
}

/// Build a ColumnDefinition packet payload.
#[allow(clippy::too_many_arguments)]
fn column_definition_payload(
    catalog: &[u8],
    schema: &[u8],
    table_alias: &[u8],
    table: &[u8],
    alias: &[u8],
    name: &[u8],
    char_set: u16,
    max_size: u32,
    type_id: u8,
    flags: u16,
    decimals: u8,
) -> Vec<u8> {
    let mut b = Vec::new();
    b.put_bytes_lenenc(catalog.to_vec());
    b.put_bytes_lenenc(schema.to_vec());
    b.put_bytes_lenenc(table_alias.to_vec());
    b.put_bytes_lenenc(table.to_vec());
    b.put_bytes_lenenc(alias.to_vec());
    b.put_bytes_lenenc(name.to_vec());
    b.put_uint_lenenc(12);
    b.extend_from_slice(&char_set.to_le_bytes());
    b.extend_from_slice(&max_size.to_le_bytes());
    b.push(type_id);
    b.extend_from_slice(&flags.to_le_bytes());
    b.push(decimals);
    b
}

fn decode_column(payload: Vec<u8>) -> Result<ColumnDefinition, rbdc::Error> {
    ColumnDefinition::decode_with(Bytes::from(payload), Capabilities::empty())
}

/// A raw payload used to exercise `Packet<T>` framing.
struct Raw(Vec<u8>);

impl<'a> Encode<'a, Capabilities> for Raw {
    fn encode_with(&self, buf: &mut Vec<u8>, _: Capabilities) {
        buf.extend_from_slice(&self.0);
    }
}

fn encode_packet(payload: Vec<u8>, caps: Capabilities, seq: &mut u8) -> Vec<u8> {
    let mut out = Vec::new();
    Packet(Raw(payload)).encode_with(&mut out, (caps, seq));
    out
}

// ---------------------------------------------------------------------------
// Length-encoded primitives (src/io/buf.rs)
// ---------------------------------------------------------------------------

#[test]
fn test_get_uint_lenenc_all_forms() {
    // One byte.
    let mut b = Bytes::from_static(b"\x05");
    assert_eq!(b.get_uint_lenenc(), 5);

    // 0xfc -> u16
    let mut b = Bytes::from_static(b"\xfc\x34\x12");
    assert_eq!(b.get_uint_lenenc(), 0x1234);

    // 0xfd -> 3-byte integer
    let mut b = Bytes::from_static(b"\xfd\x56\x34\x12");
    assert_eq!(b.get_uint_lenenc(), 0x123456);

    // 0xfe -> u64
    let mut b = Bytes::from_static(b"\xfe\x01\x00\x00\x00\x00\x00\x00\x00");
    assert_eq!(b.get_uint_lenenc(), 1);
}

#[test]
fn test_get_str_and_bytes_lenenc() {
    let mut b = Bytes::from_static(b"\x03abc");
    assert_eq!(b.get_str_lenenc().unwrap(), "abc");

    let mut b = Bytes::from_static(b"\x03xyz");
    assert_eq!(&*b.get_bytes_lenenc(), b"xyz");

    // Invalid UTF-8 is rejected.
    let mut b = Bytes::from_static(b"\x01\xff");
    assert!(b.get_str_lenenc().is_err());
}

// ---------------------------------------------------------------------------
// Packet framing (src/protocol/packet.rs)
// ---------------------------------------------------------------------------

#[test]
fn test_packet_encode_small() {
    let mut seq = 5u8;
    let out = encode_packet(vec![1, 2, 3], Capabilities::empty(), &mut seq);
    assert_eq!(out, vec![3, 0, 0, 5, 1, 2, 3]);
    assert_eq!(seq, 6);
}

#[test]
fn test_packet_encode_sequence_wraps() {
    let mut seq = 255u8;
    let out = encode_packet(vec![7], Capabilities::empty(), &mut seq);
    assert_eq!(out, vec![1, 0, 0, 255, 7]);
    assert_eq!(seq, 0);
}

#[test]
fn test_packet_encode_splits_large_payload() {
    // Two full packets plus one byte ensures both the chunk loop and a
    // non-empty remainder are exercised.
    const MAX: usize = 0xFF_FF_FF;
    let payload = vec![0xAB_u8; 2 * MAX + 1];
    let mut seq = 0u8;
    let out = encode_packet(payload, Capabilities::empty(), &mut seq);

    assert_eq!(out.len(), 4 + MAX + 4 + MAX + 4 + 1);
    assert_eq!(&out[0..3], &[0xFF, 0xFF, 0xFF]);
    assert_eq!(out[3], 0);
    let second = 4 + MAX;
    assert_eq!(&out[second..second + 3], &[0xFF, 0xFF, 0xFF]);
    assert_eq!(out[second + 3], 1);
    let third = second + 4 + MAX;
    assert_eq!(&out[third..third + 3], &[1, 0, 0]);
    assert_eq!(out[third + 3], 2);
    assert_eq!(seq, 3);
}

#[test]
fn test_packet_decode_helpers_and_deref() {
    let ok = Packet(Bytes::from_static(b"\x00\x00\x00\x02\x00\x00\x00"));
    let packet = ok.ok().unwrap();
    assert_eq!(packet.affected_rows, 0);

    let p = Packet(Bytes::from_static(b"abc"));
    assert_eq!(&**p, b"abc");
    assert_eq!(p.decode::<Bytes>().unwrap(), Bytes::from_static(b"abc"));

    let mut p = Packet(Bytes::from_static(b"abc"));
    p.advance(1);
    assert_eq!(&**p, b"bc");

    // decode_with threads a context through to the decoder.
    let eof = EofPacket::decode_with(
        Bytes::from_static(b"\xfe\x01\x00\x02\x00"),
        Capabilities::empty(),
    );
    assert!(eof.is_ok());
}

#[test]
fn test_packet_eof_deprecate_and_legacy() {
    // With DEPRECATE_EOF an OK packet (header 0x00) is accepted as EOF.
    let ok_payload = Bytes::from_static(b"\x00\x00\x00\x02\x00\x07\x00");
    let eof = Packet(ok_payload)
        .eof(Capabilities::DEPRECATE_EOF)
        .unwrap();
    assert_eq!(eof.warnings, 7);
    assert!(eof.status.contains(Status::SERVER_STATUS_AUTOCOMMIT));

    // Without it, a legacy EOF packet is decoded.
    let eof_payload = Bytes::from_static(b"\xfe\x03\x00\x02\x00");
    let eof = Packet(eof_payload).eof(Capabilities::empty()).unwrap();
    assert_eq!(eof.warnings, 3);
}

// ---------------------------------------------------------------------------
// Generic response packets
// ---------------------------------------------------------------------------

#[test]
fn test_ok_packet_lenenc_fields_and_header_fe() {
    let mut b = Vec::new();
    b.push(0x00);
    b.put_uint_lenenc(0x1234);
    b.put_uint_lenenc(0x123456);
    b.extend_from_slice(&Status::SERVER_STATUS_IN_TRANS.bits().to_le_bytes());
    b.extend_from_slice(&9u16.to_le_bytes());
    let p = OkPacket::decode(Bytes::from(b)).unwrap();
    assert_eq!(p.affected_rows, 0x1234);
    assert_eq!(p.last_insert_id, 0x123456);
    assert_eq!(p.warnings, 9);
    assert!(p.status.contains(Status::SERVER_STATUS_IN_TRANS));

    let p = OkPacket::decode(Bytes::from_static(
        b"\xfe\x00\x00\x00\x00\x00\x00",
    ))
    .unwrap();
    assert_eq!(p.affected_rows, 0);
}

#[test]
fn test_ok_packet_invalid_header() {
    assert!(OkPacket::decode(Bytes::from_static(b"\x01\x00\x00\x00\x00\x00")).is_err());
}

#[test]
fn test_eof_packet_decode_and_invalid_header() {
    let p = EofPacket::decode_with(
        Bytes::from_static(b"\xfe\x02\x00\x08\x00"),
        Capabilities::empty(),
    )
    .unwrap();
    assert_eq!(p.warnings, 2);
    assert!(p.status.contains(Status::SERVER_MORE_RESULTS_EXISTS));

    assert!(EofPacket::decode_with(Bytes::from_static(b"\x00\x00\x00\x00\x00"), Capabilities::empty())
        .is_err());
}

#[test]
fn test_err_packet_with_and_without_sql_state() {
    let p = ErrPacket::decode_with(
        Bytes::from_static(b"\xff\x19\x04#42000Unknown database 'x'"),
        Capabilities::PROTOCOL_41,
    )
    .unwrap();
    assert_eq!(p.error_code, 1049);
    assert_eq!(p.sql_state.as_deref(), Some("42000"));
    assert_eq!(p.error_message, "Unknown database 'x'");

    let p = ErrPacket::decode_with(
        Bytes::from_static(b"\xff\x84\x04Got packets out of order"),
        Capabilities::PROTOCOL_41,
    )
    .unwrap();
    assert_eq!(p.sql_state, None);
    assert_eq!(p.error_message, "Got packets out of order");
}

#[test]
fn test_err_packet_without_protocol_41_keeps_message() {
    // Without PROTOCOL_41 a leading '#' is not treated as a SQL state.
    let p = ErrPacket::decode_with(
        Bytes::from_static(b"\xff\x19\x04#42000still message"),
        Capabilities::empty(),
    )
    .unwrap();
    assert_eq!(p.sql_state, None);
    assert_eq!(p.error_message, "#42000still message");
}

#[test]
fn test_err_packet_invalid_inputs() {
    // Wrong header marker.
    assert!(ErrPacket::decode_with(
        Bytes::from_static(b"\x00\x19\x04boom"),
        Capabilities::PROTOCOL_41
    )
    .is_err());

    // SQL state that is not valid UTF-8.
    assert!(ErrPacket::decode_with(
        Bytes::from_static(b"\xff\x19\x04#\xff\xff\xff\xff\xff"),
        Capabilities::PROTOCOL_41
    )
    .is_err());

    // Error message that is not valid UTF-8.
    assert!(ErrPacket::decode_with(
        Bytes::from_static(b"\xff\x19\x04\xff"),
        Capabilities::PROTOCOL_41
    )
    .is_err());
}

// ---------------------------------------------------------------------------
// Connection phase
// ---------------------------------------------------------------------------

#[test]
fn test_auth_switch_request_decode() {
    let mut b = Vec::with_capacity(64);
    b.push(0xfe);
    b.extend_from_slice(b"mysql_native_password\0");
    b.extend_from_slice(&[0xAA; 20]);
    b.push(0);
    let r = AuthSwitchRequest::decode_with(Bytes::from(b), ()).unwrap();
    assert_eq!(r.plugin.name(), "mysql_native_password");
    assert_eq!(&*r.data, &[0xAA; 20]);

    let mut resp = Vec::new();
    AuthSwitchResponse(vec![1, 2, 3]).encode_with(&mut resp, Capabilities::empty());
    assert_eq!(resp, vec![1, 2, 3]);
}

#[test]
fn test_auth_plugin_names() {
    use rbdc_mysql::protocol::auth::AuthPlugin;
    assert_eq!(
        AuthPlugin::MySqlNativePassword.name(),
        "mysql_native_password"
    );
    assert_eq!(
        AuthPlugin::CachingSha2Password.name(),
        "caching_sha2_password"
    );
    assert_eq!(AuthPlugin::Sha256Password.name(), "sha256_password");

    assert!(matches!(
        "mysql_native_password".parse::<AuthPlugin>().unwrap(),
        AuthPlugin::MySqlNativePassword
    ));
    assert!(matches!(
        "caching_sha2_password".parse::<AuthPlugin>().unwrap(),
        AuthPlugin::CachingSha2Password
    ));
    assert!(matches!(
        "sha256_password".parse::<AuthPlugin>().unwrap(),
        AuthPlugin::Sha256Password
    ));
    assert!("bogus".parse::<AuthPlugin>().is_err());
}

#[test]
fn test_auth_switch_request_errors() {
    // Wrong header.
    assert!(AuthSwitchRequest::decode_with(Bytes::from_static(b"\xff\x00"), ()).is_err());

    // Unknown plugin name.
    let mut b = vec![0xfe];
    b.extend_from_slice(b"bogus\0");
    b.extend_from_slice(&[0u8; 21]);
    assert!(AuthSwitchRequest::decode_with(Bytes::from(b), ()).is_err());

    // Wrong number of trailing bytes.
    let mut b = vec![0xfe];
    b.extend_from_slice(b"sha256_password\0");
    b.extend_from_slice(&[0u8; 10]);
    assert!(AuthSwitchRequest::decode_with(Bytes::from(b), ()).is_err());
}

#[test]
fn test_ssl_request_encode_mysql_and_mariadb() {
    let req = SslRequest {
        max_packet_size: 1024,
        collation: 45,
    };

    let mut mysql = Vec::new();
    req.encode_with(&mut mysql, Capabilities::MYSQL | Capabilities::PROTOCOL_41);
    // capabilities (4) + max packet (4) + collation (1) + reserved (19) + 4
    assert_eq!(mysql.len(), 32);
    assert_eq!(&mysql[0..4], &((Capabilities::MYSQL | Capabilities::PROTOCOL_41).bits() as u32).to_le_bytes());
    assert_eq!(&mysql[4..8], &1024u32.to_le_bytes());
    assert_eq!(mysql[8], 45);
    assert_eq!(&mysql[28..32], &[0, 0, 0, 0]);

    // Without MYSQL the MariaDB extended-capabilities word is written; no
    // defined flag lives above bit 31, so it is zero here, but the branch runs.
    let mut mariadb = Vec::new();
    req.encode_with(&mut mariadb, Capabilities::PROTOCOL_41);
    assert_eq!(&mariadb[28..32], &0u32.to_le_bytes());
}

#[test]
fn test_handshake_response_encode_secure_connection() {
    // No plugin means PLUGIN_AUTH must be stripped from the capabilities.
    let response = HandshakeResponse {
        database: Some("db"),
        max_packet_size: 1024,
        collation: 45,
        username: "user",
        auth_plugin: None,
        auth_response: Some(b"secret"),
    };
    let caps = Capabilities::SECURE_CONNECTION | Capabilities::CONNECT_WITH_DB | Capabilities::PLUGIN_AUTH;
    let mut buf = Vec::new();
    response.encode_with(&mut buf, caps);

    // capabilities/max-size/collation/reserved (32) + "user\0" + len + auth + "db\0"
    assert_eq!(buf.len(), 32 + 5 + 1 + 6 + 3);
    assert_eq!(&buf[32..37], b"user\0");
    assert_eq!(buf[37], 6);
    assert_eq!(&buf[38..44], b"secret");
    assert_eq!(&buf[44..47], b"db\0");
    // PLUGIN_AUTH was removed before encoding.
    assert_eq!(&buf[0..4], &((caps & !Capabilities::PLUGIN_AUTH).bits() as u32).to_le_bytes());
}

#[test]
fn test_handshake_response_encode_lenenc_and_plugin() {
    let response = HandshakeResponse {
        database: None,
        max_packet_size: 2048,
        collation: 33,
        username: "u",
        auth_plugin: Some(rbdc_mysql::protocol::auth::AuthPlugin::CachingSha2Password),
        auth_response: Some(&[1, 2, 3, 4]),
    };
    let caps = Capabilities::PLUGIN_AUTH
        | Capabilities::PLUGIN_AUTH_LENENC_DATA
        | Capabilities::CONNECT_WITH_DB;
    let mut buf = Vec::new();
    response.encode_with(&mut buf, caps);

    // Connect-with-db with no database writes a single NUL; the plugin name is
    // appended (NUL terminated).
    assert!(buf.ends_with(b"caching_sha2_password\0"));
    // The auth response is length-encoded.
    assert!(buf.windows(5).any(|w| w == [4, 1, 2, 3, 4]));
}

#[test]
fn test_handshake_response_encode_legacy_auth() {
    // Neither PLUGIN_AUTH_LENENC_DATA nor SECURE_CONNECTION -> a single NUL.
    let response = HandshakeResponse {
        database: None,
        max_packet_size: 0,
        collation: 0,
        username: "u",
        auth_plugin: None,
        auth_response: Some(b"ignored"),
    };
    let mut buf = Vec::new();
    response.encode_with(&mut buf, Capabilities::empty());
    assert_eq!(&buf[32..], b"u\0\x00");
}

#[test]
fn test_handshake_decode_without_optional_capabilities() {
    // No MYSQL / SECURE_CONNECTION / PLUGIN_AUTH flags at all.
    let mut b = Vec::new();
    b.push(10);
    b.extend_from_slice(b"5.7.0\0");
    b.extend_from_slice(&7u32.to_le_bytes());
    b.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    b.push(0); // reserved
    b.extend_from_slice(&0u16.to_le_bytes()); // capabilities low
    b.push(8); // collation
    b.extend_from_slice(&2u16.to_le_bytes()); // status
    b.extend_from_slice(&0u16.to_le_bytes()); // capabilities high
    b.push(0); // auth plugin data len (not plugin auth)
    b.extend_from_slice(&[0u8; 6]); // reserved
    b.extend_from_slice(&0x0000_0001u32.to_le_bytes()); // capabilities 3

    let h = Handshake::decode_with(Bytes::from(b), ()).unwrap();
    assert_eq!(h.protocol_version, 10);
    assert_eq!(h.server_version, "5.7.0");
    assert_eq!(h.connection_id, 7);
    assert!(h.auth_plugin.is_none());
    assert!(h.server_capabilities.is_empty());
    assert_eq!(h.auth_plugin_data.into_iter().count(), 8);
}

#[test]
fn test_handshake_decode_secure_connection_without_plugin_auth() {
    // SECURE_CONNECTION set but PLUGIN_AUTH clear: extra auth bytes are read
    // using the minimum length of 12.
    let mut b = Vec::new();
    b.push(10);
    b.extend_from_slice(b"5.7.0\0");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&[9; 8]);
    b.push(0);
    b.extend_from_slice(
        &((Capabilities::MYSQL | Capabilities::SECURE_CONNECTION).bits() as u16).to_le_bytes(),
    );
    b.push(8);
    b.extend_from_slice(&0u16.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes());
    b.push(0); // auth plugin data len
    b.extend_from_slice(&[0u8; 6]);
    b.extend_from_slice(&[0u8; 4]); // MySQL reserved
    b.extend_from_slice(&[0xCC; 12]);
    b.push(0); // NUL

    let h = Handshake::decode_with(Bytes::from(b), ()).unwrap();
    assert!(h.server_capabilities.contains(Capabilities::SECURE_CONNECTION));
    assert!(h.auth_plugin.is_none());
    assert_eq!(h.auth_plugin_data.into_iter().count(), 20);
}

#[test]
fn test_handshake_decode_invalid_auth_plugin() {
    let mut b = Vec::new();
    b.push(10);
    b.extend_from_slice(b"8.0.0\0");
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&[0; 8]);
    b.push(0);
    b.extend_from_slice(
        &((Capabilities::MYSQL | Capabilities::SECURE_CONNECTION).bits() as u16).to_le_bytes(),
    );
    b.push(255);
    b.extend_from_slice(&0u16.to_le_bytes());
    b.extend_from_slice(&((Capabilities::PLUGIN_AUTH.bits() >> 16) as u16).to_le_bytes());
    b.push(21);
    b.extend_from_slice(&[0u8; 6]);
    b.extend_from_slice(&[0u8; 4]);
    b.extend_from_slice(&[0u8; 12]);
    b.push(0);
    b.extend_from_slice(b"bogus\0");

    assert!(Handshake::decode_with(Bytes::from(b), ()).is_err());
}

// ---------------------------------------------------------------------------
// COM_* command packets
// ---------------------------------------------------------------------------

#[test]
fn test_prepare_and_stmt_close_encode() {
    let mut buf = Vec::new();
    Prepare { query: "SELECT ?" }.encode_with(&mut buf, Capabilities::empty());
    assert_eq!(buf[0], 0x16);
    assert_eq!(&buf[1..], b"SELECT ?");

    let mut buf = Vec::new();
    StmtClose { statement: 0x1234_5678 }.encode_with(&mut buf, Capabilities::empty());
    assert_eq!(buf[0], 0x19);
    assert_eq!(&buf[1..], &0x1234_5678u32.to_le_bytes());
}

#[test]
fn test_execute_encode_without_arguments() {
    let args = MySqlArguments::default();
    let mut buf = Vec::new();
    Execute {
        statement_id: 42,
        arguments: &args,
    }
    .encode_with(&mut buf, Capabilities::empty());

    assert_eq!(buf.len(), 10);
    assert_eq!(buf[0], 0x17);
    assert_eq!(&buf[1..5], &42u32.to_le_bytes());
    assert_eq!(buf[5], 0); // no cursor
    assert_eq!(&buf[6..10], &1u32.to_le_bytes());
}

#[test]
fn test_execute_encode_with_arguments() {
    let args = MySqlArguments::from_args(vec![
        Value::I32(7),
        Value::Null,
        Value::String("foo".to_string()),
        Value::Bool(true),
        Value::U64(u64::MAX),
        Value::F64(1.5),
        Value::Binary(vec![1, 2, 3]),
    ])
    .unwrap();
    assert_eq!(args.len(), 7);
    assert_eq!(args.null_bitmap.len(), 1);

    let mut buf = Vec::new();
    Execute {
        statement_id: 1,
        arguments: &args,
    }
    .encode_with(&mut buf, Capabilities::empty());

    assert_eq!(buf[0], 0x17);
    // null bitmap (1) + new-params flag (1) then 2 bytes per type.
    assert_eq!(buf[10], args.null_bitmap[0]);
    assert_eq!(buf[11], 1);
    assert_eq!(buf.len(), 12 + args.types.len() * 2 + args.values.len());
}

#[test]
fn test_prepare_ok_decode() {
    let mut b = vec![0x00];
    b.extend_from_slice(&77u32.to_le_bytes());
    b.extend_from_slice(&3u16.to_le_bytes());
    b.extend_from_slice(&2u16.to_le_bytes());
    b.push(0);
    b.extend_from_slice(&5u16.to_le_bytes());

    let p = PrepareOk::decode_with(Bytes::from(b), Capabilities::empty()).unwrap();
    assert_eq!(p.statement_id, 77);
    assert_eq!(p.columns, 3);
    assert_eq!(p.params, 2);
    assert_eq!(p.warnings, 5);

    assert!(PrepareOk::decode_with(
        Bytes::from_static(b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"),
        Capabilities::empty()
    )
    .is_err());
}

#[test]
fn test_text_command_encoders() {
    let mut buf = Vec::new();
    Ping.encode_with(&mut buf, Capabilities::empty());
    assert_eq!(buf, vec![0x0e]);

    let mut buf = Vec::new();
    Quit.encode_with(&mut buf, Capabilities::empty());
    assert_eq!(buf, vec![0x01]);

    let mut buf = Vec::new();
    Query("SELECT 1").encode_with(&mut buf, Capabilities::empty());
    assert_eq!(buf[0], 0x03);
    assert_eq!(&buf[1..], b"SELECT 1");
}

// ---------------------------------------------------------------------------
// Column definitions
// ---------------------------------------------------------------------------

#[test]
fn test_column_definition_decode() {
    let payload = column_definition_payload(
        b"def",
        b"schema",
        b"table_alias",
        b"table",
        b"alias",
        b"name",
        45,
        255,
        ColumnType::VarChar as u8,
        (ColumnFlags::NOT_NULL | ColumnFlags::UNSIGNED).bits(),
        0,
    );
    let def = decode_column(payload).unwrap();
    assert_eq!(def.name().unwrap(), "name");
    assert_eq!(def.alias().unwrap(), "alias");
    assert_eq!(def.char_set, 45);
    assert_eq!(def.max_size, 255);
    assert_eq!(def.r#type, ColumnType::VarChar);
    assert!(def.flags.contains(ColumnFlags::NOT_NULL));
    assert!(def.flags.contains(ColumnFlags::UNSIGNED));
}

#[test]
fn test_column_definition_invalid_utf8_and_type() {
    let payload = column_definition_payload(
        b"def",
        b"schema",
        b"table_alias",
        b"table",
        &[0xff],
        &[0xfe],
        63,
        10,
        ColumnType::String as u8,
        0,
        0,
    );
    let def = decode_column(payload).unwrap();
    assert!(def.name().is_err());
    assert!(def.alias().is_err());

    // 0x11 is not a known column type.
    let payload = column_definition_payload(
        b"def", b"", b"", b"", b"", b"n", 63, 1, 0x11, 0, 0,
    );
    assert!(decode_column(payload).is_err());
}

#[test]
fn test_column_type_try_from_u16_all_known_and_unknown() {
    let known = [
        (0x00, ColumnType::Decimal),
        (0x01, ColumnType::Tiny),
        (0x02, ColumnType::Short),
        (0x03, ColumnType::Long),
        (0x04, ColumnType::Float),
        (0x05, ColumnType::Double),
        (0x06, ColumnType::Null),
        (0x07, ColumnType::Timestamp),
        (0x08, ColumnType::LongLong),
        (0x09, ColumnType::Int24),
        (0x0a, ColumnType::Date),
        (0x0b, ColumnType::Time),
        (0x0c, ColumnType::DateTime),
        (0x0d, ColumnType::Year),
        (0x0f, ColumnType::VarChar),
        (0x10, ColumnType::Bit),
        (0xf5, ColumnType::Json),
        (0xf6, ColumnType::NewDecimal),
        (0xf7, ColumnType::Enum),
        (0xf8, ColumnType::Set),
        (0xf9, ColumnType::TinyBlob),
        (0xfa, ColumnType::MediumBlob),
        (0xfb, ColumnType::LongBlob),
        (0xfc, ColumnType::Blob),
        (0xfd, ColumnType::VarString),
        (0xfe, ColumnType::String),
        (0xff, ColumnType::Geometry),
    ];
    for (id, expected) in known {
        assert_eq!(ColumnType::try_from_u16(id).unwrap(), expected);
    }
    for id in [0x0e, 0x11, 0x12, 0x13, 0x80, 0xf4] {
        assert!(ColumnType::try_from_u16(id).is_err(), "id {id:#x}");
    }
}

// ---------------------------------------------------------------------------
// Text protocol rows
// ---------------------------------------------------------------------------

#[test]
fn test_text_row_with_values_and_nulls() {
    let cols = columns(&[ColumnType::VarChar, ColumnType::Long, ColumnType::Blob]);
    // "abc", NULL, "xy"
    let row = decode_text_row(vec![3, b'a', b'b', b'c', 0xfb, 2, b'x', b'y'], &cols);

    assert_eq!(row.0.get(0), Some(&b"abc"[..]));
    assert_eq!(row.0.get(1), None);
    assert_eq!(row.0.get(2), Some(&b"xy"[..]));
    assert_eq!(row.0.get(3), None);
    assert_eq!(row.0.values.len(), 3);
}

#[test]
fn test_text_row_empty_and_truncated() {
    let cols = columns(&[ColumnType::VarChar]);
    let row = decode_text_row(vec![], &[]);
    assert_eq!(row.0.get(0), None);

    // Declared length exceeds the remaining payload.
    let err = TextRow::decode_with(Bytes::from_static(b"\x05ab"), &cols).unwrap_err();
    assert!(err.to_string().contains("exceeds remaining"));
}

#[test]
fn test_text_row_take_values() {
    let cols = columns(&[ColumnType::VarChar, ColumnType::VarChar]);
    let mut row = decode_text_row(vec![1, b'a', 1, b'b'], &cols);
    assert_eq!(row.0.take(0), Some(vec![b'a']));
    assert_eq!(row.0.take(0), None); // already taken
    assert_eq!(row.0.take(1), Some(vec![b'b']));
    assert_eq!(row.0.take(5), None);
}

// ---------------------------------------------------------------------------
// Binary protocol rows
// ---------------------------------------------------------------------------

#[test]
fn test_binary_row_fixed_width_integers() {
    let cols = columns(&[
        ColumnType::Tiny,
        ColumnType::Short,
        ColumnType::Long,
        ColumnType::LongLong,
        ColumnType::Float,
        ColumnType::Double,
        ColumnType::Int24,
        ColumnType::Year,
    ]);
    let mut b = vec![0u8]; // header
    b.extend_from_slice(&[0, 0]); // null bitmap (8 columns -> 2 bytes)
    b.push(0x01); // Tiny
    b.extend_from_slice(&0x1234u16.to_le_bytes()); // Short
    b.extend_from_slice(&0x12345678u32.to_le_bytes()); // Long
    b.extend_from_slice(&0x1122334455667788u64.to_le_bytes()); // LongLong
    b.extend_from_slice(&1.5f32.to_le_bytes()); // Float
    b.extend_from_slice(&2.5f64.to_le_bytes()); // Double
    b.extend_from_slice(&[3, 0, 0, 0]); // Int24
    b.extend_from_slice(&2024u16.to_le_bytes()); // Year

    let row = decode_binary_row(b, &cols);
    assert_eq!(row.0.get(0).unwrap(), &[1]);
    assert_eq!(row.0.get(1).unwrap(), &0x1234u16.to_le_bytes());
    assert_eq!(row.0.get(2).unwrap(), &0x12345678u32.to_le_bytes());
    assert_eq!(row.0.get(3).unwrap(), &0x1122334455667788u64.to_le_bytes());
    assert_eq!(row.0.get(4).unwrap(), &1.5f32.to_le_bytes());
    assert_eq!(row.0.get(5).unwrap(), &2.5f64.to_le_bytes());
    assert_eq!(row.0.get(6).unwrap(), &[3, 0, 0, 0]);
    assert_eq!(row.0.get(7).unwrap(), &2024u16.to_le_bytes());
}

#[test]
fn test_binary_row_length_encoded_and_temporal() {
    let cols = columns(&[
        ColumnType::VarChar,
        ColumnType::Blob,
        ColumnType::Json,
        ColumnType::Decimal,
        ColumnType::Bit,
    ]);
    let mut b = vec![0u8]; // header
    b.push(0); // null bitmap (5 columns -> 1 byte)
    b.extend_from_slice(&[2, b'h', b'i']); // VarChar
    b.extend_from_slice(&[2, 0xAA, 0xBB]); // Blob
    b.extend_from_slice(&[4, b'n', b'u', b'l', b'l']); // Json
    b.extend_from_slice(&[3, b'1', b'.', b'5']); // Decimal
    b.extend_from_slice(&[1, 0x01]); // Bit

    let row = decode_binary_row(b, &cols);
    assert_eq!(row.0.get(0).unwrap(), b"hi");
    assert_eq!(row.0.get(1).unwrap(), &[0xAA, 0xBB]);
    assert_eq!(row.0.get(2).unwrap(), b"null");
    assert_eq!(row.0.get(3).unwrap(), b"1.5");
    assert_eq!(row.0.get(4).unwrap(), &[0x01]);

    for ty in [
        ColumnType::Date,
        ColumnType::Time,
        ColumnType::Timestamp,
        ColumnType::DateTime,
    ] {
        let cols = columns(&[ty]);
        let mut b = vec![0u8, 0]; // header + null bitmap
        b.push(4); // temporal payload length
        b.extend_from_slice(&[0xE7, 0x07, 5, 6]);
        let row = decode_binary_row(b, &cols);
        assert_eq!(row.0.get(0).unwrap(), &[4, 0xE7, 0x07, 5, 6], "{ty:?}");
    }
}

#[test]
fn test_binary_row_null_bitmap() {
    let cols = columns(&[ColumnType::Long, ColumnType::Long, ColumnType::Long]);
    let mut b = vec![0u8]; // header
    // 3 columns -> bitmap length (3+9)/8 = 1. Column bits start at index 2:
    // col0 bit2 = 1 (NULL), col1 bit3 = 0, col2 bit4 = 1 (NULL).
    b.push(0b0001_0100);
    b.extend_from_slice(&7u32.to_le_bytes()); // only non-null column

    let row = decode_binary_row(b, &cols);
    assert_eq!(row.0.get(0), None);
    assert_eq!(row.0.get(1).unwrap(), &7u32.to_le_bytes());
    assert_eq!(row.0.get(2), None);
}

#[test]
fn test_binary_row_errors() {
    let cols = columns(&[ColumnType::VarChar]);

    // Wrong row header.
    assert!(
        BinaryRow::decode_with(Bytes::from_static(b"\x01\x00"), &cols).is_err()
    );

    // Truncated null bitmap: 15 columns need 3 bytes.
    let cols15: Vec<_> = (0..15).map(|i| column(i, ColumnType::Tiny)).collect();
    assert!(BinaryRow::decode_with(Bytes::from_static(b"\x00\x00"), &cols15).is_err());

    // Value length exceeds the buffer.
    assert!(BinaryRow::decode_with(
        Bytes::from_static(b"\x00\x00\x05ab"),
        &cols
    )
    .is_err());

    // Temporal value with no data at all.
    let cols = columns(&[ColumnType::DateTime]);
    let err = BinaryRow::decode_with(Bytes::from_static(b"\x00\x00"), &cols).unwrap_err();
    assert!(err.to_string().contains("temporal length"));
}

// ---------------------------------------------------------------------------
// Row storage helper
// ---------------------------------------------------------------------------

#[test]
fn test_row_from_ranges_and_get_take() {
    let data = b"hello world".to_vec();
    let mut row = Row::from((
        vec![Some(0..5), None, Some(6..11)],
        data,
    ));
    assert_eq!(row.storage.len(), 3);
    assert_eq!(row.get(0), Some(&b"hello"[..]));
    assert_eq!(row.get(1), None);
    assert_eq!(row.get(2), Some(&b"world"[..]));
    assert_eq!(row.get(99), None);

    assert_eq!(row.take(2), Some(b"world".to_vec()));
    assert_eq!(row.take(2), None);
    assert_eq!(row.take(99), None);

    // Explicitly constructed storage works too.
    let row = Row {
        storage: vec![Some(vec![1, 2]), None],
        values: vec![Some(0..2), None],
    };
    assert_eq!(row.get(0), Some(&[1, 2][..]));
    assert_eq!(row.get(1), None);
}
