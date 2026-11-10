//! Offline protocol message encode/decode coverage.

use bytes::Bytes;
use rbdc::io::{Decode, Encode};

use rbdc_pg::error::{PgDatabaseError, PgErrorPosition};
use rbdc_pg::message::{
    Authentication, Bind, Close, CommandComplete, CopyData, CopyDone, CopyFail, CopyResponse,
    Describe, Execute, Flush, Message, MessageFormat, Notification, Parse, Password, Query,
    ReadyForQuery, RowDescription, SaslInitialResponse, SaslResponse, SslRequest, Startup, Sync,
    Terminate,
};
use rbdc_pg::message::{Notice, PgSeverity};
use rbdc_pg::types::Oid;
use rbdc_pg::value::PgValueFormat;

fn enc<T: for<'a> Encode<'a>>(m: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    m.encode(&mut buf);
    buf
}

#[test]
fn message_format_all_variants() {
    let cases = [
        (b'1', MessageFormat::ParseComplete),
        (b'2', MessageFormat::BindComplete),
        (b'3', MessageFormat::CloseComplete),
        (b'C', MessageFormat::CommandComplete),
        (b'd', MessageFormat::CopyData),
        (b'c', MessageFormat::CopyDone),
        (b'G', MessageFormat::CopyInResponse),
        (b'H', MessageFormat::CopyOutResponse),
        (b'D', MessageFormat::DataRow),
        (b'E', MessageFormat::ErrorResponse),
        (b'I', MessageFormat::EmptyQueryResponse),
        (b'A', MessageFormat::NotificationResponse),
        (b'K', MessageFormat::BackendKeyData),
        (b'N', MessageFormat::NoticeResponse),
        (b'R', MessageFormat::Authentication),
        (b'S', MessageFormat::ParameterStatus),
        (b'T', MessageFormat::RowDescription),
        (b'Z', MessageFormat::ReadyForQuery),
        (b'n', MessageFormat::NoData),
        (b's', MessageFormat::PortalSuspended),
        (b't', MessageFormat::ParameterDescription),
    ];
    for (raw, expected) in cases {
        assert_eq!(MessageFormat::try_from_u8(raw).unwrap(), expected);
    }
    for raw in [0u8, b'x', b'?', 255] {
        assert!(MessageFormat::try_from_u8(raw).is_err());
    }
}

#[test]
fn message_decode_dispatches() {
    let message = Message {
        format: MessageFormat::ReadyForQuery,
        contents: Bytes::from_static(b"I"),
    };
    let ready: ReadyForQuery = message.decode().unwrap();
    assert!(matches!(
        ready.transaction_status,
        rbdc_pg::message::TransactionStatus::Idle
    ));

    let message = Message {
        format: MessageFormat::CommandComplete,
        contents: Bytes::from_static(b"UPDATE 3\0"),
    };
    let complete: CommandComplete = message.decode().unwrap();
    assert_eq!(complete.rows_affected(), 3);
}

#[test]
fn bind_encode() {
    let formats = [PgValueFormat::Text, PgValueFormat::Binary];
    let result_formats = [PgValueFormat::Binary];
    let params = [0u8, 0, 0, 0, 7];
    let bind = Bind {
        portal: Some(Oid(5)),
        statement: Oid(1),
        formats: &formats,
        num_params: 1,
        params: &params,
        result_formats: &result_formats,
    };
    let buf = enc(&bind);

    let mut expected = Vec::new();
    expected.push(b'B');
    let mut body = Vec::new();
    body.extend(b"rbdc_p_5\0");
    body.extend(b"rbdc_s_1\0");
    body.extend(&2i16.to_be_bytes());
    body.extend(&0i16.to_be_bytes());
    body.extend(&1i16.to_be_bytes());
    body.extend(&1i16.to_be_bytes());
    body.extend(&params);
    body.extend(&1i16.to_be_bytes());
    body.extend(&1i16.to_be_bytes());
    expected.extend(&((body.len() + 4) as i32).to_be_bytes());
    expected.extend(&body);
    assert_eq!(buf, expected);

    // unnamed portal
    let bind = Bind {
        portal: None,
        statement: Oid(2),
        formats: &[],
        num_params: 0,
        params: &[],
        result_formats: &[],
    };
    let buf = enc(&bind);
    assert_eq!(&buf[..1], b"B");
    assert!(buf.windows(1).any(|w| w == b"\0"));
}

#[test]
fn close_encode() {
    let portal = enc(&Close::Portal(Oid(7)));
    let mut expected = vec![b'C'];
    expected.extend(&14i32.to_be_bytes());
    expected.push(b'P');
    expected.extend(b"rbdc_p_7\0");
    assert_eq!(portal, expected);

    let statement = enc(&Close::Statement(Oid(7)));
    let mut expected = vec![b'C'];
    expected.extend(&14i32.to_be_bytes());
    expected.push(b'S');
    expected.extend(b"rbdc_s_7\0");
    assert_eq!(statement, expected);
}

#[test]
fn simple_message_encodings() {
    assert_eq!(enc(&Flush), b"H\0\0\0\x04".to_vec());
    assert_eq!(enc(&Sync), b"S\0\0\0\x04".to_vec());
    assert_eq!(enc(&Terminate), b"X\0\0\0\x04".to_vec());
    assert_eq!(
        enc(&SslRequest),
        vec![0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]
    );
}

#[test]
fn describe_and_execute_and_parse_and_query() {
    assert_eq!(
        enc(&Describe::UnnamedStatement),
        b"D\0\0\0\x06S\0".to_vec()
    );
    assert_eq!(
        enc(&Describe::UnnamedPortal),
        b"D\0\0\0\x06P\0".to_vec()
    );
    assert_eq!(
        enc(&Describe::Statement(Oid(5))),
        b"D\0\0\0\x0ESrbdc_s_5\0".to_vec()
    );
    assert_eq!(
        enc(&Describe::Portal(Oid(5))),
        b"D\0\0\0\x0EPrbdc_p_5\0".to_vec()
    );

    assert_eq!(
        enc(&Execute {
            portal: Some(Oid(5)),
            limit: 2
        }),
        b"E\0\0\0\x11rbdc_p_5\0\0\0\0\x02".to_vec()
    );
    assert_eq!(
        enc(&Execute {
            portal: None,
            limit: 0
        }),
        b"E\0\0\0\t\0\0\0\0\0".to_vec()
    );

    assert_eq!(
        enc(&Parse {
            statement: Oid(1),
            query: "SELECT $1",
            param_types: &[Oid(25)]
        }),
        b"P\0\0\0\x1drbdc_s_1\0SELECT $1\0\0\x01\0\0\0\x19".to_vec()
    );

    assert_eq!(enc(&Query("SELECT 1")), b"Q\0\0\0\rSELECT 1\0".to_vec());
}

#[test]
fn startup_and_password_encodings() {
    assert_eq!(
        enc(&Startup {
            username: Some("postgres"),
            database: Some("postgres"),
            params: &[]
        }),
        b"\0\0\0)\0\x03\0\0user\0postgres\0database\0postgres\0\0".to_vec()
    );

    // optional parts omitted, params included
    let out = enc(&Startup {
        username: None,
        database: None,
        params: &[("application_name", "app")],
    });
    assert!(out.ends_with(b"application_name\0app\0\0"));

    assert_eq!(
        enc(&Password::Cleartext("password")),
        b"p\0\0\0\rpassword\0".to_vec()
    );
    assert_eq!(
        enc(&Password::Md5 {
            password: "password",
            username: "root",
            salt: [147, 24, 57, 152]
        }),
        b"p\0\0\0(md53e2c9d99d49b201ef867a36f3f9ed62c\0".to_vec()
    );
}

#[test]
fn sasl_encodings() {
    let out = enc(&SaslInitialResponse {
        response: "n,,n=,r=abc",
        plus: false,
    });
    assert_eq!(out[0], b'p');
    assert!(out.windows(14).any(|w| w == b"SCRAM-SHA-256\0"));
    assert_eq!(
        &out[(out.len() - "n,,n=,r=abc".len())..],
        b"n,,n=,r=abc"
    );
    let out = enc(&SaslInitialResponse {
        response: "x",
        plus: true,
    });
    assert!(out.windows(19).any(|w| w == b"SCRAM-SHA-256-PLUS\0"));

    let out = enc(&SaslResponse("client-final"));
    assert_eq!(out[0], b'p');
    assert_eq!(&out[(out.len() - 12)..], b"client-final");
}

#[test]
fn authentication_decode_all_variants() {
    let auth = Authentication::decode(Bytes::from_static(b"\0\0\0\0")).unwrap();
    assert!(matches!(auth, Authentication::Ok));

    let auth = Authentication::decode(Bytes::from_static(b"\0\0\0\x03")).unwrap();
    assert!(matches!(auth, Authentication::CleartextPassword));

    let auth =
        Authentication::decode(Bytes::from_static(b"\0\0\0\x05salt")).unwrap();
    match auth {
        Authentication::Md5Password(md5) => assert_eq!(&md5.salt, b"salt"),
        _ => panic!("expected md5"),
    }

    let auth = Authentication::decode(Bytes::from_static(
        b"\0\0\0\x0aSCRAM-SHA-256\0\0",
    ))
    .unwrap();
    match auth {
        Authentication::Sasl(sasl) => {
            let mechanisms: Vec<_> = sasl.mechanisms().collect();
            assert_eq!(mechanisms, vec!["SCRAM-SHA-256"]);
        }
        _ => panic!("expected sasl"),
    }

    let continue_payload = b"\0\0\0\x0br=nonce,s=YWJj,i=100";
    let auth = Authentication::decode(Bytes::from_static(continue_payload)).unwrap();
    match auth {
        Authentication::SaslContinue(cont) => {
            assert_eq!(cont.nonce, "nonce");
            assert_eq!(cont.salt, b"abc");
            assert_eq!(cont.iterations, 100);
            assert!(cont.message.contains("r=nonce"));
        }
        _ => panic!("expected sasl continue"),
    }

    let auth =
        Authentication::decode(Bytes::from_static(b"\0\0\0\x0br=nonce,i=not-a-number"))
            .unwrap();
    match auth {
        Authentication::SaslContinue(cont) => assert_eq!(cont.iterations, 4096),
        _ => panic!("expected sasl continue"),
    }

    let auth = Authentication::decode(Bytes::from_static(b"\0\0\0\x0cv=YWJj")).unwrap();
    match auth {
        Authentication::SaslFinal(finalie) => assert_eq!(finalie.verifier, b"abc"),
        _ => panic!("expected sasl final"),
    }

    let auth = Authentication::decode(Bytes::from_static(b"\0\0\0\x0cother=x")).unwrap();
    match auth {
        Authentication::SaslFinal(finalie) => assert!(finalie.verifier.is_empty()),
        _ => panic!("expected sasl final"),
    }

    assert!(Authentication::decode(Bytes::from_static(b"\0\0\0\x63")).is_err());
    assert!(Authentication::decode(Bytes::from_static(
        b"\0\0\0\x0br=nonce,s=!!!,i=1"
    ))
    .is_err());
    assert!(Authentication::decode(Bytes::from_static(b"\0\0\0\x0br=\xff,s=YWJj")).is_err());
}

#[test]
fn sasl_mechanisms_edge_cases() {
    let auth = Authentication::decode(Bytes::from_static(b"\0\0\0\x0a")).unwrap();
    match auth {
        Authentication::Sasl(sasl) => {
            assert_eq!(sasl.mechanisms().next(), None);
        }
        _ => panic!(),
    }

    let auth =
        Authentication::decode(Bytes::from_static(b"\0\0\0\x0a\xff\0")).unwrap();
    match auth {
        Authentication::Sasl(sasl) => assert_eq!(sasl.mechanisms().next(), None),
        _ => panic!(),
    }

    let auth = Authentication::decode(Bytes::from_static(
        b"\0\0\0\x0aSCRAM-SHA-256\0OTHER\0\0",
    ))
    .unwrap();
    match auth {
        Authentication::Sasl(sasl) => {
            let mechanisms: Vec<_> = sasl.mechanisms().collect();
            assert_eq!(mechanisms, vec!["SCRAM-SHA-256", "OTHER"]);
        }
        _ => panic!(),
    }
}

#[test]
fn copy_messages() {
    // CopyResponse
    const COPY_RESPONSE: &[u8] = &[1, 0, 2, 0, 0, 0, 1];
    let resp = CopyResponse::decode(Bytes::from_static(COPY_RESPONSE)).unwrap();
    assert_eq!(resp.format, 1);
    assert_eq!(resp.num_columns, 2);
    assert_eq!(resp.format_codes, vec![0, 1]);

    // CopyData decode and encode
    let data = CopyData::decode(Bytes::from_static(b"hello")).unwrap();
    assert_eq!(&*data.0, b"hello");
    assert_eq!(
        enc(&CopyData(vec![1u8, 2, 3])),
        vec![b'd', 0, 0, 0, 7, 1, 2, 3]
    );

    // CopyFail
    let fail = CopyFail::decode(Bytes::from_static(b"oops\0")).unwrap();
    assert_eq!(fail.message, "oops");
    assert_eq!(
        enc(&CopyFail::new("oops")),
        vec![b'f', 0, 0, 0, 9, b'o', b'o', b'p', b's', 0]
    );

    // CopyDone
    assert!(CopyDone::decode(Bytes::new()).is_ok());
    assert!(CopyDone::decode(Bytes::from_static(b"x")).is_err());
    assert_eq!(enc(&CopyDone), vec![b'c', 0, 0, 0, 4]);
}

#[test]
fn notification_decode() {
    let notification = Notification::decode(Bytes::from_static(
        b"\x34\x20\x10\x02TEST-CHANNEL\0THIS IS A TEST\0",
    ))
    .unwrap();
    let debug = format!("{notification:?}");
    assert!(debug.contains("TEST-CHANNEL"));
    assert!(debug.contains("THIS IS A TEST"));
}

#[test]
fn row_description_decode() {
    let mut buf = Vec::new();
    buf.extend(2u16.to_be_bytes());
    // field 1: no relation
    buf.extend(b"a\0");
    buf.extend(0i32.to_be_bytes());
    buf.extend(0i16.to_be_bytes());
    buf.extend(23u32.to_be_bytes());
    buf.extend(4i16.to_be_bytes());
    buf.extend((-1i32).to_be_bytes());
    buf.extend(1i16.to_be_bytes());
    // field 2: with relation/attribute
    buf.extend(b"b\0");
    buf.extend(42i32.to_be_bytes());
    buf.extend(3i16.to_be_bytes());
    buf.extend(25u32.to_be_bytes());
    buf.extend((-1i16).to_be_bytes());
    buf.extend(0i32.to_be_bytes());
    buf.extend(0i16.to_be_bytes());

    let desc = RowDescription::decode(Bytes::from(buf)).unwrap();
    assert_eq!(desc.fields.len(), 2);
    assert_eq!(desc.fields[0].name, "a");
    assert_eq!(desc.fields[0].relation_id, None);
    assert_eq!(desc.fields[0].relation_attribute_no, None);
    assert_eq!(desc.fields[0].data_type_id, Oid(23));
    assert_eq!(desc.fields[0].data_type_size, 4);
    assert_eq!(desc.fields[0].type_modifier, -1);
    assert_eq!(desc.fields[0].format, 1);

    assert_eq!(desc.fields[1].name, "b");
    assert_eq!(desc.fields[1].relation_id, Some(42));
    assert_eq!(desc.fields[1].relation_attribute_no, Some(3));
    assert_eq!(desc.fields[1].data_type_id, Oid(25));
    assert_eq!(desc.fields[1].data_type_size, -1);
    assert_eq!(desc.fields[1].type_modifier, 0);
    assert_eq!(desc.fields[1].format, 0);

    // empty description
    let desc = RowDescription::decode(Bytes::from_static(b"\0\0")).unwrap();
    assert!(desc.fields.is_empty());
}

#[test]
fn ready_for_query_and_command_complete() {
    for (raw, status) in [
        (b'I', rbdc_pg::message::TransactionStatus::Idle),
        (b'T', rbdc_pg::message::TransactionStatus::Transaction),
        (b'E', rbdc_pg::message::TransactionStatus::Error),
    ] {
        let ready = ReadyForQuery::decode(Bytes::from(vec![raw])).unwrap();
        assert_eq!(
            std::mem::discriminant(&ready.transaction_status),
            std::mem::discriminant(&status)
        );
    }
    assert!(ReadyForQuery::decode(Bytes::from_static(b"X")).is_err());

    assert_eq!(
        CommandComplete::decode(Bytes::from_static(b"INSERT 0 1214\0"))
            .unwrap()
            .rows_affected(),
        1214
    );
    assert_eq!(
        CommandComplete::decode(Bytes::from_static(b"BEGIN\0"))
            .unwrap()
            .rows_affected(),
        0
    );
    assert_eq!(
        CommandComplete::decode(Bytes::from_static(b"DELETE 0\0"))
            .unwrap()
            .rows_affected(),
        0
    );
    assert_eq!(
        CommandComplete::decode(Bytes::from_static(b"SELECT notanumber\0"))
            .unwrap()
            .rows_affected(),
        0
    );
}

fn notice_payload(extra: &[u8]) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(
        b"SERROR\0VFATAL\0C42601\0Msyntax error\0Ddetail here\0Htry again\0P12\0Wcontext\0",
    );
    buf.extend_from_slice(b"sschema\0ttable\0ccolumn\0ddatatype\0nconstraint\0");
    buf.extend_from_slice(b"Ffile.rs\0L42\0Rroutine\0");
    buf.extend_from_slice(extra);
    buf.push(0);
    Bytes::from(buf)
}

#[test]
fn notice_and_database_error_accessors() {
    let notice = Notice::decode(notice_payload(b"")).unwrap();
    assert_eq!(notice.severity(), PgSeverity::Fatal); // V wins over S
    assert_eq!(notice.code(), "42601");
    assert_eq!(notice.message(), "syntax error");
    assert_eq!(notice.get(b'D'), Some("detail here"));
    assert_eq!(notice.get(b'n'), Some("constraint"));
    assert_eq!(notice.get(b'z'), None);
    assert_eq!(notice.get_raw(b'z'), None);

    let err = PgDatabaseError::from(notice);
    assert_eq!(err.severity(), PgSeverity::Fatal);
    assert_eq!(err.code(), "42601");
    assert_eq!(err.message(), "syntax error");
    assert_eq!(err.detail(), Some("detail here"));
    assert_eq!(err.hint(), Some("try again"));
    assert_eq!(err.position(), Some(PgErrorPosition::Original(12)));
    assert_eq!(err.r#where(), Some("context"));
    assert_eq!(err.schema(), Some("schema"));
    assert_eq!(err.table(), Some("table"));
    assert_eq!(err.column(), Some("column"));
    assert_eq!(err.data_type(), Some("datatype"));
    assert_eq!(err.constraint(), Some("constraint"));
    assert_eq!(err.file(), Some("file.rs"));
    assert_eq!(err.line(), Some(42));
    assert_eq!(err.routine(), Some("routine"));
    assert_eq!(format!("{err}"), "syntax error");
    let debug = format!("{err:?}");
    assert!(debug.contains("PgDatabaseError"));
    assert!(debug.contains("42601"));

    let rbs_err: rbs::Error = err.into();
    assert!(rbs_err.to_string().contains("42601"));
    assert!(rbs_err.to_string().contains("syntax error"));
}

#[test]
fn database_error_position_internal_and_missing() {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SERROR\0C42601\0Mbad\0p7\0qselect 1\0");
    buf.push(0);
    let err = PgDatabaseError::from(Notice::decode(Bytes::from(buf)).unwrap());
    assert_eq!(
        err.position(),
        Some(PgErrorPosition::Internal {
            position: 7,
            query: "select 1"
        })
    );

    // no position fields at all
    let err = PgDatabaseError::from(
        Notice::decode(Bytes::from_static(b"SERROR\0C1\0Mbad\0\0")).unwrap(),
    );
    assert_eq!(err.position(), None);

    // p without q -> None
    let err = PgDatabaseError::from(
        Notice::decode(Bytes::from_static(b"SERROR\0C1\0Mbad\0p3\0\0")).unwrap(),
    );
    assert_eq!(err.position(), None);

    // line number that is not numeric -> None
    let err = PgDatabaseError::from(
        Notice::decode(Bytes::from_static(b"SERROR\0C1\0Mbad\0Lx\0\0")).unwrap(),
    );
    assert_eq!(err.line(), None);
}

#[test]
fn notice_severity_and_error_paths() {
    // localized S is ignored, V used
    let notice =
        Notice::decode(Bytes::from_static(b"SFEHLER\0VERROR\0C1\0Mbad\0\0")).unwrap();
    assert_eq!(notice.severity(), PgSeverity::Error);

    // S only (older server)
    let notice = Notice::decode(Bytes::from_static(b"SWARNING\0C1\0Mbad\0\0")).unwrap();
    assert_eq!(notice.severity(), PgSeverity::Warning);

    // neither S nor V -> default Log
    let notice = Notice::decode(Bytes::from_static(b"C1\0Mbad\0\0")).unwrap();
    assert_eq!(notice.severity(), PgSeverity::Log);

    // empty payload
    assert!(Notice::decode(Bytes::new()).is_err());

    // non-UTF-8 message: cached accessors fall back to an empty string, and
    // `get` returns None.
    let notice = Notice::decode(Bytes::from_static(b"C1\0M\xff\0\0")).unwrap();
    assert_eq!(notice.message(), "");
    assert_eq!(notice.get(b'M'), None);
    // non-utf8 severity
    assert!(Notice::decode(Bytes::from_static(b"S\xff\0VERROR\0\0")).is_err());
    let mut payload = b"VERROR\0V".to_vec();
    payload.push(0xff);
    payload.extend_from_slice(b"\0\0");
    assert!(Notice::decode(Bytes::from(payload)).is_err());

    // unknown severity strings
    for (raw, expected) in [
        ("PANIC", PgSeverity::Panic),
        ("FATAL", PgSeverity::Fatal),
        ("ERROR", PgSeverity::Error),
        ("WARNING", PgSeverity::Warning),
        ("NOTICE", PgSeverity::Notice),
        ("DEBUG", PgSeverity::Debug),
        ("INFO", PgSeverity::Info),
        ("LOG", PgSeverity::Log),
    ] {
        assert_eq!(PgSeverity::try_from(raw).unwrap(), expected);
    }
    assert!(PgSeverity::try_from("UNKNOWN").is_err());
    assert!(PgSeverity::Panic.is_error());
    assert!(PgSeverity::Fatal.is_error());
    assert!(PgSeverity::Error.is_error());
    assert!(!PgSeverity::Warning.is_error());
    assert!(!PgSeverity::Notice.is_error());
    assert!(!PgSeverity::Debug.is_error());
    assert!(!PgSeverity::Info.is_error());
    assert!(!PgSeverity::Log.is_error());
}
