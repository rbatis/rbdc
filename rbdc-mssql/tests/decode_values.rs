//! Offline coverage for every `tiberius::ColumnData` variant in
//! `rbdc_mssql::decode`.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rbdc_mssql::decode::{DateTimeFromDateTimeFixedOffset, DateTimeFromNativeDatetime, Decode};
use rbs::Value;
use std::borrow::Cow;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::time::DateTime as TiberiusDateTime;
use tiberius::xml::XmlData;
use tiberius::{ColumnData, Uuid};

fn ext(name: &'static str, inner: Value) -> Value {
    Value::Ext(name, Box::new(inner))
}

fn days_between(year: i32, month: u32, day: u32, start_year: i32) -> i64 {
    let target = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let start = NaiveDate::from_ymd_opt(start_year, 1, 1).unwrap();
    (target - start).num_days()
}

fn assert_datetime_contains(value: &Value, date: &str, time: Option<&str>) {
    match value {
        Value::Ext("DateTime", inner) => {
            let text = inner
                .as_str()
                .unwrap_or_else(|| panic!("DateTime payload should be a string, got {:?}", inner));
            assert!(
                text.contains(date),
                "expected DateTime to contain {:?}, got {:?}",
                date,
                text
            );
            if let Some(time) = time {
                assert!(
                    text.contains(time),
                    "expected DateTime to contain {:?}, got {:?}",
                    time,
                    text
                );
            }
        }
        other => panic!("expected DateTime extension, got {:?}", other),
    }
}

#[test]
fn decode_all_null_column_data_variants() {
    let variants = [
        ColumnData::U8(None),
        ColumnData::I16(None),
        ColumnData::I32(None),
        ColumnData::I64(None),
        ColumnData::F32(None),
        ColumnData::F64(None),
        ColumnData::Bit(None),
        ColumnData::String(None),
        ColumnData::Guid(None),
        ColumnData::Binary(None),
        ColumnData::Numeric(None),
        ColumnData::Xml(None),
        ColumnData::DateTime(None),
        ColumnData::SmallDateTime(None),
        ColumnData::Time(None),
        ColumnData::Date(None),
        ColumnData::DateTime2(None),
        ColumnData::DateTimeOffset(None),
    ];
    for variant in variants {
        assert_eq!(
            <Value as Decode>::decode(&variant).unwrap(),
            Value::Null,
            "variant {:?} should decode to null",
            variant
        );
    }
}

#[test]
fn decode_scalar_some_values() {
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::U8(Some(255))).unwrap(),
        Value::U32(255)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I16(Some(-1234))).unwrap(),
        Value::I32(-1234)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I32(Some(-123_456))).unwrap(),
        Value::I32(-123_456)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::I64(Some(-9_876_543_210))).unwrap(),
        Value::I64(-9_876_543_210)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::F32(Some(1.5))).unwrap(),
        Value::F32(1.5)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::F64(Some(-2.25))).unwrap(),
        Value::F64(-2.25)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Bit(Some(true))).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::String(Some(Cow::Borrowed("hello")))).unwrap(),
        Value::String("hello".to_string())
    );
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Binary(Some(Cow::Borrowed(b"abc")))).unwrap(),
        Value::Binary(b"abc".to_vec())
    );
}

#[test]
fn decode_guid_and_xml() {
    let uuid = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Guid(Some(uuid))).unwrap(),
        ext("Uuid", Value::String(uuid.to_string()))
    );

    let xml = XmlData::new("<root>ok</root>");
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Xml(Some(Cow::Owned(xml)))).unwrap(),
        ext("Xml", Value::String("<root>ok</root>".to_string()))
    );
}

#[test]
fn decode_numeric() {
    let value = Numeric::new_with_scale(12345, 2);
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Numeric(Some(value))).unwrap(),
        ext("Decimal", Value::String("123.45".to_string()))
    );

    let negative = Numeric::new_with_scale(-12345, 2);
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Numeric(Some(negative))).unwrap(),
        ext("Decimal", Value::String("-123.45".to_string()))
    );
}

#[test]
fn decode_datetime_and_smalldatetime() {
    let days = days_between(2020, 1, 2, 1900) as i32;
    let fragments = ((12 * 3600 + 34 * 60 + 56) * 300) as u32;
    let decoded = <Value as Decode>::decode(&ColumnData::DateTime(Some(TiberiusDateTime::new(
        days,
        fragments,
    ))))
    .unwrap();
    assert_datetime_contains(&decoded, "2020-01-02", Some("12:34:56"));

    // SmallDateTime counts minutes in `seconds_fragments`.
    let decoded = <Value as Decode>::decode(&ColumnData::SmallDateTime(Some(SmallDateTime::new(
        days as u16,
        (12 * 60 + 34) as u16,
    ))))
    .unwrap();
    assert_datetime_contains(&decoded, "2020-01-02", Some("12:34:00"));
}

#[test]
fn decode_time_date_and_datetime2() {
    let time = Time::new((12 * 3600 + 34 * 60 + 56) as u64 * 10_000_000, 7);
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Time(Some(time))).unwrap(),
        ext("Time", Value::String("12:34:56".to_string()))
    );

    let days = days_between(2020, 1, 2, 1) as u32;
    assert_eq!(
        <Value as Decode>::decode(&ColumnData::Date(Some(Date::new(days)))).unwrap(),
        ext("Date", Value::String("2020-01-02".to_string()))
    );

    let datetime2 = DateTime2::new(Date::new(days), time);
    let decoded = <Value as Decode>::decode(&ColumnData::DateTime2(Some(datetime2))).unwrap();
    assert_datetime_contains(&decoded, "2020-01-02", Some("12:34:56"));
}

#[test]
fn decode_datetimeoffset() {
    let days = days_between(2020, 1, 2, 1) as u32;
    let time = Time::new((12 * 3600 + 34 * 60 + 56) as u64 * 10_000_000, 7);
    let datetime2 = DateTime2::new(Date::new(days), time);

    let decoded = <Value as Decode>::decode(&ColumnData::DateTimeOffset(Some(
        DateTimeOffset::new(datetime2, 0),
    )))
    .unwrap();
    assert_datetime_contains(&decoded, "2020-01-02", Some("12:34:56"));

    // Non-zero offsets still produce a DateTime extension value.
    let decoded = <Value as Decode>::decode(&ColumnData::DateTimeOffset(Some(
        DateTimeOffset::new(datetime2, 120),
    )))
    .unwrap();
    assert!(matches!(decoded, Value::Ext("DateTime", _)));
}

#[test]
fn decode_far_future_dates_use_second_precision_fallback() {
    let days_1900 = days_between(2500, 12, 31, 1900) as i32;
    let fragments = 1u32;
    let decoded = <Value as Decode>::decode(&ColumnData::DateTime(Some(TiberiusDateTime::new(
        days_1900,
        fragments,
    ))))
    .unwrap();
    assert_datetime_contains(&decoded, "2500", None);

    // SmallDateTime stores days as u16 (max ~2079), so the extreme-date
    // fallback is impossible here; use the largest representable day instead.
    let max_days = u16::MAX;
    let expected = (NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()
        + chrono::Duration::days(max_days as i64))
    .to_string();
    let decoded = <Value as Decode>::decode(&ColumnData::SmallDateTime(Some(SmallDateTime::new(
        max_days,
        0,
    ))))
    .unwrap();
    assert_datetime_contains(&decoded, &expected, None);

    let days_year_one = days_between(2500, 12, 31, 1) as u32;
    let datetime2 = DateTime2::new(Date::new(days_year_one), Time::new(0, 7));
    let decoded = <Value as Decode>::decode(&ColumnData::DateTime2(Some(datetime2))).unwrap();
    assert_datetime_contains(&decoded, "2500", None);

    let decoded = <Value as Decode>::decode(&ColumnData::DateTimeOffset(Some(
        DateTimeOffset::new(datetime2, 0),
    )))
    .unwrap();
    assert_datetime_contains(&decoded, "2500", None);
}

#[test]
fn native_datetime_conversion_covers_nanosecond_and_fallback_paths() {
    // Nanosecond-representable date (Some branch).
    let recent = NaiveDate::from_ymd_opt(2020, 1, 2)
        .unwrap()
        .and_hms_opt(12, 34, 56)
        .unwrap();
    let converted = <fastdate::DateTime as DateTimeFromNativeDatetime>::from(recent);
    assert!(converted.to_string().contains("2020-01-02"));

    // Beyond the nanosecond range (None branch).
    let far = NaiveDate::from_ymd_opt(2500, 12, 31)
        .unwrap()
        .and_hms_opt(23, 59, 59)
        .unwrap();
    let converted = <fastdate::DateTime as DateTimeFromNativeDatetime>::from(far);
    assert!(converted.to_string().contains("2500"));

    // Also exercise the chrono `NaiveDateTime` construction directly.
    let direct = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1999, 12, 31).unwrap(),
        NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
    );
    assert!(<fastdate::DateTime as DateTimeFromNativeDatetime>::from(direct)
        .to_string()
        .contains("1999"));
}

#[test]
fn fixed_offset_conversion_covers_nanosecond_and_fallback_paths() {
    use chrono::FixedOffset;

    let offset = FixedOffset::east_opt(8 * 3600).unwrap();
    let recent = NaiveDate::from_ymd_opt(2020, 1, 2)
        .unwrap()
        .and_hms_opt(12, 34, 56)
        .unwrap();
    let converted =
        <fastdate::DateTime as DateTimeFromDateTimeFixedOffset>::from(
            chrono::DateTime::from_naive_utc_and_offset(recent, offset),
        );
    assert!(converted.to_string().contains("2020-01-02"));

    // Mid-year keeps the offset shift inside the same calendar year.
    let far = NaiveDate::from_ymd_opt(2500, 6, 15)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let converted =
        <fastdate::DateTime as DateTimeFromDateTimeFixedOffset>::from(
            chrono::DateTime::from_naive_utc_and_offset(far, offset),
        );
    assert!(converted.to_string().contains("2500"));
}
