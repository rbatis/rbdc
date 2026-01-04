use crate::connection::{sasl, stream::PgStream, tls, PgConnection};
use crate::message::{
    Authentication, BackendKeyData, MessageFormat, Password, ReadyForQuery, Startup,
};
use crate::options::PgConnectOptions;
use crate::types::Oid;
use rbdc::common::StatementCache;
use rbdc::io::Decode;
use rbdc::{err_protocol, Error};
use std::collections::HashMap;

/// Converts timezone offset in seconds to PostgreSQL timezone format.
/// e.g., 28800 (UTC+08:00) -> "-08:00", -14400 (UTC-05:00) -> "+05:00", 0 -> "UTC"
/// Note: PostgreSQL uses POSIX-style signs where "+" means west of UTC.
fn format_timezone_offset(offset_sec: i32) -> String {
    if offset_sec == 0 {
        return "UTC".to_string();
    }

    // PostgreSQL uses POSIX-style timezone: + means west (behind UTC), - means east (ahead of UTC)
    // So for UTC+08:00 (28800 seconds), we need "-08:00"
    let sign = if offset_sec < 0 { '+' } else { '-' };
    let abs_sec = offset_sec.abs();
    let hours = abs_sec / 3600;
    let minutes = (abs_sec % 3600) / 60;

    format!("{}{:02}:{:02}", sign, hours, minutes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_timezone_offset() {
        assert_eq!(format_timezone_offset(0), "UTC");
        // UTC+08:00 -> PostgreSQL "-08:00" (east of UTC)
        assert_eq!(format_timezone_offset(28800), "-08:00");
        // UTC-05:00 -> PostgreSQL "+05:00" (west of UTC)
        assert_eq!(format_timezone_offset(-18000), "+05:00");
        // UTC+01:00 -> "-01:00"
        assert_eq!(format_timezone_offset(3600), "-01:00");
        // UTC-00:30 -> "+00:30"
        assert_eq!(format_timezone_offset(-1800), "+00:30");
    }
}

// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3
// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11

impl PgConnection {
    pub async fn establish(options: &PgConnectOptions) -> Result<Self, Error> {
        let mut stream = PgStream::connect(options).await?;

        // Upgrade to TLS if we were asked to and the server supports it
        tls::maybe_upgrade(&mut stream, options).await?;

        // To begin a session, a frontend opens a connection to the server
        // and sends a startup message.

        let timezone = options
            .timezone_sec
            .map(|sec| format_timezone_offset(sec))
            .unwrap_or_else(|| "UTC".to_string());

        let mut params = vec![
            // Sets the display format for date and time values,
            // as well as the rules for interpreting ambiguous date input values.
            ("DateStyle", "ISO, MDY"),
            // Sets the client-side encoding (character set).
            // <https://www.postgresql.org/docs/devel/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED>
            ("client_encoding", "UTF8"),
            // Sets the time zone for displaying and interpreting time stamps.
            ("TimeZone", timezone.as_str()),
        ];

        if let Some(ref extra_float_digits) = options.extra_float_digits {
            params.push(("extra_float_digits", extra_float_digits));
        }

        if let Some(ref application_name) = options.application_name {
            params.push(("application_name", application_name));
        }

        if let Some(ref options) = options.options {
            params.push(("options", options));
        }

        stream
            .send(Startup {
                username: Some(&options.username),
                database: options.database.as_deref(),
                params: &params,
            })
            .await?;

        // The server then uses this information and the contents of
        // its configuration files (such as pg_hba.conf) to determine whether the connection is
        // provisionally acceptable, and what additional
        // authentication is required (if any).

        let mut process_id = 0;
        let mut secret_key = 0;
        let transaction_status;

        loop {
            let message = stream.recv().await?;
            match message.format {
                MessageFormat::Authentication => match message.decode()? {
                    Authentication::Ok => {
                        // the authentication exchange is successfully completed
                        // do nothing; no more information is required to continue
                    }

                    Authentication::CleartextPassword => {
                        // The frontend must now send a [PasswordMessage] containing the
                        // password in clear-text form.

                        stream
                            .send(Password::Cleartext(
                                options.password.as_deref().unwrap_or_default(),
                            ))
                            .await?;
                    }

                    Authentication::Md5Password(body) => {
                        // The frontend must now send a [PasswordMessage] containing the
                        // password (with user name) encrypted via MD5, then encrypted again
                        // using the 4-byte random salt specified in the
                        // [AuthenticationMD5Password] message.

                        stream
                            .send(Password::Md5 {
                                username: &options.username,
                                password: options.password.as_deref().unwrap_or_default(),
                                salt: body.salt,
                            })
                            .await?;
                    }

                    Authentication::Sasl(body) => {
                        sasl::authenticate(&mut stream, options, body).await?;
                    }

                    method => {
                        return Err(err_protocol!(
                            "unsupported authentication method: {:?}",
                            method
                        ));
                    }
                },

                MessageFormat::BackendKeyData => {
                    // provides secret-key data that the frontend must save if it wants to be
                    // able to issue cancel requests later

                    let data: BackendKeyData = message.decode()?;

                    process_id = data.process_id;
                    secret_key = data.secret_key;
                }

                MessageFormat::ReadyForQuery => {
                    // start-up is completed. The frontend can now issue commands
                    transaction_status =
                        ReadyForQuery::decode(message.contents)?.transaction_status;
                    break;
                }

                _ => {
                    return Err(err_protocol!(
                        "establish: unexpected message: {:?}",
                        message.format
                    ))
                }
            }
        }

        Ok(PgConnection {
            stream,
            process_id,
            secret_key,
            transaction_status,
            pending_ready_for_query_count: 0,
            next_statement_id: Oid(1),
            cache_statement: StatementCache::new(options.statement_cache_capacity),
            cache_type_oid: HashMap::with_capacity(10),
            cache_type_info: HashMap::with_capacity(10),
            timezone_sec: options.timezone_sec,
        })
    }
}
