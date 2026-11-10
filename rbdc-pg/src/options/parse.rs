use crate::options::PgConnectOptions;
use percent_encoding::percent_decode_str;
use rbdc::error::Error;
use std::net::IpAddr;
use std::num::ParseIntError;
use std::str::FromStr;
use url::Url;

impl FromStr for PgConnectOptions {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let url: Url = s
            .parse()
            .map_err(|e: url::ParseError| Error::from(e.to_string()))?;

        let mut options = Self::new_without_pgpass();

        if let Some(host) = url.host_str() {
            let host_decoded = percent_decode_str(host);
            options = match host_decoded.clone().next() {
                Some(b'/') => options.socket(
                    &*host_decoded
                        .decode_utf8()
                        .map_err(|e| Error::from(e.to_string()))?,
                ),
                _ => options.host(host),
            }
        }

        if let Some(port) = url.port() {
            options = options.port(port);
        }

        let username = url.username();
        if !username.is_empty() {
            options = options.username(
                &*percent_decode_str(username)
                    .decode_utf8()
                    .map_err(|e| Error::from(e.to_string()))?,
            );
        }

        if let Some(password) = url.password() {
            options = options.password(
                &*percent_decode_str(password)
                    .decode_utf8()
                    .map_err(|e| Error::from(e.to_string()))?,
            );
        }

        let path = url.path().trim_start_matches('/');
        if !path.is_empty() {
            options = options.database(path);
        }

        for (key, value) in url.query_pairs().into_iter() {
            match &*key {
                "sslmode" | "ssl-mode" => {
                    options = options.ssl_mode(value.parse()?);
                }

                "sslrootcert" | "ssl-root-cert" | "ssl-ca" => {
                    options = options.ssl_root_cert(&*value);
                }

                "statement-cache-capacity" => {
                    options = options.statement_cache_capacity(
                        value
                            .parse()
                            .map_err(|e: ParseIntError| Error::from(e.to_string()))?,
                    );
                }

                "host" => {
                    if value.starts_with("/") {
                        options = options.socket(&*value);
                    } else {
                        options = options.host(&*value);
                    }
                }

                "hostaddr" => {
                    value
                        .parse::<IpAddr>()
                        .map_err(|e| Error::from(e.to_string()))?;
                    options = options.host(&*value)
                }

                "port" => {
                    options = options.port(
                        value
                            .parse()
                            .map_err(|e: ParseIntError| Error::from(e.to_string()))?,
                    )
                }

                "dbname" => options = options.database(&*value),

                "user" => options = options.username(&*value),

                "password" => options = options.password(&*value),

                "application_name" => options = options.application_name(&*value),

                "timezone_sec" => {
                    options = options.timezone_sec(
                        value
                            .parse()
                            .map_err(|e: ParseIntError| Error::from(e.to_string()))?,
                    )
                }

                "options" => {
                    if let Some(options) = options.options.as_mut() {
                        options.push(' ');
                        options.push_str(&*value);
                    } else {
                        options.options = Some(value.to_string());
                    }
                }

                k if k.starts_with("options[") => {
                    if let Some(key) = k
                        .strip_prefix("options[")
                        .unwrap_or_default()
                        .strip_suffix(']')
                    {
                        options = options.options([(key, &*value)]);
                    }
                }

                _ => log::warn!("ignoring unrecognized connect parameter: {}={}", key, value),
            }
        }

        let options = options.apply_pgpass();

        Ok(options)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::options::{PgConnectOptions, PgSslMode};

    #[test]
    fn it_parses_socket_correctly_from_parameter() {
        let uri = "postgres:///?host=/var/run/postgres/";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(Some("/var/run/postgres/".into()), opts.socket);
    }

    #[test]
    fn it_parses_host_correctly_from_parameter() {
        let uri = "postgres:///?host=google.database.com";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!("google.database.com", &opts.host);
    }

    #[test]
    fn it_parses_hostaddr_correctly_from_parameter() {
        let uri = "postgres:///?hostaddr=8.8.8.8";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!("8.8.8.8", &opts.host);
    }

    #[test]
    fn it_parses_port_correctly_from_parameter() {
        let uri = "postgres:///?port=1234";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!(1234, opts.port);
    }

    #[test]
    fn it_parses_dbname_correctly_from_parameter() {
        let uri = "postgres:///?dbname=some_db";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!(Some("some_db"), opts.database.as_deref());
    }

    #[test]
    fn it_parses_user_correctly_from_parameter() {
        let uri = "postgres:///?user=some_user";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!("some_user", opts.username);
    }

    #[test]
    fn it_parses_password_correctly_from_parameter() {
        let uri = "postgres:///?password=some_pass";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(None, opts.socket);
        assert_eq!(Some("some_pass"), opts.password.as_deref());
    }

    #[test]
    fn it_parses_application_name_correctly_from_parameter() {
        let uri = "postgres:///?application_name=some_name";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(Some("some_name"), opts.application_name.as_deref());
    }

    #[test]
    fn it_parses_username_with_at_sign_correctly() {
        let uri = "postgres://user@hostname:password@hostname:5432/database";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!("user@hostname", &opts.username);
    }

    #[test]
    fn it_parses_password_with_non_ascii_chars_correctly() {
        let uri = "postgres://username:p@ssw0rd@hostname:5432/database";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(Some("p@ssw0rd".into()), opts.password);
    }

    #[test]
    fn it_parses_socket_correctly_percent_encoded() {
        let uri = "postgres://%2Fvar%2Flib%2Fpostgres/database";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(Some("/var/lib/postgres/".into()), opts.socket);
    }
    #[test]
    fn it_parses_socket_correctly_with_username_percent_encoded() {
        let uri = "postgres://some_user@%2Fvar%2Flib%2Fpostgres/database";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!("some_user", opts.username);
        assert_eq!(Some("/var/lib/postgres/".into()), opts.socket);
        assert_eq!(Some("database"), opts.database.as_deref());
    }
    #[test]
    fn it_parses_libpq_options_correctly() {
        let uri = "postgres:///?options=-c%20synchronous_commit%3Doff%20--search_path%3Dpostgres";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(
            Some("-c synchronous_commit=off --search_path=postgres".into()),
            opts.options
        );
    }
    #[test]
    fn it_parses_rbdc_options_correctly() {
        let uri = "postgres:///?options[synchronous_commit]=off&options[search_path]=postgres";
        let opts = PgConnectOptions::from_str(uri).unwrap();

        assert_eq!(
            Some("-c synchronous_commit=off -c search_path=postgres".into()),
            opts.options
        );
    }

    #[test]
    fn it_parses_ssl_mode() {
        let uri = "postgres:///?port=1234";
        let opts = PgConnectOptions::from_str(uri).unwrap();
        assert_eq!(PgSslMode::Disable, opts.ssl_mode);
    }

    #[test]
    fn it_parses_timezone_sec() {
        let uri = "postgres:///?timezone_sec=28800";
        let opts = PgConnectOptions::from_str(uri).unwrap();
        assert_eq!(Some(28800), opts.timezone_sec);
    }

    #[test]
    fn it_parses_timezone_sec_negative() {
        let uri = "postgres:///?timezone_sec=-14400";
        let opts = PgConnectOptions::from_str(uri).unwrap();
        assert_eq!(Some(-14400), opts.timezone_sec);
    }

    #[test]
    fn it_rejects_invalid_uris() {
        assert!(PgConnectOptions::from_str("not a url").is_err());
        assert!(PgConnectOptions::from_str("").is_err());
        // invalid UTF-8 after percent decoding
        assert!(PgConnectOptions::from_str("postgres://%ff@localhost/db").is_err());
        assert!(PgConnectOptions::from_str("postgres://user:%ff@localhost/db").is_err());
    }

    #[test]
    fn it_parses_all_ssl_modes() {
        for (raw, expected) in [
            ("disable", PgSslMode::Disable),
            ("allow", PgSslMode::Allow),
            ("prefer", PgSslMode::Prefer),
            ("require", PgSslMode::Require),
            ("verify-ca", PgSslMode::VerifyCa),
            ("verify-full", PgSslMode::VerifyFull),
            ("VERIFY-FULL", PgSslMode::VerifyFull),
        ] {
            let opts = PgConnectOptions::from_str(&format!("postgres:///?sslmode={raw}")).unwrap();
            assert_eq!(opts.ssl_mode, expected, "sslmode={raw}");
        }
        // alias spelling
        let opts = PgConnectOptions::from_str("postgres:///?ssl-mode=require").unwrap();
        assert_eq!(opts.ssl_mode, PgSslMode::Require);
        assert!(PgConnectOptions::from_str("postgres:///?sslmode=bogus").is_err());
        assert!(PgSslMode::from_str("bogus").is_err());
    }

    #[test]
    fn it_parses_ssl_root_cert_aliases() {
        for key in ["sslrootcert", "ssl-root-cert", "ssl-ca"] {
            let opts =
                PgConnectOptions::from_str(&format!("postgres:///?{key}=/tmp/ca.pem")).unwrap();
            assert!(opts.ssl_root_cert.is_some(), "key={key}");
        }
    }

    #[test]
    fn it_parses_statement_cache_capacity() {
        let opts =
            PgConnectOptions::from_str("postgres:///?statement-cache-capacity=7").unwrap();
        assert_eq!(opts.statement_cache_capacity, 7);
        assert!(
            PgConnectOptions::from_str("postgres:///?statement-cache-capacity=x").is_err()
        );
    }

    #[test]
    fn it_parses_host_socket_and_invalid_hostaddr() {
        let opts = PgConnectOptions::from_str("postgres:///?host=/var/run/postgresql").unwrap();
        assert_eq!(
            Some(std::path::PathBuf::from("/var/run/postgresql")),
            opts.socket
        );

        assert!(PgConnectOptions::from_str("postgres:///?hostaddr=999.1.1.1").is_err());
        let opts = PgConnectOptions::from_str("postgres:///?hostaddr=::1").unwrap();
        assert_eq!("::1", opts.host);
    }

    #[test]
    fn it_rejects_invalid_numeric_parameters() {
        assert!(PgConnectOptions::from_str("postgres:///?port=not-a-port").is_err());
        assert!(PgConnectOptions::from_str("postgres:///?timezone_sec=not-a-number").is_err());
    }

    #[test]
    fn it_appends_options_and_ignores_unknown_parameters() {
        let opts = PgConnectOptions::from_str(
            "postgres:///?options=-c%20a%3D1&options=-c%20b%3D2&unknown=value",
        )
        .unwrap();
        assert_eq!(opts.options.as_deref(), Some("-c a=1 -c b=2"));

        // malformed options[...] key is ignored rather than erroring
        let opts = PgConnectOptions::from_str("postgres:///?options[foo=1").unwrap();
        assert_eq!(opts.options, None);
    }

    #[test]
    fn it_percent_decodes_credentials_and_database() {
        let opts = PgConnectOptions::from_str(
            "postgres://us%65r:p%40ss@localhost/mydb",
        )
        .unwrap();
        assert_eq!("user", opts.username);
        assert_eq!(Some("p@ss".to_string()), opts.password);
        assert_eq!(Some("mydb".to_string()), opts.database);
    }

    #[test]
    fn it_applies_builder_overrides_from_query() {
        let opts = PgConnectOptions::from_str(
            "postgres://user:pass@localhost:5433/mydb?user=other&password=secret&dbname=otherdb&port=5434",
        )
        .unwrap();
        assert_eq!("other", opts.username);
        assert_eq!(Some("secret".to_string()), opts.password);
        assert_eq!(Some("otherdb".to_string()), opts.database);
        assert_eq!(5434, opts.port);
    }

    #[test]
    fn connect_options_set_uri_replaces_options() {
        use rbdc::db::ConnectOptions;
        let mut opts = PgConnectOptions::new();
        opts.set_uri("postgres://user:pw@localhost:5444/db").unwrap();
        assert_eq!("user", opts.username);
        assert_eq!(Some("pw".to_string()), opts.password);
        assert_eq!(Some("db".to_string()), opts.database);
        assert_eq!(5444, opts.port);
        assert!(opts.set_uri("not a url").is_err());
    }
}
