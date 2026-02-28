mod connect;

use crate::error::TursoError;
use futures_core::future::BoxFuture;
use rbdc::db::{ConnectOptions, Connection};
use rbdc::Error;
use std::str::FromStr;

/// Options for connecting to a Turso/libSQL database.
///
/// Backend selection is determined at startup/initialization time only.
/// There is no support for runtime backend switching. Once a `TursoConnectOptions`
/// is constructed and used to open a connection, the backend is fixed for
/// the lifetime of that connection.
///
/// # URI Format
///
/// | URI | Description |
/// | -- | -- |
/// | `turso://hostname:port` | Connect to a remote Turso/libSQL server. |
/// | `turso://?url=libsql://host&token=TOKEN` | Connect with explicit URL and auth token. |
/// | `turso://:memory:` | Open an in-memory libSQL database (local, no network). |
/// | `turso://path/to/file.db` | Open a local libSQL database file. |
///
/// # Required Parameters
///
/// For remote Turso connections, `auth_token` must be provided either through
/// the `token` query parameter or via the builder API.
#[derive(Clone, Debug)]
pub struct TursoConnectOptions {
    /// The endpoint URL for the Turso/libSQL database.
    ///
    /// For remote: `libsql://your-db.turso.io`
    /// For local file: a filesystem path
    /// For in-memory: `:memory:`
    pub(crate) url: String,

    /// Authentication token for remote Turso databases.
    /// Required for remote connections, ignored for local/in-memory.
    pub(crate) auth_token: Option<String>,

    /// Whether this is an in-memory database.
    pub(crate) in_memory: bool,
}

impl Default for TursoConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl TursoConnectOptions {
    /// Construct default options pointing to an in-memory database.
    pub fn new() -> Self {
        Self {
            url: ":memory:".to_string(),
            auth_token: None,
            in_memory: true,
        }
    }

    /// Set the Turso/libSQL endpoint URL.
    ///
    /// For remote Turso databases, use `libsql://your-db.turso.io`.
    /// For local file databases, use a filesystem path.
    /// For in-memory databases, use `:memory:`.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        let u: String = url.into();
        self.in_memory = u == ":memory:";
        self.url = u;
        self
    }

    /// Set the authentication token for remote Turso databases.
    ///
    /// This is required for remote connections and will produce an error
    /// at connect time if missing for a remote URL.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Returns whether this configuration targets an in-memory database.
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    /// Returns whether this configuration targets a remote Turso endpoint.
    pub fn is_remote(&self) -> bool {
        self.url.starts_with("libsql://")
            || self.url.starts_with("https://")
            || self.url.starts_with("http://")
    }

    /// Validate the options, returning an error if the configuration is incomplete
    /// or contradictory.
    ///
    /// This enforces startup-only activation semantics: options must be fully
    /// valid before any connection is established.
    pub fn validate(&self) -> Result<(), TursoError> {
        if self.url.is_empty() {
            return Err(TursoError::configuration(
                "Turso URL must not be empty. Provide a valid libsql:// URL, file path, or :memory:",
            ));
        }

        // Remote connections require an auth token
        if self.is_remote() && self.auth_token.is_none() {
            return Err(TursoError::configuration(
                "auth_token is required for remote Turso connections (libsql:// or https:// URLs)",
            ));
        }

        Ok(())
    }
}

impl FromStr for TursoConnectOptions {
    type Err = Error;

    fn from_str(uri: &str) -> Result<Self, Self::Err> {
        let mut options = Self::new();

        // Strip scheme prefix
        let rest = uri
            .trim_start_matches("turso://")
            .trim_start_matches("turso:");

        // Check for in-memory
        if rest == ":memory:" || rest.is_empty() {
            options.in_memory = true;
            options.url = ":memory:".to_string();
            return Ok(options);
        }

        // Split path from query parameters
        let mut parts = rest.splitn(2, '?');
        let path_part = parts.next().unwrap_or_default();
        let query_part = parts.next();

        // Parse query parameters
        let mut explicit_url: Option<String> = None;
        let mut token: Option<String> = None;

        if let Some(params) = query_part {
            for (key, value) in url::form_urlencoded::parse(params.as_bytes()) {
                match &*key {
                    "url" => {
                        explicit_url = Some(value.into_owned());
                    }
                    "token" => {
                        token = Some(value.into_owned());
                    }
                    _ => {
                        return Err(Error::from(format!(
                            "turso configuration: unknown query parameter `{}`",
                            key
                        )));
                    }
                }
            }
        }

        // Determine the actual URL
        if let Some(url) = explicit_url {
            options.url = url;
        } else if !path_part.is_empty() {
            options.url = path_part.to_string();
        } else {
            return Err(Error::from(
                "turso configuration: no database URL or path provided",
            ));
        }

        options.in_memory = options.url == ":memory:";
        options.auth_token = token;

        Ok(options)
    }
}

impl ConnectOptions for TursoConnectOptions {
    fn connect(&self) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
        Box::pin(async move {
            let conn = self.connect_turso().await?;
            Ok(Box::new(conn) as Box<dyn Connection>)
        })
    }

    fn set_uri(&mut self, uri: &str) -> Result<(), Error> {
        *self = TursoConnectOptions::from_str(uri).map_err(|e| Error::from(e.to_string()))?;
        Ok(())
    }
}
