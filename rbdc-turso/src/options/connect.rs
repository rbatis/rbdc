use crate::connection::TursoConnection;
use crate::error::TursoError;
use crate::TursoConnectOptions;
use rbdc::error::Error;

impl TursoConnectOptions {
    /// Establish a connection to the Turso database using these options.
    ///
    /// Validates all options before attempting the connection. This enforces
    /// startup-only activation: the configuration must be complete and valid
    /// at initialization time.
    pub async fn connect_turso(&self) -> Result<TursoConnection, Error> {
        self.validate()?;

        let db = if self.in_memory {
            log::info!("turso: connecting to in-memory database");
            libsql::Builder::new_local(":memory:")
                .build()
                .await
                .map_err(|e| {
                    log::error!("turso: failed to create in-memory database: {}", e);
                    TursoError::from(e)
                })?
        } else if self.is_remote() {
            log::info!("turso: connecting to remote database at {}", self.url);
            let builder = libsql::Builder::new_remote(
                self.url.clone(),
                self.auth_token.clone().unwrap_or_default(),
            );
            builder.build().await.map_err(|e| {
                log::error!(
                    "turso: failed to connect to remote database {}: {}",
                    self.url,
                    e
                );
                TursoError::from(e)
            })?
        } else {
            log::info!("turso: connecting to local database at {}", self.url);
            libsql::Builder::new_local(&self.url)
                .build()
                .await
                .map_err(|e| {
                    log::error!(
                        "turso: failed to open local database {}: {}",
                        self.url,
                        e
                    );
                    TursoError::from(e)
                })?
        };

        let conn = db.connect().map_err(|e| {
            log::error!("turso: failed to obtain connection handle: {}", e);
            TursoError::from(e)
        })?;

        log::debug!("turso: connection established successfully");
        Ok(TursoConnection {
            db,
            conn,
            json_detect: self.json_detect,
        })
    }
}
