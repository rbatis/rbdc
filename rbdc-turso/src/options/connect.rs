use crate::connection::TursoConnection;
use crate::error::TursoError;
use crate::TursoConnectOptions;
use rbdc::error::Error;

impl TursoConnectOptions {
    /// Establish a connection to the Turso/libSQL database using these options.
    ///
    /// Validates all options before attempting the connection. This enforces
    /// startup-only activation: the configuration must be complete and valid
    /// at initialization time.
    pub async fn connect_turso(&self) -> Result<TursoConnection, Error> {
        // Validate options before any connection attempt.
        // This ensures startup-only activation semantics: incomplete or invalid
        // configuration fails immediately rather than partially activating.
        self.validate()?;

        let db = if self.in_memory {
            let db = libsql::Builder::new_local(":memory:")
                .build()
                .await
                .map_err(|e| TursoError::from(e))?;
            db
        } else if self.is_remote() {
            let builder =
                libsql::Builder::new_remote(self.url.clone(), self.auth_token.clone().unwrap_or_default());
            let db = builder.build().await.map_err(|e| TursoError::from(e))?;
            db
        } else {
            // Local file database
            let db = libsql::Builder::new_local(&self.url)
                .build()
                .await
                .map_err(|e| TursoError::from(e))?;
            db
        };

        let conn = db.connect().map_err(|e| TursoError::from(e))?;

        Ok(TursoConnection { db, conn })
    }
}
