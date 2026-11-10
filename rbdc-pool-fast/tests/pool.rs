//! Integration tests for rbdc-pool-fast covering construction, configuration,
//! connection acquisition, connection reuse and error paths.
//!
//! All tests use in-process mocks: no network or database server is required.

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use rbdc::db::{ConnectOptions, Connection, Driver, ExecResult, Row};
use rbdc::pool::{ConnectionManager, Pool};
use rbdc::try_stream;
use rbdc_pool_fast::{ConnManagerProxy, FastPool};
use rbs::Value;
use std::pin::Pin;
use std::time::Duration;

mod mock {
    use super::*;

    #[derive(Debug)]
    pub struct Opt {
        pub uri_error: bool,
    }

    impl ConnectOptions for Opt {
        fn connect(&self) -> BoxFuture<'_, Result<Box<dyn Connection>, rbdc::Error>> {
            Box::pin(async { Ok(Box::new(Conn { ping_ok: true }) as Box<dyn Connection>) })
        }

        fn set_uri(&mut self, _uri: &str) -> Result<(), rbdc::Error> {
            if self.uri_error {
                Err(rbdc::Error::from("invalid uri"))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Debug)]
    pub struct Conn {
        pub ping_ok: bool,
    }

    impl Connection for Conn {
        fn exec_rows(
            &mut self,
            _sql: &str,
            _params: Vec<Value>,
        ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, rbdc::Error>>, rbdc::Error>>
        {
            Box::pin(async move {
                let rows: Vec<Box<dyn Row>> = vec![];
                let stream = try_stream! {
                    for row in rows {
                        r#yield!(row);
                    }
                    Ok(())
                };
                let stream: Pin<
                    Box<dyn futures_core::Stream<Item = Result<Box<dyn Row>, rbdc::Error>> + Send>,
                > = Box::pin(stream);
                Ok(stream)
            })
        }

        fn exec(
            &mut self,
            _sql: &str,
            _params: Vec<Value>,
        ) -> BoxFuture<'_, Result<ExecResult, rbdc::Error>> {
            Box::pin(async { Ok(ExecResult::default()) })
        }

        fn ping(&mut self) -> BoxFuture<'_, Result<(), rbdc::Error>> {
            let ok = self.ping_ok;
            Box::pin(async move {
                if ok {
                    Ok(())
                } else {
                    Err(rbdc::Error::from("ping failed"))
                }
            })
        }

        fn close(&mut self) -> BoxFuture<'_, Result<(), rbdc::Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct D {
        pub connect_error: bool,
        pub ping_ok: bool,
        pub uri_error: bool,
    }

    impl D {
        pub fn ok() -> Self {
            Self {
                connect_error: false,
                ping_ok: true,
                uri_error: false,
            }
        }
    }

    impl Driver for D {
        fn name(&self) -> &str {
            "mock"
        }

        fn connect(&self, _url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, rbdc::Error>> {
            let this = *self;
            Box::pin(async move { this.make_conn() })
        }

        fn connect_opt<'a>(
            &'a self,
            _opt: &'a dyn ConnectOptions,
        ) -> BoxFuture<'a, Result<Box<dyn Connection>, rbdc::Error>> {
            let this = *self;
            Box::pin(async move { this.make_conn() })
        }

        fn default_option(&self) -> Box<dyn ConnectOptions> {
            Box::new(Opt {
                uri_error: self.uri_error,
            })
        }
    }

    impl D {
        fn make_conn(&self) -> Result<Box<dyn Connection>, rbdc::Error> {
            if self.connect_error {
                Err(rbdc::Error::from("connect failed"))
            } else {
                Ok(Box::new(Conn {
                    ping_ok: self.ping_ok,
                }) as Box<dyn Connection>)
            }
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn make_pool(driver: mock::D) -> FastPool {
    let manager = ConnectionManager::new(driver, "").expect("connection manager");
    FastPool::new(manager).expect("pool")
}

fn state_field(state: &Value, key: &str) -> Value {
    match state {
        Value::Map(m) => m.get(&Value::String(key.to_string())).clone(),
        other => panic!("expected map state, got {other:?}"),
    }
}

#[test]
fn test_new_url_success_and_uri_error() {
    let pool = FastPool::new_url(mock::D::ok(), "mock://url").expect("new_url");
    assert_eq!(pool.driver_type(), "mock");

    let err = FastPool::new_url(
        mock::D {
            uri_error: true,
            ..mock::D::ok()
        },
        "bad://url",
    )
    .expect_err("invalid uri must fail");
    assert!(err.to_string().contains("invalid uri"));

    // new_option never parses a URI, so it succeeds even with a uri_error option.
    let pool = FastPool::new_option(mock::D::ok(), mock::Opt { uri_error: true })
        .expect("new_option");
    assert_eq!(pool.driver_type(), "mock");
}

#[test]
fn test_driver_accessors() {
    let pool = make_pool(mock::D::ok());
    assert_eq!(pool.driver_type(), "mock");
    assert_eq!(pool.driver().name(), "mock");
}

#[test]
fn test_get_and_get_timeout() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    let conn = rt.block_on(pool.get());
    assert!(conn.is_ok());

    let conn = rt.block_on(pool.get_timeout(Duration::from_secs(1)));
    assert!(conn.is_ok());

    // Zero timeout is interpreted as "use the pool's default wait timeout".
    let conn = rt.block_on(pool.get_timeout(Duration::ZERO));
    assert!(conn.is_ok());
}

#[test]
fn test_zero_timeout_returns_error_when_exhausted() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());
    rt.block_on(pool.set_max_open_conns(1));

    let held = rt.block_on(pool.get()).expect("first connection");
    let state = rt.block_on(pool.state());
    assert_eq!(state_field(&state, "max_open"), Value::U64(1));
    assert_eq!(state_field(&state, "in_use"), Value::U64(1));

    let err = match rt.block_on(pool.get_timeout(Duration::ZERO)) {
        Err(e) => e,
        Ok(_) => panic!("pool should be exhausted"),
    };
    assert!(err.to_string().contains("Time out"), "got: {err}");

    // Releasing the connection makes the pool usable again.
    drop(held);
    let conn = rt.block_on(pool.get_timeout(Duration::ZERO));
    assert!(conn.is_ok());
}

#[test]
fn test_failed_connect_propagates_error() {
    let rt = runtime();
    let pool = make_pool(mock::D {
        connect_error: true,
        ping_ok: true,
        uri_error: false,
    });

    let err = match rt.block_on(pool.get()) {
        Err(e) => e,
        Ok(_) => panic!("get should fail"),
    };
    assert!(err.to_string().contains("connect failed"), "got: {err}");

    let err = match rt.block_on(pool.get_timeout(Duration::from_millis(50))) {
        Err(e) => e,
        Ok(_) => panic!("get_timeout should fail"),
    };
    assert!(err.to_string().contains("connect failed"), "got: {err}");
}

#[test]
fn test_connection_operations_and_reuse() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    {
        let mut conn = rt.block_on(pool.get()).expect("connection");

        let result = rt.block_on(conn.exec("SELECT 1", vec![])).unwrap();
        assert_eq!(result.rows_affected, 0);

        let decoded = rt
            .block_on(conn.exec_decode("SELECT 1", vec![]))
            .expect("exec_decode");
        assert!(decoded.is_array());

        {
            let mut stream = rt
                .block_on(conn.exec_rows("SELECT 1", vec![]))
                .expect("exec_rows");
            assert!(rt.block_on(stream.next()).is_none());
        }

        rt.block_on(conn.ping()).expect("ping");
        rt.block_on(conn.begin()).expect("begin");
        rt.block_on(conn.commit()).expect("commit");
        rt.block_on(conn.rollback()).expect("rollback");
        rt.block_on(conn.close()).expect("close");

        let state = rt.block_on(pool.state());
        assert_eq!(state_field(&state, "in_use"), Value::U64(1));
    }

    // Dropping the proxy returns the connection to the pool.
    let state = rt.block_on(pool.state());
    assert_eq!(state_field(&state, "in_use"), Value::U64(0));
    assert_eq!(state_field(&state, "idle"), Value::U64(1));
    assert_eq!(state_field(&state, "connections"), Value::U64(1));

    // And it can be acquired again.
    let conn = rt.block_on(pool.get());
    assert!(conn.is_ok());
}

#[test]
fn test_set_timeout_and_timeouts_reflected() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    rt.block_on(pool.set_timeout(Some(Duration::from_secs(3))));
    assert_eq!(pool.inner.get_timeout_wait(), Some(Duration::from_secs(3)));

    rt.block_on(pool.set_timeout(None));
    assert_eq!(pool.inner.get_timeout_wait(), None);

    // The pool still works with the default timeout restored by fast_pool.
    assert!(rt.block_on(pool.get()).is_ok());
}

#[test]
fn test_set_conn_max_lifetime_modes() {
    use fast_pool::plugin::{CheckMode, DurationManager};

    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    rt.block_on(pool.set_conn_max_lifetime(Some(Duration::from_secs(7))));
    {
        let manager = pool
            .inner
            .downcast_manager::<DurationManager<ConnManagerProxy>>()
            .expect("duration manager present");
        assert!(matches!(
            manager.mode.get_mode(),
            CheckMode::MaxLifetime(d) if d == Duration::from_secs(7)
        ));
    }

    rt.block_on(pool.set_conn_max_lifetime(None));
    {
        let manager = pool
            .inner
            .downcast_manager::<DurationManager<ConnManagerProxy>>()
            .expect("duration manager present");
        assert!(matches!(manager.mode.get_mode(), CheckMode::NoLimit));
    }
}

#[test]
fn test_set_max_open_and_idle_conns() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    rt.block_on(pool.set_max_open_conns(5));
    assert_eq!(pool.inner.get_max_open(), 5);
    let state = rt.block_on(pool.state());
    assert_eq!(state_field(&state, "max_open"), Value::U64(5));

    rt.block_on(pool.set_max_idle_conns(2));
    assert_eq!(pool.inner.get_max_idle_conns(), 2);
}

#[test]
fn test_state_contains_all_fields() {
    let rt = runtime();
    let pool = make_pool(mock::D::ok());

    // Acquire one connection so in_use/connections are nonzero.
    let conn = rt.block_on(pool.get());
    assert!(conn.is_ok());

    let state = rt.block_on(pool.state());
    for key in [
        "max_open",
        "connections",
        "in_use",
        "idle",
        "waits",
        "connecting",
        "checking",
    ] {
        assert!(
            matches!(state_field(&state, key), Value::U64(_)),
            "missing or non-numeric key {key}"
        );
    }
    assert_eq!(state_field(&state, "in_use"), Value::U64(1));
}

#[test]
fn test_conn_manager_proxy_check_success_and_failure() {
    use fast_pool::Manager;

    let rt = runtime();

    // Healthy connection: check succeeds.
    let manager =
        ConnectionManager::new(mock::D::ok(), "").expect("manager");
    let proxy = ConnManagerProxy::new(manager.clone());
    let mut guard = rt.block_on(manager.connect()).expect("connect");
    rt.block_on(proxy.check(&mut guard)).expect("check success");

    // Broken connection: check fails and closes the connection.
    let bad_driver = mock::D {
        ping_ok: false,
        ..mock::D::ok()
    };
    let bad_manager = ConnectionManager::new(bad_driver, "").expect("manager");
    let bad_proxy = ConnManagerProxy::new(bad_manager.clone());
    let mut bad_guard = rt.block_on(bad_manager.connect()).expect("connect");
    let err = rt
        .block_on(bad_proxy.check(&mut bad_guard))
        .expect_err("check must fail");
    assert!(err.to_string().contains("ping failed"), "got: {err}");
}

#[test]
fn test_from_connection_manager() {
    use fast_pool::Manager;

    let manager = ConnectionManager::new(mock::D::ok(), "").expect("manager");
    let proxy: ConnManagerProxy = manager.into();
    let rt = runtime();
    let guard = rt.block_on(proxy.connect());
    assert!(guard.is_ok());
}
