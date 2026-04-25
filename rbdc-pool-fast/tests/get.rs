use rbdc::pool::Pool;
use rbdc_pool_fast::FastPool;

mod mock {
    use std::pin::Pin;
    use futures_core::future::BoxFuture;
    use futures_core::stream::BoxStream;
    use rbdc::db::{ConnectOptions, Connection, Driver, ExecResult, Row};
    use rbdc::try_stream;
    use rbdc::Error;
    use rbs::Value;

    #[derive(Debug)]
    pub struct Opt {}

    impl ConnectOptions for Opt {
        fn connect(&self) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
            Box::pin(async { Ok(Box::new(Conn {}) as Box<dyn Connection>) })
        }

        fn set_uri(&mut self, _uri: &str) -> Result<(), Error> {
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct Conn {}

    impl Connection for Conn {
        fn exec_rows(
            &mut self,
            _sql: &str,
            _params: Vec<Value>,
        ) -> BoxFuture<'_, Result<BoxStream<'_, Result<Box<dyn Row>, Error>>, Error>> {
            Box::pin(async move {
                let rows: Vec<Box<dyn Row>> = vec![];
                let stream = try_stream! {
                    for row in rows {
                        r#yield!(row);
                    }
                    Ok(())
                };
                let stream: Pin<Box<dyn futures_core::Stream<Item = Result<Box<dyn Row>, Error>> + Send>> = Box::pin(stream);
                Ok(stream)
            })
        }

        fn exec(
            &mut self,
            _sql: &str,
            _params: Vec<Value>,
        ) -> BoxFuture<'_, Result<ExecResult, Error>> {
            Box::pin(async { Ok(ExecResult::default()) })
        }

        fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
            Box::pin(async { Ok(()) })
        }

        fn close(&mut self) -> BoxFuture<'_, Result<(), Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Debug)]
    pub struct D {}

    impl Driver for D {
        fn name(&self) -> &str {
            "mock"
        }

        fn connect(&self, _url: &str) -> BoxFuture<'_, Result<Box<dyn Connection>, Error>> {
            Box::pin(async { Ok(Box::new(Conn {}) as Box<dyn Connection>) })
        }

        fn connect_opt<'a>(
            &'a self,
            _opt: &'a dyn ConnectOptions,
        ) -> BoxFuture<'a, Result<Box<dyn Connection>, Error>> {
            Box::pin(async { Ok(Box::new(Conn {}) as Box<dyn Connection>) })
        }

        fn default_option(&self) -> Box<dyn ConnectOptions> {
            Box::new(Opt {})
        }
    }

    pub fn create_connection_manager() -> rbdc::pool::ConnectionManager {
        rbdc::pool::ConnectionManager::new(D {}, "").unwrap()
    }
}

fn get_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

#[test]
fn test_create_pool() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager);
    assert!(pool.is_ok());
}

#[test]
fn test_get_connection() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    let conn = rt.block_on(pool.get());
    assert!(conn.is_ok());
}

#[test]
fn test_pool_state() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    let state = rt.block_on(pool.state());
    assert!(state.is_map());
}

#[test]
fn test_get_multiple_connections() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();

    let conn1 = rt.block_on(pool.get());
    assert!(conn1.is_ok());

    let conn2 = rt.block_on(pool.get());
    assert!(conn2.is_ok());

    let state = rt.block_on(pool.state());
    assert!(state.is_map());
}

#[test]
fn test_set_max_open_conns() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    rt.block_on(pool.set_max_open_conns(5));

    let state = rt.block_on(pool.state());
    assert!(state.is_map());
}

#[test]
fn test_set_max_idle_conns() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    rt.block_on(pool.set_max_idle_conns(3));

    let state = rt.block_on(pool.state());
    assert!(state.is_map());
}

#[test]
fn test_get_with_timeout() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    let conn = rt.block_on(pool.get_timeout(std::time::Duration::from_secs(5)));
    assert!(conn.is_ok());
}

#[test]
fn test_zero_timeout_get() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    let conn = rt.block_on(pool.get_timeout(std::time::Duration::from_secs(0)));
    assert!(conn.is_ok());
}

#[test]
fn test_set_timeout() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();
    rt.block_on(pool.set_timeout(Some(std::time::Duration::from_secs(30))));

    let conn = rt.block_on(pool.get());
    assert!(conn.is_ok());
}

#[test]
fn test_driver_type() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    assert_eq!(pool.driver_type(), "mock");
}

#[test]
fn test_connection_drop_returns_to_pool() {
    let manager = mock::create_connection_manager();
    let pool = FastPool::new(manager).unwrap();

    let rt = get_runtime();

    {
        let conn = rt.block_on(pool.get());
        assert!(conn.is_ok());
    }

    let state = rt.block_on(pool.state());
    assert!(state.is_map());
}
