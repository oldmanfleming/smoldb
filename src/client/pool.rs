use std::collections::VecDeque;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, Weak};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use super::ClientResult;

/// Defines the Inner Pooled Resource
#[derive(Debug)]
pub struct Connection {
    pub reader: OwnedReadHalf,
    pub writer: OwnedWriteHalf,
}

impl Connection {
    async fn new(addr: SocketAddr) -> ClientResult<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        Ok(Connection { reader, writer })
    }
}

/// Wrapper around the pooled Connection.
/// Returns connection to the pool when dropped.
/// Provides a Deref implementation to access the inner Connection.
#[derive(Debug)]
pub struct Object {
    inner: Option<Connection>,
    pool: Weak<PoolInner>,
}

impl Deref for Object {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl DerefMut for Object {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl Drop for Object {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Some(pool) = self.pool.upgrade() {
                pool.return_object(inner);
            }
        }
    }
}

///The Pool that manages Connections
#[derive(Debug, Clone)]
pub struct Pool {
    addr: SocketAddr,
    inner: Arc<PoolInner>,
}

impl Pool {
    /// Create a new Pool with the given address and max size.
    /// Connections are created lazily and thus calling new is not necessarily
    /// indicative of connections being created successfully or the current number of connections in the pool.
    pub fn new(addr: SocketAddr, max_size: usize) -> Self {
        let inner = Arc::new(PoolInner {
            slots: Mutex::new(VecDeque::with_capacity(max_size)),
            semaphore: Semaphore::new(max_size),
        });
        Pool { addr, inner }
    }

    /// Get a connection from the pool.
    /// If the pool is full, this will block until a connection is available.
    /// If the pool is empty of availabile connections but not at max capacity, a new connection will be created.
    pub async fn get(&self) -> ClientResult<Object> {
        let permit = self.inner.semaphore.acquire().await?;

        // Learning lesson note:
        // Since we are using a sync Mutex, we must release the lock
        // before calling an await as sync locks cannot be held across await points.
        // We scope the lock acquisition to ensure it is released before the next await.
        let conn = {
            let mut conns = self.inner.slots.lock()?;
            conns.pop_front()
        };

        let conn = match conn {
            Some(conn) => conn,
            None => Connection::new(self.addr).await?,
        };

        permit.forget();
        Ok(Object {
            inner: Some(conn),
            pool: Arc::downgrade(&self.inner),
        })
    }
}

// sync mutex is used to acquire locks within Drop implementation
#[derive(Debug)]
struct PoolInner {
    slots: Mutex<VecDeque<Connection>>,
    semaphore: Semaphore,
}

impl PoolInner {
    fn return_object(&self, obj: Connection) {
        let mut slots = self.slots.lock().unwrap();
        slots.push_back(obj);
        drop(slots);
        self.semaphore.add_permits(1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tokio::{net::TcpListener, spawn};

    #[tokio::test]
    async fn test_pool() {
        let addr = "127.0.0.1:4012";
        spawn_test_server(addr).await;
        let pool = Pool::new(addr.parse().unwrap(), 2);

        let conn1 = pool.get().await.unwrap();
        let conn2 = pool.get().await.unwrap();
        drop(conn1);
        let conn3 = pool.get().await.unwrap();

        drop(conn2);
        drop(conn3);

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 2);

        let conn4 = pool.get().await.unwrap();
        let conn5 = pool.get().await.unwrap();

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 0);

        drop(conn4);
        drop(conn5);

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_pool_concurrent() {
        let addr = "127.0.0.1:4013";
        spawn_test_server(addr).await;
        let pool = Pool::new(addr.parse().unwrap(), 2);

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 0);

        let f1 = pool.get();
        let f2 = pool.get();

        let (conn1, conn2) = tokio::join!(f1, f2);

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 0);

        drop(conn1.unwrap());
        drop(conn2.unwrap());

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 2);

        let f3 = pool.get();
        let f4 = pool.get();

        let (conn3, conn4) = tokio::join!(f3, f4);

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 0);

        drop(conn3.unwrap());
        drop(conn4.unwrap());

        assert_eq!(pool.inner.slots.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_pool_concurrent_gets() {
        let addr = "127.0.0.1:4014";

        spawn_test_server(addr).await;

        let pool = Pool::new(addr.parse().unwrap(), 2);

        let handles = (0..100)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let conn = pool.get().await.unwrap();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    drop(conn);
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.await.unwrap();
        }
    }

    async fn spawn_test_server(addr: &str) {
        let listener = TcpListener::bind(addr).await.unwrap();
        spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                // Just accept connections and let them drop
                let _ = socket;
            }
        });
        // Brief delay to ensure server is ready
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
