use crate::ServerResult;

mod naive;
mod rayon;
mod shared_queue;

pub use self::naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;

/// The trait that all thread pools should implement.
pub trait ThreadPool {
    /// Creates a new thread pool, immediately spawning the specified number of
    /// threads.
    ///
    /// Returns an error if any thread fails to spawn. All previously-spawned threads
    /// are terminated.
    fn new(threads: u32) -> ServerResult<Self>
    where
        Self: Sized;

    /// Spawns a function into the thread pool.
    ///
    /// Spawning always succeeds, but if the function panics the threadpool continues
    /// to operate with the same number of threads &mdash; the thread count is not
    /// reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crossbeam_utils::sync::WaitGroup;

    use crate::ServerResult;

    use super::{NaiveThreadPool, RayonThreadPool, SharedQueueThreadPool, ThreadPool};

    fn spawn_counter<P: ThreadPool>(pool: P) -> ServerResult<()> {
        const TASK_NUM: usize = 20;
        const ADD_COUNT: usize = 1000;

        let wg = WaitGroup::new();
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..TASK_NUM {
            let counter = Arc::clone(&counter);
            let wg = wg.clone();
            pool.spawn(move || {
                for _ in 0..ADD_COUNT {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
                drop(wg);
            })
        }

        wg.wait();
        assert_eq!(counter.load(Ordering::SeqCst), TASK_NUM * ADD_COUNT);
        Ok(())
    }

    fn spawn_panic_task<P: ThreadPool>() -> ServerResult<()> {
        const TASK_NUM: usize = 1000;

        let pool = P::new(4)?;
        for _ in 0..TASK_NUM {
            pool.spawn(move || {
                // It suppresses flood of panic messages to the console.
                // You may find it useful to comment this out during development.
                panic_control::disable_hook_in_current_thread();

                panic!();
            })
        }

        spawn_counter(pool)
    }

    #[test]
    fn naive_thread_pool_spawn_counter() -> ServerResult<()> {
        let pool = NaiveThreadPool::new(4)?;
        spawn_counter(pool)
    }

    #[test]
    fn shared_queue_thread_pool_spawn_counter() -> ServerResult<()> {
        let pool = SharedQueueThreadPool::new(4)?;
        spawn_counter(pool)
    }

    #[test]
    fn rayon_thread_pool_spawn_counter() -> ServerResult<()> {
        let pool = RayonThreadPool::new(4)?;
        spawn_counter(pool)
    }

    #[test]
    fn shared_queue_thread_pool_panic_task() -> ServerResult<()> {
        spawn_panic_task::<SharedQueueThreadPool>()
    }
}
