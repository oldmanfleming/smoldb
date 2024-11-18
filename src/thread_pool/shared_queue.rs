use std::thread;

use crossbeam::channel::{self, Receiver, Sender};

use crate::ServerResult;

use super::ThreadPool;

/// A thread pool implementation that shares a queue of jobs among the specified number of worker threads.
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> ServerResult<Self> {
        let (tx, rx) = channel::unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        for _ in 0..threads {
            let rx = TaskReceiver(rx.clone());
            thread::spawn(move || {
                while let Ok(job) = rx.0.recv() {
                    job();
                }
            });
        }

        Ok(SharedQueueThreadPool { tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.tx
            .send(Box::new(job))
            .expect("The thread pool has no receivers.");
    }
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

// Keep the thread alive in the pool if an individual job panics.
impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = self.clone();
            thread::spawn(move || {
                while let Ok(job) = rx.0.recv() {
                    job();
                }
            });
        }
    }
}
