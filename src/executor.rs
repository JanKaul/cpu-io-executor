use async_task::Runnable;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// An executor designed to run potentially blocking futures
pub struct AsyncExecutor {
    io: tokio::runtime::Handle,
    cpu: Arc<rayon::ThreadPool>,
}

impl AsyncExecutor {
    pub fn new() -> Self {
        let io = tokio::runtime::Handle::current();
        let cpu = rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .use_current_thread()
            .build()
            .unwrap();

        let cpu = Arc::new(cpu);
        Self { io, cpu }
    }

    pub fn spawn<F>(&self, fut: F) -> SpawnHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, receiver) = futures::channel::oneshot::channel();
        let handle = self.io.clone();

        // This box is technically unnecessary, but avoids some pin shenanigans
        let mut boxed = Box::pin(fut);

        // Enter tokio runtime whilst polling future - allowing IO and timers to work
        let io_fut = futures::future::poll_fn(move |cx| {
            let _guard = handle.enter();
            boxed.poll_unpin(cx)
        });
        // Route result back to oneshot
        let remote_fut = io_fut.map(|out| {
            let _ = sender.send(out);
        });

        // Task execution is scheduled on rayon
        let cpu = self.cpu.clone();
        let (runnable, task) = async_task::spawn(remote_fut, move |runnable: Runnable<()>| {
            cpu.spawn(move || {
                let _ = runnable.run();
            });
        });
        runnable.schedule();
        SpawnHandle {
            _task: task,
            receiver,
        }
    }
}

/// Handle returned by [`AsyncExecutor`]
///
/// Cancels task on drop
pub struct SpawnHandle<T> {
    receiver: futures::channel::oneshot::Receiver<T>,
    _task: async_task::Task<()>,
}

impl<T> Future for SpawnHandle<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.receiver.poll_unpin(cx).map(|x| x.ok())
    }
}
