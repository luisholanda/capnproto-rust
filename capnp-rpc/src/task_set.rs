// Copyright (c) 2013-2016 Sandstorm Development Group, Inc. and contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use futures::channel::{mpsc, oneshot};
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{Future, FutureExt, Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

use std::cell::RefCell;
use std::rc::Rc;

enum EnqueuedTask<E> {
    Task(Pin<Box<dyn Future<Output = Result<(), E>>>>),
    Terminate(Result<(), E>),
    OnEmpty(oneshot::Sender<()>),
}

#[must_use = "a TaskSet does nothing unless polled"]
pub struct TaskSet<E> {
    enqueued: Option<mpsc::UnboundedReceiver<EnqueuedTask<E>>>,
    completed_tasks_tx: mpsc::UnboundedSender<()>,
    completed_tasks_rx: mpsc::UnboundedReceiver<()>,
    terminate_tx: mpsc::Sender<Result<(), E>>,
    terminate_rx: mpsc::Receiver<Result<(), E>>,
    executor: Pin<Box<dyn Executor>>,
    on_empty_fulfillers: Vec<oneshot::Sender<()>>,
    reaper: Rc<RefCell<dyn TaskReaper<E>>>,
}

impl<E> TaskSet<E>
where
    E: 'static,
{
    pub(crate) fn new(reaper: impl TaskReaper<E> + 'static) -> (TaskSetHandle<E>, Self)
    where
        E: 'static,
        E: ::std::fmt::Debug,
    {
        Self::with_executor(reaper, Box::new(DefaultExecutor::default()))
    }

    pub(crate) fn with_executor(
        reaper: impl TaskReaper<E> + 'static,
        executor: Box<dyn Executor>,
    ) -> (TaskSetHandle<E>, Self)
    where
        E: std::fmt::Debug,
    {
        let (sender, receiver) = mpsc::unbounded();
        let (completed_tasks_tx, completed_tasks_rx) = mpsc::unbounded();
        let (terminate_tx, terminate_rx) = mpsc::channel(1);

        let set = Self {
            enqueued: Some(receiver),
            completed_tasks_tx,
            completed_tasks_rx,
            terminate_tx,
            terminate_rx,
            executor: Pin::from(executor),
            on_empty_fulfillers: vec![],
            reaper: Rc::new(RefCell::new(reaper)) as Rc<RefCell<dyn TaskReaper<E>>>,
        };

        let handle = TaskSetHandle { sender };

        (handle, set)
    }

    pub(crate) fn executor(&self) -> &dyn Executor {
        &*self.executor
    }

    fn update_on_empty_fulfillers(&mut self) {
        // All spawned tasks (ignoring terminating ones), have a weak ref to the reaper.
        if Rc::weak_count(&self.reaper) == 0 {
            for f in self.on_empty_fulfillers.drain(..) {
                let _ = f.send(());
            }
        }
    }
}

#[derive(Clone)]
pub struct TaskSetHandle<E> {
    sender: mpsc::UnboundedSender<EnqueuedTask<E>>,
}

impl<E> TaskSetHandle<E>
where
    E: 'static,
{
    pub fn add<F>(&mut self, f: F)
    where
        F: Future<Output = Result<(), E>> + 'static,
    {
        let _ = self.sender.unbounded_send(EnqueuedTask::Task(Box::pin(f)));
    }

    pub fn terminate(&mut self, result: Result<(), E>) {
        let _ = self.sender.unbounded_send(EnqueuedTask::Terminate(result));
    }

    /// Returns a future that finishes at the next time when the task set
    /// is empty. If the task set is terminated, the oneshot will be canceled.
    pub fn on_empty(&mut self) -> oneshot::Receiver<()> {
        let (s, r) = oneshot::channel();
        let _ = self.sender.unbounded_send(EnqueuedTask::OnEmpty(s));
        r
    }
}

/// For a specific kind of task, `TaskReaper` defines the procedure that should
/// be invoked when it succeeds or fails.
pub trait TaskReaper<E>
where
    E: 'static,
{
    fn task_succeeded(&mut self) {}
    fn task_failed(&mut self, error: E);
}

impl<E> Future for TaskSet<E>
where
    E: 'static,
{
    type Output = Result<(), E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Self {
            enqueued: Some(ref mut enqueued),
            ref reaper,
            ref mut on_empty_fulfillers,
            executor,
            completed_tasks_tx,
            terminate_tx,
            ..
        } = self.as_mut().get_mut()
        {
            loop {
                match Pin::new(&mut *enqueued).poll_next(cx) {
                    Poll::Pending => break,
                    Poll::Ready(None) => {
                        drop(self.enqueued.take());
                        break;
                    }
                    Poll::Ready(Some(EnqueuedTask::Terminate(r))) => {
                        // Give spawned futures a chance to finish before stopping the TaskSet.
                        let mut tx = terminate_tx.clone();
                        executor.as_mut().spawn(Box::pin(async move {
                            // If couldn't send, another terminate task was called prior to this.
                            let _ = tx.try_send(r);
                        }));
                    }
                    Poll::Ready(Some(EnqueuedTask::Task(f))) => {
                        let reaper = Rc::downgrade(reaper);
                        let tx = completed_tasks_tx.clone();
                        let wake_up_on_finish = executor.need_polling();
                        executor.as_mut().spawn(Box::pin(async move {
                            let r = f.await;
                            if let Some(reaper) = reaper.upgrade() {
                                match r {
                                    Ok(()) => reaper.borrow_mut().task_succeeded(),
                                    Err(e) => reaper.borrow_mut().task_failed(e),
                                }
                            }

                            if wake_up_on_finish || reaper.weak_count() == 1 {
                                let _ = tx.unbounded_send(());
                            }
                        }));
                    }
                    Poll::Ready(Some(EnqueuedTask::OnEmpty(f))) => {
                        on_empty_fulfillers.push(f);
                    }
                }
            }
        }

        loop {
            let _ = self.executor.poll_unpin(cx);
            match self.completed_tasks_rx.poll_next_unpin(cx) {
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(_)) => self.update_on_empty_fulfillers(),
                Poll::Pending => match std::task::ready!(self.terminate_rx.poll_next_unpin(cx)) {
                    None => return Poll::Ready(Ok(())),
                    Some(r) => {
                        self.on_empty_fulfillers.clear();
                        return Poll::Ready(r);
                    }
                },
            }
        }
    }
}

/// Trait for abstracting how [`RpcSystem`] should spawn futures.
///
/// ## `Future` implementation
///
/// This trait requires the executor type to implement [`Future`]. Polling it should
/// make progress in the futures spawned. For implementations that spawn in the current
/// running executor, e.g. tokio's `LocalSet`, the implementation can be a no-op, as
/// it can be assumed that the application already took care of driving the executor.
///
/// This requirement is mostly for implementations that gives [`RpcSystem`] its own
/// executor.
///
/// ## Examples
///
/// ### Inside `tokio`'s `LocalSet`
///
/// ```ignore
/// struct TokioLocalSetExecutor;
///
/// impl Future for TokioLocalSetExecutor {
///     type Output = ();
///
///     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
///          Poll::Pending
///     }
/// }
///
/// impl Executor for TokioLocalSetExecutor {
///     fn spawn(self: Pin<&mut Self>, fut: LocalBoxFuture<'static, ()>) {
///         tokio::task::spawn_local(fut);
///     }
///
///     fn need_polling(&self) -> bool {
///         false
///     }
///
///     fn clone_empty(&self) -> Box<dyn Executor> {
///         Box::new(TokioLocalSetExecutor)
///     }
/// }
/// ```
///
/// [`RpcSystem`]: crate::RpcSystem
pub trait Executor: Future<Output = ()> {
    // NOTE: This receives a boxed future to keep the trait dyn-compatible and allowing
    //       `TaskSet` to type erase its executor.
    /// Spawns the future in the executor.
    fn spawn(self: Pin<&mut Self>, fut: LocalBoxFuture<'static, ()>);

    /// If this executor needs to be polled explicitly.
    ///
    /// In the cases where the executor forwards futures to the current running executor,
    /// that is polled by some top-level task, this can return `false`. This allows some
    /// optimizations in the RPC system task loop.
    fn need_polling(&self) -> bool;

    /// Clones the executor, returning it in a standalone empty state.
    ///
    /// In the cases where the executor forwards futures to the current running executor,
    /// "empty" doesn't make sense, but the returned executor should not share state with
    /// `self`.
    fn clone_empty(&self) -> Box<dyn Executor>;
}

#[derive(Default)]
pub(crate) struct DefaultExecutor {
    in_progress: FuturesUnordered<LocalBoxFuture<'static, ()>>,
}

impl Future for DefaultExecutor {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().in_progress.poll_next_unpin(cx).map(|_| ())
    }
}

impl Executor for DefaultExecutor {
    fn spawn(self: Pin<&mut Self>, fut: LocalBoxFuture<'static, ()>) {
        self.get_mut().in_progress.push(fut);
    }

    fn need_polling(&self) -> bool {
        true
    }

    fn clone_empty(&self) -> Box<dyn Executor> {
        Box::new(Self::default())
    }
}
