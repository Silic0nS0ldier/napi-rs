use std::{
  sync::atomic::{AtomicBool, Ordering},
  sync::mpsc,
  thread::sleep,
  time::{Duration, Instant},
};

use napi::{bindgen_prelude::*, ScopedTask};

/// Set by [`SignalWhenExecuting::compute`] as it starts.
///
/// A test that needs an `AsyncTask` to be genuinely in flight — rather than
/// merely queued — cannot get there by sleeping: how long a pool thread takes
/// to pick work up is a property of the machine, and guessing it is what makes
/// such a test flaky on a slow runner. This flag lets the test wait for the
/// real transition instead. On a threaded wasm build the flag lives in the
/// shared linear memory, so a store from a pool thread is visible to the
/// JavaScript thread.
static ASYNC_TASK_EXECUTING: AtomicBool = AtomicBool::new(false);

pub struct SimpleTask {
  receiver: mpsc::Receiver<i32>,
}

#[napi]
impl napi::Task for SimpleTask {
  type Output = i32;
  type JsValue = i32;

  fn compute(&mut self) -> Result<Self::Output> {
    self.receiver.recv().map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("Channel receive error: {}", e),
      )
    })
  }

  fn resolve(&mut self, _env: napi::Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }

  fn finally(self, _env: napi::Env) -> Result<()> {
    Ok(())
  }
}

pub struct DelaySum(u32, u32);

#[napi]
impl napi::Task for DelaySum {
  type Output = u32;
  type JsValue = u32;

  fn compute(&mut self) -> Result<Self::Output> {
    sleep(std::time::Duration::from_millis(100));
    Ok(self.0 + self.1)
  }

  fn resolve(&mut self, _env: napi::Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }

  fn finally(self, _env: Env) -> Result<()> {
    Ok(())
  }
}

#[napi]
pub fn without_abort_controller(a: u32, b: u32) -> AsyncTask<DelaySum> {
  AsyncTask::new(DelaySum(a, b))
}

pub struct SignalWhenExecuting(u32);

#[napi]
impl napi::Task for SignalWhenExecuting {
  type Output = u32;
  type JsValue = u32;

  fn compute(&mut self) -> Result<Self::Output> {
    ASYNC_TASK_EXECUTING.store(true, Ordering::SeqCst);
    sleep(std::time::Duration::from_millis(u64::from(self.0)));
    Ok(self.0)
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }
}

/// An `AsyncTask` that announces the start of its own `compute`, then runs for
/// `duration_ms`.
///
/// Poll [`async_task_is_executing`] until it answers `true` to know the task is
/// past the point where `napi_cancel_async_work` can still take it — the
/// transition a test would otherwise have to guess with a sleep.
#[napi]
pub fn async_task_signal_when_executing(duration_ms: u32) -> AsyncTask<SignalWhenExecuting> {
  // Cleared before the task is queued, so a previous run cannot answer for this
  // one.
  ASYNC_TASK_EXECUTING.store(false, Ordering::SeqCst);
  AsyncTask::new(SignalWhenExecuting(duration_ms))
}

/// Whether the most recent [`async_task_signal_when_executing`] task has
/// entered its `compute`.
#[napi]
pub fn async_task_is_executing() -> bool {
  ASYNC_TASK_EXECUTING.load(Ordering::SeqCst)
}

/// Rejecting counterpart of [`ASYNC_TASK_EXECUTING`], set by
/// [`SignalWhenExecutingReject::compute`] as it starts.
static ASYNC_TASK_REJECT_EXECUTING: AtomicBool = AtomicBool::new(false);

pub struct SignalWhenExecutingReject(u32);

#[napi]
impl napi::Task for SignalWhenExecutingReject {
  type Output = u32;
  type JsValue = u32;

  fn compute(&mut self) -> Result<Self::Output> {
    ASYNC_TASK_REJECT_EXECUTING.store(true, Ordering::SeqCst);
    sleep(std::time::Duration::from_millis(u64::from(self.0)));
    Err(Error::from_reason("reject during teardown"))
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }
}

/// The rejecting counterpart of [`async_task_signal_when_executing`]: the task
/// stays in `compute` for `duration_ms`, then rejects — so a test that waits on
/// [`async_task_reject_is_executing`] knows the rejection is still pending, not
/// already settled.
#[napi]
pub fn async_task_signal_when_executing_reject(
  duration_ms: u32,
) -> AsyncTask<SignalWhenExecutingReject> {
  ASYNC_TASK_REJECT_EXECUTING.store(false, Ordering::SeqCst);
  AsyncTask::new(SignalWhenExecutingReject(duration_ms))
}

/// Whether the most recent [`async_task_signal_when_executing_reject`] task has
/// entered its `compute`.
#[napi]
pub fn async_task_reject_is_executing() -> bool {
  ASYNC_TASK_REJECT_EXECUTING.load(Ordering::SeqCst)
}

#[napi]
pub fn with_abort_controller(a: u32, b: u32, signal: AbortSignal) -> AsyncTask<DelaySum> {
  AsyncTask::with_signal(DelaySum(a, b), signal)
}

#[napi]
fn with_abort_signal_handle(signal: AbortSignal) -> AsyncTask<SimpleTask> {
  let (sender, receiver) = mpsc::channel::<i32>();
  signal.on_abort(move || sender.send(999).unwrap());
  AsyncTask::with_signal(SimpleTask { receiver }, signal)
}

/// Set by [`PollAbortToken::compute`] as it starts, so a test can abort a task that's running
/// rather than queued. See [`ASYNC_TASK_EXECUTING`].
static POLL_ABORT_TOKEN_EXECUTING: AtomicBool = AtomicBool::new(false);

pub struct PollAbortToken {
  token: AbortToken,
  cancel_immediately: bool,
}

#[napi]
impl Task for PollAbortToken {
  type Output = u32;
  type JsValue = u32;

  fn compute(&mut self) -> Result<Self::Output> {
    POLL_ABORT_TOKEN_EXECUTING.store(true, Ordering::SeqCst);
    if self.cancel_immediately {
      return Err(Error::new(
        Status::Cancelled,
        "cancelled without an abort".to_owned(),
      ));
    }
    // The deadline only keeps a failed test from holding a pool thread forever.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
      self.token.check()?;
      sleep(Duration::from_millis(1));
    }
    Ok(0)
  }

  fn resolve(&mut self, _env: napi::Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }
}

/// Runs a task whose `compute` polls `signal`'s token until it aborts, then fails with
/// `Status::Cancelled`. With `cancel_immediately`, `compute` fails with `Status::Cancelled` straight
/// away instead.
#[napi]
pub fn poll_abort_token(
  signal: AbortSignal,
  cancel_immediately: bool,
) -> AsyncTask<PollAbortToken> {
  POLL_ABORT_TOKEN_EXECUTING.store(false, Ordering::SeqCst);
  let token = signal.token();
  AsyncTask::with_signal(
    PollAbortToken {
      token,
      cancel_immediately,
    },
    signal,
  )
}

/// Whether the most recent [`poll_abort_token`] task's `compute` has started.
#[napi]
pub fn poll_abort_token_is_executing() -> bool {
  POLL_ABORT_TOKEN_EXECUTING.load(Ordering::SeqCst)
}

/// `AbortSignal::aborted` for `signal`.
#[napi]
pub fn abort_signal_aborted(signal: AbortSignal) -> bool {
  signal.aborted()
}

struct AsyncTaskVoidReturn {}

#[napi]
impl Task for AsyncTaskVoidReturn {
  type JsValue = ();
  type Output = ();

  fn compute(&mut self) -> Result<Self::Output> {
    Ok(())
  }

  fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
    Ok(output)
  }
}

#[napi]
fn async_task_void_return() -> AsyncTask<AsyncTaskVoidReturn> {
  AsyncTask::new(AsyncTaskVoidReturn {})
}

pub struct AsyncTaskOptionalReturn {}

#[napi]
impl Task for AsyncTaskOptionalReturn {
  type JsValue = Option<u32>;
  type Output = ();

  fn compute(&mut self) -> Result<Self::Output> {
    Ok(())
  }

  fn resolve(&mut self, _env: Env, _: Self::Output) -> Result<Self::JsValue> {
    Ok(None)
  }
}

#[napi]
pub fn async_task_optional_return() -> AsyncTask<AsyncTaskOptionalReturn> {
  AsyncTask::new(AsyncTaskOptionalReturn {})
}

pub struct AsyncTaskReadFile {
  path: String,
}

#[napi]
impl<'task> ScopedTask<'task> for AsyncTaskReadFile {
  type Output = Vec<u8>;
  type JsValue = BufferSlice<'task>;

  fn compute(&mut self) -> Result<Self::Output> {
    std::fs::read(&self.path).map_err(|e| Error::new(Status::GenericFailure, format!("{}", e)))
  }

  fn resolve(&mut self, env: &'task Env, output: Self::Output) -> Result<Self::JsValue> {
    BufferSlice::from_data(env, output)
  }
}

#[napi]
pub fn async_task_read_file(path: String) -> AsyncTask<AsyncTaskReadFile> {
  AsyncTask::new(AsyncTaskReadFile { path })
}

pub struct AsyncResolveArray {
  inner: usize,
}

#[napi]
impl<'task> ScopedTask<'task> for AsyncResolveArray {
  type Output = u32;
  type JsValue = Array<'task>;

  fn compute(&mut self) -> Result<Self::Output> {
    Ok(self.inner as u32)
  }

  fn resolve(&mut self, env: &'task Env, output: Self::Output) -> Result<Self::JsValue> {
    let mut array = env.create_array(output)?;
    for i in 0..output {
      array.set(i, i)?;
    }
    Ok(array)
  }
}

#[napi]
pub fn async_resolve_array(inner: u32) -> AsyncTask<AsyncResolveArray> {
  AsyncTask::new(AsyncResolveArray {
    inner: inner as usize,
  })
}

pub struct AsyncTaskFinally {
  inner: ObjectRef,
}

#[napi]
impl Task for AsyncTaskFinally {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> Result<Self::Output> {
    Ok(())
  }

  fn resolve(&mut self, env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    let mut obj = self.inner.get_value(&env)?;
    obj.set("resolve", true)?;
    Ok(())
  }

  fn finally(self, env: Env) -> Result<()> {
    let mut obj = self.inner.get_value(&env)?;
    obj.set("finally", true)?;
    self.inner.unref(&env)?;
    Ok(())
  }
}

#[napi]
pub fn async_task_finally(inner: ObjectRef) -> AsyncTask<AsyncTaskFinally> {
  AsyncTask::new(AsyncTaskFinally { inner })
}

/// An `Error` captured from an arbitrary JS value on the JS thread
/// (`Error::from_unknown_without_coercion`), carried through an `AsyncTask`'s
/// `compute` on the libuv thread and handed back by the default `Task::reject`.
/// The promise must settle with the identical retained value — same primitive,
/// same object identity — not a synthesized `Error` built from the reason.
/// The completion callback runs back on the owning env/thread, so the retained
/// reference is restorable there; `values.spec.ts` asserts the identity.
pub struct AsyncTaskRejectWithCapturedValue {
  error: Option<Error>,
}

#[napi]
impl Task for AsyncTaskRejectWithCapturedValue {
  type Output = ();
  type JsValue = ();

  fn compute(&mut self) -> Result<Self::Output> {
    Err(
      self
        .error
        .take()
        .unwrap_or_else(|| Error::from_reason("captured error was already taken")),
    )
  }

  fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    Ok(())
  }
}

#[napi]
pub fn async_task_reject_with_captured_value(
  value: Unknown,
) -> AsyncTask<AsyncTaskRejectWithCapturedValue> {
  AsyncTask::new(AsyncTaskRejectWithCapturedValue {
    error: Some(Error::from_unknown_without_coercion(value)),
  })
}

pub struct AsyncTaskArrayBuffer {
  data: Vec<u8>,
}

#[napi]
impl<'task> ScopedTask<'task> for AsyncTaskArrayBuffer {
  type Output = Vec<u8>;
  type JsValue = ArrayBuffer<'task>;

  fn compute(&mut self) -> Result<Self::Output> {
    // Simulate some async computation
    sleep(std::time::Duration::from_millis(10));
    Ok(self.data.clone())
  }

  fn resolve(&mut self, env: &'task Env, output: Self::Output) -> Result<Self::JsValue> {
    ArrayBuffer::from_data(env, output)
  }
}

#[napi]
pub fn async_task_arraybuffer(data: Vec<u8>) -> AsyncTask<AsyncTaskArrayBuffer> {
  AsyncTask::new(AsyncTaskArrayBuffer { data })
}
