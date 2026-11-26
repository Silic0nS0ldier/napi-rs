use std::cell::{Cell, RefCell};
use std::ffi::{c_void, CStr};
use std::panic::UnwindSafe;
use std::ptr;
use std::rc::Rc;

#[cfg(not(feature = "noop"))]
use crate::bindgen_runtime::env_registration::{self, EnvState, RegistrationId};
use crate::{
  async_work,
  bindgen_prelude::{FromNapiValue, ToNapiValue, TypeName},
  check_status, sys, Error, ScopedTask, Status,
};

pub struct AsyncTask<T: for<'task> ScopedTask<'task>> {
  inner: T,
  abort_signal: Option<AbortSignal>,
}

impl<T: for<'task> ScopedTask<'task>> TypeName for T {
  fn type_name() -> &'static str {
    "AsyncTask"
  }

  fn value_type() -> crate::ValueType {
    crate::ValueType::Object
  }
}

impl<T: for<'task> ScopedTask<'task>> AsyncTask<T> {
  pub fn new(task: T) -> Self {
    Self {
      inner: task,
      abort_signal: None,
    }
  }

  pub fn with_signal(task: T, signal: AbortSignal) -> Self {
    Self {
      inner: task,
      abort_signal: Some(signal),
    }
  }

  pub fn with_optional_signal(task: T, signal: Option<AbortSignal>) -> Self {
    Self {
      inner: task,
      abort_signal: signal,
    }
  }
}

type AbortCallbacks = RefCell<Vec<Box<dyn Fn()>>>;

/// State shared by an [`AbortSignal`] and the `abort` listener it registers.
struct AbortState {
  /// The queued work, once the signal is attached to an [`AsyncTask`].
  raw_work: Cell<sys::napi_async_work>,
  /// Shared with the work: 0 not started, 1 completed, 2 cancelled.
  status: Rc<Cell<u8>>,
  callbacks: AbortCallbacks,
  /// Set when the [`AbortSignal`] drops. A listener that couldn't be removed then does nothing.
  detached: Cell<bool>,
}

impl AbortState {
  /// Runs on the JavaScript thread, from the signal's `abort` event.
  fn abort(&self, env: sys::napi_env) {
    if self.detached.get() {
      return;
    }
    for cb in self.callbacks.borrow().iter() {
      cb();
    }
    // Completed: the work handle is already freed.
    if self.status.get() == 1 {
      return;
    }
    let raw_work = self.raw_work.get();
    // Not attached to a task (yet).
    if raw_work.is_null() {
      return;
    }
    // Only succeeds while the work is still queued; once started it runs to completion.
    let status = unsafe { sys::napi_cancel_async_work(env, raw_work) };
    self
      .status
      .set(if status == sys::Status::napi_ok { 2 } else { 0 });
  }
}

/// A JavaScript [`AbortSignal`](https://developer.mozilla.org/en-US/docs/Web/API/AbortSignal).
///
/// Converting one registers an `abort` listener with `addEventListener`, which is removed when this
/// value drops. Passed to [`AsyncTask::with_signal`], it lives until the task settles, and aborting
/// cancels the task if it hasn't started running yet.
///
/// Any object with `addEventListener` and `removeEventListener` is accepted, so polyfills work on
/// Node.js versions without a global `AbortController`.
pub struct AbortSignal {
  state: Rc<AbortState>,
  env: sys::napi_env,
  #[cfg(not(feature = "noop"))]
  registration: Option<RegistrationId>,
  signal: sys::napi_ref,
  listener: sys::napi_ref,
}

impl AbortSignal {
  /// Registers `cb` to run on the JavaScript thread when the signal aborts, for as long as this
  /// value is alive.
  pub fn on_abort<F: Fn() + 'static>(&self, cb: F) {
    self.state.callbacks.borrow_mut().push(Box::new(cb));
  }

  pub(crate) fn status(&self) -> Rc<Cell<u8>> {
    self.state.status.clone()
  }

  pub(crate) fn set_raw_work(&self, raw_work: sys::napi_async_work) {
    self.state.raw_work.set(raw_work);
  }

  /// Whether `env` can still be called into from this value's `Drop`.
  #[cfg(not(feature = "noop"))]
  fn env_is_alive(&self) -> bool {
    match self.registration {
      Some(id) => env_registration::is_registration_alive(id, self.env),
      None => env_registration::env_state(self.env) != EnvState::TornDown,
    }
  }

  #[cfg(feature = "noop")]
  fn env_is_alive(&self) -> bool {
    false
  }

  /// Calls `signal.removeEventListener('abort', listener)`, losing no exception:
  ///
  /// - One already pending is taken first, so the call can run, and thrown again afterwards.
  /// - One thrown by `removeEventListener` is left pending, like an exception from a `finally`.
  /// - If both happen, they're combined into a `SuppressedError`, as `using` does when disposal
  ///   throws during an exception.
  ///
  /// A failure that throws nothing (e.g. JavaScript can't run during teardown) leaves the listener
  /// registered but detached.
  fn remove_listener(&self) {
    let env = self.env;
    let pending = unsafe { take_exception(env) };
    let thrown = match unsafe { self.call_remove_listener() } {
      Ok(()) => None,
      Err(_) => unsafe { take_exception(env) },
    };
    let exception = match (pending, thrown) {
      (None, None) => return,
      (Some(exception), None) | (None, Some(exception)) => exception,
      (Some(pending), Some(thrown)) => {
        unsafe { suppressed_error(env, thrown, pending) }.unwrap_or(pending)
      }
    };
    unsafe { sys::napi_throw(env, exception) };
  }

  unsafe fn call_remove_listener(&self) -> crate::Result<()> {
    let env = self.env;
    let mut signal = ptr::null_mut();
    check_status!(unsafe { sys::napi_get_reference_value(env, self.signal, &mut signal) })?;
    let mut listener = ptr::null_mut();
    check_status!(unsafe { sys::napi_get_reference_value(env, self.listener, &mut listener) })?;
    if signal.is_null() || listener.is_null() {
      return Err(Error::new(
        Status::GenericFailure,
        "AbortSignal reference is no longer alive".to_owned(),
      ));
    }
    let remove = unsafe { get_method(env, signal, c"removeEventListener") }?;
    unsafe { call_listener_method(env, signal, remove, listener) }
  }
}

/// Takes the pending exception, if any.
unsafe fn take_exception(env: sys::napi_env) -> Option<sys::napi_value> {
  let mut pending = false;
  if unsafe { sys::napi_is_exception_pending(env, &mut pending) } != sys::Status::napi_ok
    || !pending
  {
    return None;
  }
  let mut exception = ptr::null_mut();
  (unsafe { sys::napi_get_and_clear_last_exception(env, &mut exception) } == sys::Status::napi_ok)
    .then_some(exception)
}

/// `new SuppressedError(error, suppressed, message)`, or an `Error` with the same `name`, `error`
/// and `suppressed` where the global `SuppressedError` (ES2025) isn't available.
unsafe fn suppressed_error(
  env: sys::napi_env,
  error: sys::napi_value,
  suppressed: sys::napi_value,
) -> Option<sys::napi_value> {
  const MESSAGE: &CStr = c"An error was suppressed while removing an AbortSignal listener";
  let mut message = ptr::null_mut();
  if unsafe {
    sys::napi_create_string_utf8(
      env,
      MESSAGE.as_ptr(),
      MESSAGE.count_bytes() as isize,
      &mut message,
    )
  } != sys::Status::napi_ok
  {
    return None;
  }
  let mut global = ptr::null_mut();
  let mut constructor = ptr::null_mut();
  let mut value_type = 0;
  if unsafe { sys::napi_get_global(env, &mut global) } == sys::Status::napi_ok
    && unsafe {
      sys::napi_get_named_property(env, global, c"SuppressedError".as_ptr(), &mut constructor)
    } == sys::Status::napi_ok
    && unsafe { sys::napi_typeof(env, constructor, &mut value_type) } == sys::Status::napi_ok
    && value_type == sys::ValueType::napi_function
  {
    let args = [error, suppressed, message];
    let mut result = ptr::null_mut();
    if unsafe { sys::napi_new_instance(env, constructor, args.len(), args.as_ptr(), &mut result) }
      == sys::Status::napi_ok
    {
      return Some(result);
    }
  }
  // A throwing `SuppressedError` lookup or constructor mustn't stand in for the two exceptions.
  unsafe { take_exception(env) };
  let mut result = ptr::null_mut();
  if unsafe { sys::napi_create_error(env, ptr::null_mut(), message, &mut result) }
    != sys::Status::napi_ok
  {
    return None;
  }
  let mut name = ptr::null_mut();
  let name_text = c"SuppressedError";
  unsafe {
    sys::napi_create_string_utf8(
      env,
      name_text.as_ptr(),
      name_text.count_bytes() as isize,
      &mut name,
    );
    sys::napi_set_named_property(env, result, c"name".as_ptr(), name);
    sys::napi_set_named_property(env, result, c"error".as_ptr(), error);
    sys::napi_set_named_property(env, result, c"suppressed".as_ptr(), suppressed);
    take_exception(env);
  }
  Some(result)
}

impl Drop for AbortSignal {
  fn drop(&mut self) {
    self.state.detached.set(true);
    // A torn-down env frees the references itself, and its JavaScript can't run.
    if !self.env_is_alive() {
      return;
    }
    let env = self.env;
    let mut scope = ptr::null_mut();
    let scope_opened =
      unsafe { sys::napi_open_handle_scope(env, &mut scope) } == sys::Status::napi_ok;
    self.remove_listener();
    if scope_opened {
      unsafe { sys::napi_close_handle_scope(env, scope) };
    }
    unsafe {
      sys::napi_delete_reference(env, self.listener);
      sys::napi_delete_reference(env, self.signal);
    }
  }
}

impl UnwindSafe for AbortSignal {}
impl std::panic::RefUnwindSafe for AbortSignal {}

fn not_an_abort_signal() -> Error {
  Error::new(Status::InvalidArg, "Value is not an AbortSignal".to_owned())
}

/// `target[name]`, which must be a function.
unsafe fn get_method(
  env: sys::napi_env,
  target: sys::napi_value,
  name: &CStr,
) -> crate::Result<sys::napi_value> {
  let mut function = ptr::null_mut();
  check_status!(
    unsafe { sys::napi_get_named_property(env, target, name.as_ptr(), &mut function) },
    "Get AbortSignal.{} failed",
    name.to_string_lossy()
  )?;
  let mut value_type = 0;
  check_status!(unsafe { sys::napi_typeof(env, function, &mut value_type) })?;
  if value_type != sys::ValueType::napi_function {
    return Err(not_an_abort_signal());
  }
  Ok(function)
}

/// Calls `method.call(target, 'abort', listener)`.
unsafe fn call_listener_method(
  env: sys::napi_env,
  target: sys::napi_value,
  method: sys::napi_value,
  listener: sys::napi_value,
) -> crate::Result<()> {
  let mut event = ptr::null_mut();
  check_status!(unsafe { sys::napi_create_string_utf8(env, c"abort".as_ptr(), 5, &mut event) })?;
  let args = [event, listener];
  let mut result = ptr::null_mut();
  check_status!(
    unsafe { sys::napi_call_function(env, target, method, args.len(), args.as_ptr(), &mut result) },
    "Call AbortSignal listener method failed"
  )
}

/// Creates the `abort` listener. Its callback data is a boxed `Rc<AbortState>`, freed by a wrap
/// finalizer on the function when it's collected. Only Node-API 1 is used, as `AbortSignal` is
/// available without the `napi5` feature `napi_add_finalizer` needs.
unsafe fn create_listener(
  env: sys::napi_env,
  state: Rc<AbortState>,
) -> crate::Result<sys::napi_value> {
  let data = Box::into_raw(Box::new(state));
  let mut listener = ptr::null_mut();
  let name = c"onabort";
  let mut status = unsafe {
    sys::napi_create_function(
      env,
      name.as_ptr(),
      name.count_bytes() as isize,
      Some(abort_listener),
      data.cast(),
      &mut listener,
    )
  };
  if status == sys::Status::napi_ok {
    status = unsafe {
      sys::napi_wrap(
        env,
        listener,
        data.cast(),
        Some(abort_listener_finalize),
        ptr::null_mut(),
        ptr::null_mut(),
      )
    };
    if status == sys::Status::napi_ok {
      return Ok(listener);
    }
  }
  // Nothing references the function, so its callback can't run with the freed data.
  drop(unsafe { Box::from_raw(data) });
  Err(Error::new(
    Status::from(status),
    "Create AbortSignal listener failed".to_owned(),
  ))
}

extern "C" fn abort_listener(
  env: sys::napi_env,
  callback_info: sys::napi_callback_info,
) -> sys::napi_value {
  let mut data = ptr::null_mut();
  let status = unsafe {
    sys::napi_get_cb_info(
      env,
      callback_info,
      ptr::null_mut(),
      ptr::null_mut(),
      ptr::null_mut(),
      &mut data,
    )
  };
  // The data is the one `create_listener` gave this function, never a value from JavaScript.
  if status == sys::Status::napi_ok && !data.is_null() {
    let state = unsafe { &*data.cast::<Rc<AbortState>>() };
    state.abort(env);
  }
  ptr::null_mut()
}

unsafe extern "C" fn abort_listener_finalize(
  _env: sys::napi_env,
  finalize_data: *mut c_void,
  _finalize_hint: *mut c_void,
) {
  drop(unsafe { Box::from_raw(finalize_data.cast::<Rc<AbortState>>()) });
}

impl FromNapiValue for AbortSignal {
  unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> crate::Result<Self> {
    let mut value_type = 0;
    check_status!(unsafe { sys::napi_typeof(env, napi_val, &mut value_type) })?;
    if value_type != sys::ValueType::napi_object {
      return Err(not_an_abort_signal());
    }
    // Both are checked up front, so a signal that can be listened to can also be cleaned up.
    let add_event_listener = unsafe { get_method(env, napi_val, c"addEventListener") }?;
    unsafe { get_method(env, napi_val, c"removeEventListener") }?;

    #[cfg(not(feature = "noop"))]
    let registration = match env_registration::env_state(env) {
      EnvState::Live(id) => Some(id),
      EnvState::Unregistered => None,
      EnvState::TornDown => {
        return Err(Error::new(
          Status::InvalidArg,
          "Cannot use an AbortSignal from a torn-down JavaScript environment".to_owned(),
        ))
      }
    };

    let state = Rc::new(AbortState {
      raw_work: Cell::new(ptr::null_mut()),
      status: Rc::new(Cell::new(0)),
      callbacks: RefCell::default(),
      detached: Cell::new(false),
    });
    let listener = unsafe { create_listener(env, state.clone()) }?;

    let mut signal_ref = ptr::null_mut();
    check_status!(unsafe { sys::napi_create_reference(env, napi_val, 1, &mut signal_ref) })?;
    let mut listener_ref = ptr::null_mut();
    if let Err(err) =
      check_status!(unsafe { sys::napi_create_reference(env, listener, 1, &mut listener_ref) })
    {
      unsafe { sys::napi_delete_reference(env, signal_ref) };
      return Err(err);
    }

    // From here, dropping `signal` on an error removes the listener and releases both references.
    let signal = AbortSignal {
      state,
      env,
      #[cfg(not(feature = "noop"))]
      registration,
      signal: signal_ref,
      listener: listener_ref,
    };
    unsafe { call_listener_method(env, napi_val, add_event_listener, listener) }?;
    Ok(signal)
  }
}

impl<T: for<'task> ScopedTask<'task>> ToNapiValue for AsyncTask<T> {
  unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> crate::Result<sys::napi_value> {
    // Only the value actually converted into the JavaScript promise may claim a lease on
    // the borrow scope that generated glue deferred for `&T`/`&self` arguments captured
    // by the task. The glue arms the deferred stack while converting its return value —
    // after the native call — so a conversion reaching this point is the one the scope
    // was deferred for. Claiming upgrades the recorded borrows into `napi_ref` roots
    // (idempotently: every element of a `Vec<AsyncTask>` takes its own lease on the
    // same scope) and a conversion with no deferred scope (the task came from
    // `Env::spawn`, `spawn_future`, user code) yields `None`.
    let borrow_lease = crate::bindgen_runtime::claim_deferred_native_borrow_lease()?;
    let async_promise = async_work::run(env, val.inner, val.abort_signal, borrow_lease)?;
    Ok(async_promise.promise_object().inner)
  }
}
