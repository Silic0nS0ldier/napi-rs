import * as events from 'node:events'

import test from 'ava'

import { spawnAsyncTask, withAbortController } from '../index.cjs'

import { LOAD_IN_WORKER, runInWorker } from './multi-env-helpers.js'

// Node.js has a global `AbortController` from v15 (v14.17 behind a flag). Older versions use
// polyfills, which the duck-typed tests below stand in for.
const hasAbortController = typeof AbortController !== 'undefined'
// `getEventListeners` accepts an `EventTarget` from Node.js v15.
const getEventListeners = (
  events as { getEventListeners?: (target: object, type: string) => unknown[] }
).getEventListeners

// Tests that run tasks are serial: the libuv pool, and the release flag of the tasks that
// saturate it, are shared by the whole process. ava runs serial tests before concurrent ones.
// Saturating the pool relies on libuv's FIFO queue, which emnapi's pool doesn't promise.
const queuedTest = test.serial.skipIf(
  !hasAbortController || Boolean(process.env.WASI_TEST),
)
const signalTest = test.serial.skipIf(!hasAbortController)

// A promise that never settles must fail the test, not hang it until the ava timeout.
function settledWithin<T>(promise: Promise<T>, ms = 5000) {
  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`promise still pending after ${ms} ms`)),
      ms,
    )
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

/**
 * Occupies every libuv pool thread, so work queued afterwards can't start until `release()`.
 * The blockers share one release flag (see `spawnAsyncTask`), so one `release()` frees them all.
 */
function saturatePool() {
  const poolSize = Number(process.env.UV_THREADPOOL_SIZE) || 4
  const blockers = Array.from({ length: poolSize }, () => spawnAsyncTask(1))
  return {
    release: async () => {
      blockers[0].task.release()
      await settledWithin(Promise.all(blockers.map(({ promise }) => promise)))
    },
  }
}

/** Runs `withAbortController` behind a saturated pool, aborting before it can start. */
async function abortQueued(
  signal: AbortSignal,
  abort: () => void | Promise<void>,
) {
  const pool = saturatePool()
  const promise = withAbortController(1, 2, signal)
  await abort()
  await pool.release()
  return settledWithin(promise).then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  )
}

queuedTest(
  'aborting a queued task rejects with the default reason',
  async (t) => {
    const controller = new AbortController()
    const result = await abortQueued(controller.signal, () =>
      controller.abort(),
    )
    t.deepEqual(result, { error: controller.signal.reason })
    t.like(result.error, { name: 'AbortError' })
  },
)

queuedTest('aborting a queued task rejects with a custom reason', async (t) => {
  const controller = new AbortController()
  const reason = { custom: true }
  const result = await abortQueued(controller.signal, () =>
    controller.abort(reason),
  )
  t.is((result as { error: unknown }).error, reason)
})

// `AbortSignal.timeout` is in Node.js from v17.3.
queuedTest.skipIf(typeof AbortSignal?.timeout !== 'function')(
  'a timed out signal rejects a queued task with a TimeoutError',
  async (t) => {
    const signal = AbortSignal.timeout(1)
    const result = await abortQueued(signal, async () => {
      while (!signal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, 5))
      }
    })
    t.like(result, { error: { name: 'TimeoutError' } })
  },
)

// Every task on a signal is kept in one list, and the abort handler stopped at the first
// completed one, skipping the tasks after it.
queuedTest(
  'a signal shared with a completed task still cancels later tasks',
  async (t) => {
    const controller = new AbortController()
    t.is(await withAbortController(1, 2, controller.signal), 3)
    const result = await abortQueued(controller.signal, () =>
      controller.abort(),
    )
    t.deepEqual(result, { error: controller.signal.reason })
  },
)

/**
 * Calls `withAbortController` with an already-aborted signal while every pool thread is busy, and
 * waits for it to settle before freeing them. A task that was queued couldn't settle in time.
 */
async function callAborted(signal: AbortSignal) {
  const pool = saturatePool()
  try {
    return await settledWithin(withAbortController(1, 2, signal), 1000).then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    )
  } finally {
    await pool.release()
  }
}

queuedTest(
  'an already-aborted signal rejects with its reason without queuing the task',
  async (t) => {
    const controller = new AbortController()
    const reason = new Error('already aborted')
    controller.abort(reason)
    t.deepEqual(await callAborted(controller.signal), { error: reason })
  },
)

test.serial.skipIf(Boolean(process.env.WASI_TEST))(
  'an already-aborted duck-typed signal rejects with its reason',
  async (t) => {
    const reason = new Error('already aborted')
    const signal = Object.assign(fakeSignal().signal, { aborted: true, reason })
    t.deepEqual(await callAborted(signal), { error: reason })
  },
)

// Node.js before v17.2 and some polyfills have no `reason`.
test.serial.skipIf(Boolean(process.env.WASI_TEST))(
  'an already-aborted signal without a reason rejects with an AbortError',
  async (t) => {
    const signal = Object.assign(fakeSignal().signal, { aborted: true })
    t.like(await callAborted(signal), {
      error: { name: 'AbortError', message: 'AbortError' },
    })
  },
)

signalTest('a user-assigned onabort is kept and still runs', async (t) => {
  const controller = new AbortController()
  let calls = 0
  const onabort = () => {
    calls++
  }
  controller.signal.onabort = onabort
  await withAbortController(1, 2, controller.signal)
  t.is(controller.signal.onabort, onabort)
  controller.abort()
  t.is(calls, 1)
})

signalTest.skipIf(!getEventListeners)(
  'the abort listener is removed once the task settles',
  async (t) => {
    const controller = new AbortController()
    const promise = withAbortController(1, 2, controller.signal)
    t.is(getEventListeners!(controller.signal, 'abort').length, 1)
    await promise
    t.is(getEventListeners!(controller.signal, 'abort').length, 0)
  },
)

/**
 * Stands in for an `AbortSignal` polyfill on Node.js versions without one. Each method records
 * its listener, then throws the given error if there is one.
 */
function fakeSignal(throws: { add?: unknown; remove?: unknown } = {}) {
  const added: unknown[] = []
  const removed: unknown[] = []
  const signal = {
    aborted: false,
    addEventListener(type: string, listener: unknown) {
      if (type === 'abort') added.push(listener)
      if ('add' in throws) throw throws.add
    },
    removeEventListener(type: string, listener: unknown) {
      if (type === 'abort') removed.push(listener)
      if ('remove' in throws) throw throws.remove
    },
  }
  return { signal: signal as unknown as AbortSignal, added, removed }
}

test.serial(
  'a duck-typed signal gets a listener that is removed on settle',
  async (t) => {
    const { signal, added, removed } = fakeSignal()
    const promise = withAbortController(1, 2, signal)
    t.is(added.length, 1)
    t.is(typeof added[0], 'function')
    t.is(await promise, 3)
    t.deepEqual(removed, added)
  },
)

test.serial.skipIf(Boolean(process.env.WASI_TEST))(
  'a duck-typed signal cancels a queued task',
  async (t) => {
    const { signal, added } = fakeSignal()
    const result = await abortQueued(signal, () =>
      (added[0] as () => void).call(signal),
    )
    // The fake has no `reason`, so this is the fallback.
    t.like(result, { error: { name: 'AbortError', message: 'AbortError' } })
  },
)

test('an object without addEventListener is rejected', (t) => {
  t.throws(() => withAbortController(1, 2, {} as AbortSignal), {
    message: 'Value is not an AbortSignal',
  })
})

test('an object without removeEventListener is rejected', (t) => {
  const signal = { addEventListener() {} } as unknown as AbortSignal
  t.throws(() => withAbortController(1, 2, signal), {
    message: 'Value is not an AbortSignal',
  })
})

// A throwing `addEventListener` drops the signal with that exception pending. Removing the
// listener must still happen, and must not lose either exception.
test('an exception pending when the signal drops is thrown after removing the listener', (t) => {
  const added = new Error('added')
  const { signal, removed } = fakeSignal({ add: added })
  t.throws(() => withAbortController(1, 2, signal), { is: added })
  t.is(removed.length, 1)
})

test('removeEventListener throwing during a pending exception gives a SuppressedError', (t) => {
  const added = new Error('added')
  const removing = new Error('removing')
  const { signal } = fakeSignal({ add: added, remove: removing })
  const error = t.throws(() => withAbortController(1, 2, signal)) as Error & {
    error: unknown
    suppressed: unknown
  }
  t.is(error.name, 'SuppressedError')
  t.is(error.error, removing)
  t.is(error.suppressed, added)
  // Node.js has `SuppressedError` from v24.
  const SuppressedErrorCtor = (globalThis as { SuppressedError?: Function })
    .SuppressedError
  if (SuppressedErrorCtor) t.true(error instanceof SuppressedErrorCtor)
})

test('without a global SuppressedError, an Error with the same fields is thrown', (t) => {
  const global = globalThis as { SuppressedError?: unknown }
  const original = Object.getOwnPropertyDescriptor(global, 'SuppressedError')
  delete global.SuppressedError
  try {
    const added = new Error('added')
    const removing = new Error('removing')
    const { signal } = fakeSignal({ add: added, remove: removing })
    const error = t.throws(() => withAbortController(1, 2, signal))
    t.true(error instanceof Error)
    t.like(error, {
      name: 'SuppressedError',
      error: removing,
      suppressed: added,
    })
  } finally {
    if (original) Object.defineProperty(global, 'SuppressedError', original)
  }
})

// With nothing pending, a throwing `removeEventListener` is left to propagate. When the task
// settles, that makes it an uncaught exception, so it runs in a worker.
test.serial.skipIf(Boolean(process.env.WASI_TEST))(
  'removeEventListener throwing when the task settles is an uncaught exception',
  async (t) => {
    const result = await runInWorker(`
      ${LOAD_IN_WORKER}
      const native = load()
      const uncaught = []
      process.on('uncaughtException', (error) => uncaught.push(error.message))
      const signal = {
        addEventListener() {},
        removeEventListener() {
          throw new Error('removing')
        },
      }
      native.withAbortController(1, 2, signal).then((value) => {
        setImmediate(() => parentPort.postMessage({ value, uncaught }))
      })
    `)
    t.deepEqual(result, { value: 3, uncaught: ['removing'] })
  },
)

// The listener reads its state from its own callback data, never from `this`.
test.serial('the listener ignores its receiver', async (t) => {
  const { signal, added } = fakeSignal()
  await withAbortController(1, 2, signal)
  t.notThrows(() => (added[0] as () => void).call({}))
})
