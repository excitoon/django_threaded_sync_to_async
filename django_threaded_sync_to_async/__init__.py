import concurrent.futures
import contextlib
import contextvars
import functools
import threading

import asgiref.sync


_reentrant_patch_lock = threading.Lock()


@contextlib.contextmanager
def reentrant_patch(obj, attr, value):
    """
    Makes time-aware patch on the attribute of the object without locking like in `unittest.mock.patch`, the context
    will leak system-wide.
    However, if no `await` happens after obtaining the context, and no threads are getting the same attribute,
    it guarantees that the attribute will have the desired value.
    Effectively guarantees to restore original value after all contexts are destroyed.
    No protection from interleaving foreign code doing same.
    """

    with _reentrant_patch_lock:
        contexts = getattr(obj, f"__{attr}__contexts__", {})
        if not contexts:
            contexts[1] = getattr(obj, attr)
            setattr(obj, f"__{attr}__contexts__", contexts)
        context_id = len(contexts) + 1
        contexts[context_id] = value
        setattr(obj, attr, value)

    yield

    with _reentrant_patch_lock:
        last_context_id = next(reversed(contexts))
        del contexts[context_id]
        if last_context_id == context_id:
            setattr(obj, attr, next(reversed(contexts.values())))
        if len(contexts) == 1:
            delattr(obj, f"__{attr}__contexts__")


_one_time_patch_lock = threading.Lock()


@contextlib.contextmanager
def one_time_patch(obj, attr, value):
    """
    More lightweight implementation, only sets the attribute once — in outer context.
    Effectively guarantees to restore original value after all contexts are destroyed.
    """

    with _one_time_patch_lock:
        if not hasattr(obj, f"__{attr}__value__"):
            setattr(obj, f"__{attr}__value__", getattr(obj, attr))
            setattr(obj, attr, value)
        setattr(obj, f"__{attr}__count__", getattr(obj, f"__{attr}__count__", 0) + 1)

    yield

    with _one_time_patch_lock:
        count = getattr(obj, f"__{attr}__count__") - 1
        setattr(obj, f"__{attr}__count__", count)
        if not count:
            setattr(obj, attr, getattr(obj, f"__{attr}__value__"))
            delattr(obj, f"__{attr}__value__")
            delattr(obj, f"__{attr}__count__")


_current_executor = contextvars.ContextVar("current_executor", default=None)
_max_tasks_semaphore = contextvars.ContextVar("max_tasks_semaphore", default=None)


async def _sync_to_async_call(self, orig, *args, **kwargs):
    if (executor := _current_executor.get()) is not None:
        self = asgiref.sync.SyncToAsync(self.func, thread_sensitive=False, executor=executor)

    else:
        """
        The task is called outside of executor's scope (or in different context).
        """

    if _max_tasks_semaphore.get() is not None:
        with _max_tasks_semaphore.get():
            return await orig(self, *args, **kwargs)

    else:
        return await orig(self, *args, **kwargs)


@contextlib.contextmanager
def _set_context_variable(variable, value):
    token = variable.set(value)
    yield
    variable.reset(token)


@contextlib.contextmanager
def _use_executor(executor):
    with _set_context_variable(_current_executor, executor):
        # Can be replaced by a single call to `setattr(asgiref.sync.SyncToAsync, "__call__", new_call)`
        # if we don't care about restoring everything back.
        new_call = functools.partialmethod(_sync_to_async_call, asgiref.sync.SyncToAsync.__call__)
        with one_time_patch(asgiref.sync.SyncToAsync, "__call__", new_call):
            yield executor


@contextlib.asynccontextmanager
async def Executor(*args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(*args, **kwargs) as executor:
        with _use_executor(executor):
            yield executor


_shared_executors = {}
_shared_executors_lock = threading.Lock()


@contextlib.asynccontextmanager
async def SharedExecutor(name, *args, max_tasks=None, **kwargs):
    with _shared_executors_lock:
        if name in _shared_executors:
            executor = _shared_executors[name]
            if "max_workers" in kwargs:
                executor._max_workers = max(kwargs["max_workers"], executor._max_workers)
        else:
            kwargs.setdefault("thread_name_prefix", name)
            executor = _shared_executors[name] = concurrent.futures.ThreadPoolExecutor(*args, **kwargs)

    with _set_context_variable(_max_tasks_semaphore, max_tasks and threading.Semaphore(max_tasks)):
        with _use_executor(executor):
            yield executor
