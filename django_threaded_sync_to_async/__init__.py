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


async def _sync_to_async_call(self, orig, *args, **kwargs):
    if (executor := _get_current_executor()) is not None:
        self = asgiref.sync.SyncToAsync(self.func, thread_sensitive=False, executor=executor)

    else:
        """
        The task is called outside of executor's scope (or in different context).
        """

    return await orig(self, *args, **kwargs)


_current_executor = contextvars.ContextVar("current_executor", default=None)


@contextlib.contextmanager
def _set_current_executor(value):
    token = _current_executor.set(value)
    yield
    _current_executor.reset(token)


def _get_current_executor():
    return _current_executor.get()


@contextlib.asynccontextmanager
async def Executor(*args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(*args, **kwargs) as executor:
        with _set_current_executor(executor):
            # It can be replaced by a single call to `setattr(obj, attr, value)` if we don't care about restoring everything back.
            new_call = functools.partialmethod(_sync_to_async_call, asgiref.sync.SyncToAsync.__call__)
            with one_time_patch(asgiref.sync.SyncToAsync, "__call__", new_call):
                yield executor
