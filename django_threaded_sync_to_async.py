import asyncio
import concurrent.futures
import contextlib
import contextvars
import functools
import threading
import time

import asgiref.sync


_reentrant_patch_lock = threading.Lock()


@contextlib.contextmanager
def reentrant_patch(obj, attr, value):
    """
    This is a time-aware patching without locking like in `unittest.mock.patch`, the context will leak system-wide.
    However, if no `await` happens after obtaining the context, and no threads are getting the same attribute,
    it guarantees that the attribute will have the desired value.
    Effectively guarantees to restore original value after all contexts are destroyed.
    No protection from interleaving foreign code doing same.
    """

    with _reentrant_patch_lock:
        contexts = getattr(obj, f"__{attr}__contexts__", {})
        if not contexts:
            setattr(obj, f"__{attr}__contexts__", contexts)
        context_id = len(contexts) + 1
        contexts[context_id] = getattr(obj, attr)
        setattr(obj, attr, value)

    yield

    with _reentrant_patch_lock:
        if next(reversed(contexts)) == context_id:
            setattr(obj, attr, contexts[context_id])
        del contexts[context_id]
        if not contexts:
            delattr(obj, f"__{attr}__contexts__")


_one_time_patch_lock = threading.Lock()


@contextlib.contextmanager
def one_time_patch(obj, attr, value):
    """
    More lightweight implementation, only sets the attribute once — in outer context.
    Effectively guarantees to restore original value after all contexts are destroyed.
    """

    if not hasattr(obj, f"__{attr}__patched__"):
        with _one_time_patch_lock:
            old_value = getattr(obj, f"__{attr}__patched__", ...)
            if old_value == ...:
                setattr(obj, f"__{attr}__patched__", getattr(obj, attr))
                setattr(obj, attr, value)

    yield

    if old_value == ...:
        with _one_time_patch_lock:
            setattr(obj, attr, getattr(obj, f"__{attr}__patched__"))
            delattr(obj, f"__{attr}__patched__")


async def sync_to_async_call(self, orig, *args, **kwargs):
    if (executor := get_current_executor()) is None:
        # The task is called outside of executor's scope (or in different context).
        return await orig(self, *args, **kwargs)

    else:
        new_self = asgiref.sync.SyncToAsync(self.func, thread_sensitive=False, executor=executor)

        try:
            print(f"Started {self.func.__name__}({list(args)}, {kwargs})")
            r = await orig(new_self, *args, **kwargs)
        finally:
            print(f"Ended {self.func.__name__}({list(args)}, {kwargs})")
        return r


_current_executor = contextvars.ContextVar("current_executor", default=None)


@contextlib.contextmanager
def set_current_executor(value):
    # This is almost the same thing as `reentrant_patch()`.
    token = _current_executor.set(value)
    yield
    _current_executor.reset(token)


def get_current_executor():
    return _current_executor.get()


@contextlib.asynccontextmanager
async def SyncToAsyncThreadPoolExecutor(*args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(*args, **kwargs) as executor:
        with set_current_executor(executor):
            # It can be replaced by a single call to `setattr(obj, attr, value)` if we don't care about restoring everything back.
            with one_time_patch(asgiref.sync.SyncToAsync, "__call__", functools.partialmethod(sync_to_async_call, asgiref.sync.SyncToAsync.__call__)):
                yield executor


async def amain():
    print("--- Hey")
    await test()
    print("--- Second test")
    await test2()
    print("--- Bye")


def long_call(arg):
    print(f"1 from {threading.current_thread().name}")
    time.sleep(1)
    print(f"2 from {threading.current_thread().name}")
    time.sleep(1)
    print(f"3 from {threading.current_thread().name}")


async def four_calls():
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    return await asyncio.gather(a, b, c, d)


async def test():
    async with SyncToAsyncThreadPoolExecutor(thread_name_prefix="thread", max_workers=3) as executor:
        await four_calls()


# N.B. `contextlib.asynccontextmanager` only works as decorator since Python 3.10.
@SyncToAsyncThreadPoolExecutor(thread_name_prefix="thread", max_workers=3)
async def test2():
    await four_calls()


asyncio.run(amain())
