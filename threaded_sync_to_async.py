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
    # No protection from interleaving foreign code doing same.

    with _reentrant_patch_lock:
        contexts = getattr(obj, f"{attr}__contexts", {})
        if not contexts:
            setattr(obj, f"{attr}__contexts", contexts)
        context_id = len(contexts) + 1
        contexts[context_id] = getattr(obj, attr)
        setattr(obj, attr, value)

    yield

    with _reentrant_patch_lock:
        if next(reversed(contexts)) == context_id:
            setattr(obj, attr, contexts[context_id])
        del contexts[context_id]


_sync_to_async_call_lock = threading.Lock()

async def sync_to_async_call(self, orig, *args, **kwargs):
    if (executor := get_current_executor()) is None:
        # If executor is None, then the task is called outside of executor's scope.
        return await orig(self, *args, **kwargs)

    else:
        with _sync_to_async_call_lock:
            clones = getattr(self, "__clones", {})
            if not clones:
                setattr(self, "__clones", clones)
            if id(executor) not in clones:
                clone = clones[id(executor)] = asgiref.sync.SyncToAsync(self.func, thread_sensitive=False, executor=executor)

        try:
            print(f"Started {self.func.__name__}({list(args)}, {kwargs})")
            r = await orig(clone, *args, **kwargs)
        finally:
            print(f"Ended {self.func.__name__}({list(args)}, {kwargs})")
        return r


_current_executor = contextvars.ContextVar('current_executor', default=None)

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
            with reentrant_patch(asgiref.sync.SyncToAsync, "__call__", functools.partialmethod(sync_to_async_call, asgiref.sync.SyncToAsync.__call__)):
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


@SyncToAsyncThreadPoolExecutor(thread_name_prefix="thread", max_workers=3)
async def test2():
    await four_calls()


asyncio.run(amain())
