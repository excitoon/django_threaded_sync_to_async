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


async def sync_to_async_call(self, orig, name, *args, **kwargs):
    executor = thread_local_context_get(name)

    # `reentrant_patch` is an overkill here, but it's fine.
    with reentrant_patch(self, "_thread_sensitive", False):
        with reentrant_patch(self, "_executor", executor):
            try:
                print(f"Started {self.func.__name__}({list(args)}, {kwargs})")
                r = await orig(self, *args, **kwargs)
            finally:
                print(f"Ended {self.func.__name__}({list(args)}, {kwargs})")
            return r


_thread_local_storage = threading.local()

@contextlib.contextmanager
def thread_local_context_set(name, value):
    stack = getattr(_thread_local_storage, name, [])
    if not stack:
        setattr(_thread_local_storage, name, stack)
    stack.append(value)
    yield name
    stack.pop()


def thread_local_context_get(name):
    stack = getattr(_thread_local_storage, name)
    return stack and stack[-1] or None


@contextlib.asynccontextmanager
async def SyncToAsyncThreadPoolExecutor(*args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(*args, **kwargs) as executor:
        with thread_local_context_set("__executors", executor) as name:
            with reentrant_patch(asgiref.sync.SyncToAsync, "__call__", functools.partialmethod(sync_to_async_call, asgiref.sync.SyncToAsync.__call__, name)):
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


async def test():
    async with SyncToAsyncThreadPoolExecutor(thread_name_prefix="thread", max_workers=3) as executor:
        a = asgiref.sync.sync_to_async(long_call)(1)
        b = asgiref.sync.sync_to_async(long_call)(2)
        c = asgiref.sync.sync_to_async(long_call)(3)
        d = asgiref.sync.sync_to_async(long_call)(4)
        await asyncio.gather(a, b, c, d)


@SyncToAsyncThreadPoolExecutor(thread_name_prefix="thread", max_workers=3)
async def test2():
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    await asyncio.gather(a, b, c, d)


asyncio.run(amain())
