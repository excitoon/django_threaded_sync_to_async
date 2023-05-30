import asyncio
import concurrent.futures
import contextlib
import contextvars
import threading

import asgiref.sync

import django_threaded_sync_to_async.patch


_current_executor = contextvars.ContextVar("current_executor", default=None)
_max_tasks_semaphore = contextvars.ContextVar("max_tasks_semaphore", default=None)
_original_sync_to_async_call = asgiref.sync.SyncToAsync.__call__
_shared_executors = {}
_shared_executors_lock = threading.Lock()


@contextlib.asynccontextmanager
async def _nullcontext():
    # Python 3.9 and earlier.
    yield


async def _sync_to_async_call(self, *args, **kwargs):
    if (executor := _current_executor.get()) is None:
        """
        The task hit the call outside of executor's scope (or in different context).
        """

    else:
        self = asgiref.sync.SyncToAsync(self.func, thread_sensitive=False, executor=executor)

    async with _max_tasks_semaphore.get() or _nullcontext():
        return await _original_sync_to_async_call(self, *args, **kwargs)


@contextlib.contextmanager
def _set_context_variable(variable, value):
    token = variable.set(value)
    yield
    variable.reset(token)


@contextlib.contextmanager
def _use_executor(executor, patcher=None):
    patcher = patcher or django_threaded_sync_to_async.patch.one_time
    # `patch.one_time()` can be replaced with `patch.permanent()` if we don't care about restoring everything back.
    with patcher(asgiref.sync.SyncToAsync, "__call__", _sync_to_async_call):
        with _set_context_variable(_current_executor, executor):
            yield executor


@contextlib.asynccontextmanager
async def Executor(*args, patcher=None, **kwargs):
    with concurrent.futures.ThreadPoolExecutor(*args, **kwargs) as executor:
        with _use_executor(executor, patcher=patcher):
            yield executor


@contextlib.asynccontextmanager
async def SharedExecutor(name, *args, max_tasks=None, patcher=None, **kwargs):
    with _shared_executors_lock:
        if name in _shared_executors:
            executor = _shared_executors[name]
            if "max_workers" in kwargs:
                executor._max_workers = max(kwargs["max_workers"], executor._max_workers)
        else:
            kwargs.setdefault("thread_name_prefix", name)
            executor = _shared_executors[name] = concurrent.futures.ThreadPoolExecutor(*args, **kwargs)

    with _set_context_variable(_max_tasks_semaphore, max_tasks and asyncio.BoundedSemaphore(max_tasks)):
        with _use_executor(executor, patcher=patcher):
            yield executor
