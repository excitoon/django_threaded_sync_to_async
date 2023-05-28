## `django_threaded_sync_to_async`

Tools for replacing `sync_to_async` calls to `sync_to_async(thread_sensitive=False, executor=...)`, effectively allowing Django to make calls to database concurrently.

```
pip3 install django_threaded_sync_to_async
```

### `Executor`

```python3
import django_threaded_sync_to_async

async with django_threaded_sync_to_async.Executor(thread_name_prefix="thread", max_workers=3) as executor:
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    await asyncio.gather(a, b, c, d)
```

### `SharedExecutor`

Maintains global dictionary of executors (`concurrent.futures.ThreadPoolExecutor`) accessed by name and allows to limit utilization of executor for a single context.

```python3
import django_threaded_sync_to_async

@django_threaded_sync_to_async.SharedExecutor("common", max_workers=3, max_tasks=2)
def operations():
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    await asyncio.gather(a, b, c, d)
```
