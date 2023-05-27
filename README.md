### `django_threaded_sync_to_async`

Under executor context, replaces `sync_to_async` calls to `sync_to_async(thread_sensitive=None, executor=...)`, effectively allowing Django to make calls to database concurrently:

```python3
async with django_threaded_sync_to_async.Executor(thread_name_prefix="thread", max_workers=3) as executor:
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    await asyncio.gather(a, b, c, d)
```
