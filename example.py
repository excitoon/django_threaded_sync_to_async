import asyncio
import threading
import time

import asgiref.sync
import django_threaded_sync_to_async


async def amain():
    print("--- Hey")
    await test()
    print("--- Second test")
    await test2()
    print("--- Bye")


def long_call(arg):
    print(f"Started long_call({arg})")
    print(f"1 from {threading.current_thread().name}")
    time.sleep(1)
    print(f"2 from {threading.current_thread().name}")
    time.sleep(1)
    print(f"3 from {threading.current_thread().name}")
    print(f"Ended long_call({arg})")


async def four_calls():
    a = asgiref.sync.sync_to_async(long_call)(1)
    b = asgiref.sync.sync_to_async(long_call)(2)
    c = asgiref.sync.sync_to_async(long_call)(3)
    d = asgiref.sync.sync_to_async(long_call)(4)
    return await asyncio.gather(a, b, c, d)


async def test():
    async with django_threaded_sync_to_async.Executor(thread_name_prefix="thread", max_workers=3) as executor:
        await four_calls()


# N.B. `contextlib.asynccontextmanager` only works as decorator since Python 3.10.
@django_threaded_sync_to_async.Executor(thread_name_prefix="thread", max_workers=3)
async def test2():
    await four_calls()


asyncio.run(amain())
