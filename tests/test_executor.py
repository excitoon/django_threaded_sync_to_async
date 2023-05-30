import asyncio
import concurrent.futures
import contextlib
import threading
import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    async def testConcurrent(self):
        workers = 50
        timeout = 0.1

        def long_call(barrier, threads):
            threads.add(threading.current_thread().name)
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                pass
            return len(threads)

        @asgiref.sync.sync_to_async
        def decorated_long_call(*args):
            return long_call(*args)

        @contextlib.asynccontextmanager
        async def empty(**kwargs):
            # Reset shared executor.
            asgiref.sync.SyncToAsync.single_thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            yield

        for parallel, context in ((False, empty), (True, django_threaded_sync_to_async.Executor)):
            for decorated, function in ((False, asgiref.sync.sync_to_async(long_call)), (True, decorated_long_call)):
                for patcher in (
                    django_threaded_sync_to_async.patch.reentrant,
                    django_threaded_sync_to_async.patch.one_time,
                    django_threaded_sync_to_async.patch.permanent,
                ):
                    with self.subTest(parallel=parallel, decorated=decorated, patcher=patcher.__name__):
                        barrier = threading.Barrier(workers, timeout=timeout * 2 / 3)
                        threads = set()
                        results = set()

                        async with context(max_workers=workers, patcher=patcher):
                            tt = [asyncio.create_task(function(barrier, threads)) for _ in range(workers)]
                            try:
                                try:
                                    for c in asyncio.as_completed(tt, timeout=timeout):
                                        results.add(await c)
                                except:
                                    for t in tt:
                                        t.cancel()
                                    raise
                            except asyncio.TimeoutError:
                                if parallel:
                                    self.assertEqual("Timed", "out")
                            except self.failureException:
                                raise
                            except Exception:
                                self.assertEqual("Exception", "occurred")

                        self.assertEqual(len(threads), workers if parallel else 1)
                        self.assertEqual(results, {workers} if parallel else {1})

    @django_threaded_sync_to_async.Executor()
    async def testDecorator(self):
        self.assertIsNotNone(django_threaded_sync_to_async._current_executor.get())
