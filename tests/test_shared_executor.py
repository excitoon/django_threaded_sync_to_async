import asyncio
import concurrent.futures
import contextlib
import functools
import threading
import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestSharedExecutor(unittest.IsolatedAsyncioTestCase):
    async def testSimple(self):
        async with django_threaded_sync_to_async.SharedExecutor("simple_common") as executor:
            pass

        with self.subTest(same_name=True):
            async with django_threaded_sync_to_async.SharedExecutor("simple_common") as another_executor:
                self.assertIs(executor, another_executor)

        with self.subTest(same_name=False):
            async with django_threaded_sync_to_async.SharedExecutor("simple_specific") as specific_executor:
                self.assertIsNot(executor, specific_executor)

    async def testMaxTasks(self):
        workers = 10
        timeout = 0.1

        def long_call(barriers, threads):
            threads.add(threading.current_thread().name)
            try:
                next(filter(lambda b: not b.broken, barriers)).wait()
            except threading.BrokenBarrierError:
                pass
            result = len(threads)
            try:
                next(filter(lambda b: not b.broken, barriers)).wait()
            except threading.BrokenBarrierError:
                pass
            threads.discard(threading.current_thread().name)
            return result

        @asgiref.sync.sync_to_async
        def decorated_long_call(*args):
            return long_call(*args)

        @contextlib.asynccontextmanager
        async def empty(name, **kwargs):
            # Reset shared executor.
            asgiref.sync.SyncToAsync.single_thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            yield

        for parallel, context in ((False, empty), (True, django_threaded_sync_to_async.SharedExecutor)):
            for decorated, function in ((False, asgiref.sync.sync_to_async(long_call)), (True, decorated_long_call)):
                for tasks in (workers, workers - 1):
                    for patcher in (
                        django_threaded_sync_to_async.patch.reentrant,
                        django_threaded_sync_to_async.patch.one_time,
                        django_threaded_sync_to_async.patch.permanent,
                    ):
                        with self.subTest(
                            parallel=parallel, decorated=decorated, tasks=tasks, patcher=patcher.__name__
                        ):
                            # In some tests barriers will break, so we need few of them.
                            barriers = [threading.Barrier(workers, timeout=timeout / 3) for _ in range(4)]
                            threads = set()
                            results = set()

                            async with context(
                                f"max_tasks_{parallel}_{decorated}_{tasks}_{patcher.__name__}",
                                max_workers=workers,
                                max_tasks=tasks,
                                patcher=patcher,
                            ):
                                tt = [asyncio.create_task(function(barriers, threads)) for _ in range(workers)]
                                try:
                                    try:
                                        for c in asyncio.as_completed(tt, timeout=timeout):
                                            results.add(await c)
                                    except:
                                        for t in tt:
                                            t.cancel()
                                        raise
                                except asyncio.TimeoutError:
                                    if parallel and tasks == workers:
                                        self.assertEqual("Timed", "out")
                                except Exception:
                                    self.assertEqual("Exception", "occurred")

                            self.assertEqual(results, {tasks} if parallel else {1})

    @django_threaded_sync_to_async.SharedExecutor("decorator")
    async def testDecorator(self):
        self.assertIsNotNone(django_threaded_sync_to_async._current_executor.get())
