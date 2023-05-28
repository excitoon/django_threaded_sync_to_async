import asyncio
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
        timeout = 0.05

        def long_call(cv, threads, another_threads):
            threads.add(threading.current_thread().name)

            def notify_cv(predicate):
                with cv:
                    if predicate():
                        cv.notify_all()

            with cv:
                notify = threading.Thread(target=notify_cv, args=(lambda: len(threads) == workers,))
                notify.start()
                cv.wait_for(lambda: len(threads) == workers, timeout)

            result = len(threads)
            another_threads.add(threading.current_thread().name)

            with cv:
                notify = threading.Thread(target=notify_cv, args=(lambda: len(another_threads) == workers,))
                notify.start()
                cv.wait_for(lambda: len(another_threads) == workers, timeout)

            threads.discard(threading.current_thread().name)
            another_threads.discard(threading.current_thread().name)
            return result

        @asgiref.sync.sync_to_async
        def decorated_long_call(*args):
            return long_call(*args)

        @contextlib.asynccontextmanager
        async def empty(name, **kwargs):
            yield

        for parallel, context in ((False, empty), (True, django_threaded_sync_to_async.SharedExecutor)):
            for decorated, function in ((False, asgiref.sync.sync_to_async(long_call)), (True, decorated_long_call)):
                for tasks in (workers, workers - 1):
                    # One or two passes are allowed — `tasks` must not be less than `workers/2`.
                    with self.subTest(parallel=parallel, decorated=decorated, tasks=tasks):
                        cv = threading.Condition()
                        threads = set()
                        another_threads = set()

                        async with context(
                            f"max_tasks_{parallel}_{decorated}_{tasks}", max_workers=workers, max_tasks=tasks
                        ):
                            tt = [asyncio.create_task(function(cv, threads, another_threads)) for _ in range(workers)]
                            try:
                                total_timeout = timeout * (2.5 if tasks == workers else 4.5)
                                for c in asyncio.as_completed(tt, timeout=total_timeout):
                                    self.assertIn(await c, (tasks, workers - tasks) if parallel else (1,))
                            except asyncio.TimeoutError:
                                for t in tt:
                                    t.cancel()
                                if parallel:
                                    self.assertEqual("Exception", "occurred")
                            else:
                                if not parallel:
                                    self.assertEqual("No", "exception")
