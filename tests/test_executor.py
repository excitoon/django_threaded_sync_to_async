import asyncio
import contextlib
import threading
import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    async def testConcurrent(self):
        cv = threading.Condition()
        workers = 50
        timeout = 0.1

        def long_call(threads):
            threads.add(threading.current_thread().name)

            def notify_cv():
                with cv:
                    if len(threads) == workers:
                        cv.notify_all()

            with cv:
                notify = threading.Thread(target=notify_cv)
                notify.start()
                return cv.wait_for(lambda: len(threads) == workers, timeout)

        @asgiref.sync.sync_to_async
        def decorated_long_call(threads):
            return long_call(threads)

        @contextlib.asynccontextmanager
        async def empty(**kwargs):
            yield

        for parallel, context in ((False, empty), (True, django_threaded_sync_to_async.Executor)):
            for decorated, function in ((False, asgiref.sync.sync_to_async(long_call)), (True, decorated_long_call)):
                with self.subTest(parallel=parallel, decorated=decorated):
                    threads = set()
                    async with context(max_workers=workers):
                        ff = [asyncio.create_task(function(threads)) for _ in range(workers)]
                        try:
                            for c in asyncio.as_completed(ff, timeout=timeout):
                                self.assertEqual(await c, parallel)
                        except asyncio.TimeoutError:
                            for f in ff:
                                f.cancel()
                            if parallel:
                                self.assertEqual("Exception", "occurred")
                        else:
                            if not parallel:
                                self.assertEqual("No", "exception")
