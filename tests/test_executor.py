import asyncio
import contextlib
import threading
import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    async def testConcurrent(self):
        workers = 50
        timeout = 0.1

        def long_call(cv, threads):
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
        def decorated_long_call(*args):
            return long_call(*args)

        @contextlib.asynccontextmanager
        async def empty(**kwargs):
            yield

        for parallel, context in ((False, empty), (True, django_threaded_sync_to_async.Executor)):
            for decorated, function in ((False, asgiref.sync.sync_to_async(long_call)), (True, decorated_long_call)):
                with self.subTest(parallel=parallel, decorated=decorated):
                    cv = threading.Condition()
                    threads = set()

                    async with context(max_workers=workers):
                        tt = [asyncio.create_task(function(cv, threads)) for _ in range(workers)]
                        try:
                            for c in asyncio.as_completed(tt, timeout=timeout):
                                self.assertEqual(await c, parallel)
                        except asyncio.TimeoutError:
                            for t in tt:
                                t.cancel()
                            if parallel:
                                self.assertEqual("Exception", "occurred")
                        else:
                            if not parallel:
                                self.assertEqual("No", "exception")

                    self.assertEqual(len(threads), workers if parallel else 1)
