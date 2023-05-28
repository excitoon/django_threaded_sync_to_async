import asyncio
import threading
import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    async def testSimple(self):
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

        with self.subTest(parallel=False):
            threads = set()
            ff = [asyncio.create_task(asgiref.sync.sync_to_async(long_call)(threads)) for _ in range(workers)]
            try:
                for c in asyncio.as_completed(ff, timeout=timeout):
                    self.assertEqual(await c, False)
            except asyncio.TimeoutError:
                for f in ff:
                    f.cancel()

        with self.subTest(parallel=True):
            threads = set()
            async with django_threaded_sync_to_async.Executor(thread_name_prefix="T", max_workers=workers):
                ff = [asyncio.create_task(asgiref.sync.sync_to_async(long_call)(threads)) for _ in range(workers)]
                try:
                    for c in asyncio.as_completed(ff, timeout=timeout):
                        self.assertEqual(await c, True)
                except asyncio.TimeoutError:
                    for f in ff:
                        f.cancel()
                    raise
