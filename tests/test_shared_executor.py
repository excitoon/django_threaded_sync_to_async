import unittest

import asgiref.sync

import django_threaded_sync_to_async


class TestSharedExecutor(unittest.IsolatedAsyncioTestCase):
    async def testSimple(self):
        async with django_threaded_sync_to_async.SharedExecutor("common") as executor:
            pass

        with self.subTest(same_name=True):
            async with django_threaded_sync_to_async.SharedExecutor("common") as another_executor:
                self.assertIs(executor, another_executor)

        with self.subTest(same_name=False):
            async with django_threaded_sync_to_async.SharedExecutor("specific") as specific_executor:
                self.assertIsNot(executor, specific_executor)

    async def testMaxTasks(self):
        async with django_threaded_sync_to_async.SharedExecutor("common", max_tasks=2, max_workers=3) as executor:
            # TODO Make proper test on `max_tasks`.
            self.assertEqual(await asgiref.sync.sync_to_async(lambda: 42)(), 42)
