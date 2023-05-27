import unittest

import django_threaded_sync_to_async


class TestOneTimePatch(unittest.TestCase):
    def testSimple(self):
        class Dummy:
            x = 1

        o = Dummy()
        fields = dir(o)

        with self.subTest(inside=[]):
            self.assertEqual(o.x, 1)
        with django_threaded_sync_to_async.one_time_patch(o, "x", 2):
            with self.subTest(inside=["c1"]):
                self.assertEqual(o.x, 2)
            with django_threaded_sync_to_async.one_time_patch(o, "x", 3):
                with self.subTest(inside=["c1", "c2"]):
                    self.assertEqual(o.x, 2)
            with self.subTest(inside=["c1"]):
                self.assertEqual(o.x, 2)
        with self.subTest(inside=[]):
            self.assertEqual(o.x, 1)

        with self.subTest(fields_are_same=True):
            self.assertEqual(dir(o), fields)

    def testReentrant(self):
        class Dummy:
            x = 1

        o = Dummy()
        fields = dir(o)

        c1 = django_threaded_sync_to_async.one_time_patch(o, "x", 2)
        c2 = django_threaded_sync_to_async.one_time_patch(o, "x", 3)

        with self.subTest(inside=[]):
            self.assertEqual(o.x, 1)
        c1.__enter__()
        with self.subTest(inside=["c1"]):
            self.assertEqual(o.x, 2)
        c2.__enter__()
        with self.subTest(inside=["c1", "c2"]):
            self.assertEqual(o.x, 2)
        c1.__exit__(None, None, None)
        with self.subTest(inside=["c2"]):
            self.assertEqual(o.x, 2)
        c2.__exit__(None, None, None)
        with self.subTest(inside=[]):
            self.assertEqual(o.x, 1)

        with self.subTest(fields_are_same=True):
            self.assertEqual(dir(o), fields)
