from pathlib import Path

import sys
import logging
import unittest
import time

from types import SimpleNamespace

result = SimpleNamespace(
    background_task_annotated=None,
    background_task_thread=None,
    background_task_mp=None,
    background_worker=0,
    background_interval_worker=0,
    background_interval_worker_async_ex=0,
    background_queue_worker=0,
    background_event_worker=0)

logging.basicConfig(level=logging.DEBUG)

sys.path.append(Path().absolute().parent.as_posix())


def wait():
    time.sleep(0.1)


from atasker import task_supervisor, background_task, background_worker, TT_MP


class Test(unittest.TestCase):

    def test_background_task_annotated(self):

        @background_task
        def t(a, x):
            result.background_task_annotated = a + x

        t(1, x=2)
        wait()
        self.assertEqual(result.background_task_annotated, 3)

    def test_background_task_thread(self):

        def t(a, x):
            result.background_task_thread = a + x

        background_task(t)(2, x=3)
        wait()
        self.assertEqual(result.background_task_thread, 5)

    def test_background_task_mp(self):

        def callback(res):
            result.background_task_mp = res

        from mp import test_mp
        background_task(test_mp, tt=TT_MP, callback=callback)(3, x=7)
        wait()
        self.assertEqual(result.background_task_mp, 10)

    def test_background_worker(self):

        @background_worker
        def t(**kwargs):
            result.background_worker += 1

        t.start()
        wait()
        t.stop()
        self.assertGreater(result.background_worker, 0)

    def test_background_interval_worker(self):

        @background_worker(interval=0.02)
        def t(**kwargs):
            result.background_interval_worker += 1

        t.start()
        wait()
        t.stop()
        self.assertLess(result.background_interval_worker, 10)
        self.assertGreater(result.background_interval_worker, 4)

    def test_background_interval_worker_async_ex(self):

        @background_worker(interval=0.02)
        async def t(**kwargs):
            result.background_interval_worker_async_ex += 1

        t.start()
        wait()
        t.stop()
        self.assertLess(result.background_interval_worker_async_ex, 10)
        self.assertGreater(result.background_interval_worker_async_ex, 4)

    def test_background_queue_worker(self):

        @background_worker(q=True)
        def t(a, **kwargs):
            result.background_queue_worker += a

        t.start()
        t.put(2)
        t.put(3)
        t.put(4)
        wait()
        t.stop()
        self.assertEqual(result.background_queue_worker, 9)

    def test_background_event_worker(self):

        @background_worker(e=True)
        def t(**kwargs):
            result.background_event_worker += 1

        t.start()
        t.trigger()
        wait()
        t.trigger()
        wait()
        t.stop()
        self.assertEqual(result.background_event_worker, 2)

    def test_background_interval_worker_mp(self):

        from mpworker import TestMPWorker

        t = TestMPWorker(interval=0.02)
        t.start()
        wait()
        t.stop()
        self.assertLess(t.a, 10)
        self.assertGreater(t.a, 4)


task_supervisor.set_thread_pool(pool_size=20, reserve_normal=5, reserve_high=5)
task_supervisor.set_mp_pool(pool_size=20, reserve_normal=5, reserve_high=5)

if __name__ == '__main__':
    task_supervisor.start()
    task_supervisor.poll_delay = 0.01
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    test_result = unittest.TextTestRunner().run(test_suite)
    task_supervisor.stop(wait=2)
    sys.exit(not test_result.wasSuccessful())
