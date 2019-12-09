#!/usr/bin/env python3

__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.7.8"

from pathlib import Path

import sys
import logging
import unittest
import time
import threading

from types import SimpleNamespace

result = SimpleNamespace(g=None,
                         function_collection=0,
                         task_collection=0,
                         background_task_annotated=None,
                         background_task_thread=None,
                         background_task_thread_critical=None,
                         background_task_mp=None,
                         background_worker=0,
                         wait1=None,
                         wait2=None,
                         wait3=None,
                         background_interval_worker=0,
                         background_interval_worker_async_ex=0,
                         background_queue_worker=0,
                         background_event_worker=0,
                         locker_success=False,
                         locker_failed=False,
                         test_aloop=None,
                         test_aloop_background_task=None,
                         async_js1=0,
                         async_js2=0)

sys.path.insert(0, Path(__file__).absolute().parents[1].as_posix())


def wait():
    time.sleep(0.1)


from atasker import task_supervisor, background_task, background_worker
from atasker import TT_MP, TASK_CRITICAL, wait_completed

from atasker import FunctionCollection, TaskCollection, g

from atasker import Locker, set_debug


class Test(unittest.TestCase):

    def test_g(self):

        @background_task
        def f():
            result.g = g.get('test', 222)
            g.set('ttt', 333)

        g.set('test', 1)
        g.clear('test')
        g.set('test_is', g.has('test'))
        self.assertFalse(g.get('test_is'))
        g.set('test', 999)
        f()
        wait()
        self.assertIsNone(g.get('ttt'))
        self.assertEqual(result.g, 222)

    def test_function_collection(self):

        f = FunctionCollection()

        @f
        def f1():
            result.function_collection += 1

        @f
        def f2():
            result.function_collection += 2

        f.run()
        self.assertEqual(result.function_collection, 3)

    def test_task_collection(self):

        f = TaskCollection()

        @f
        def f1():
            result.task_collection += 1

        @f
        def f2():
            result.task_collection += 2

        f.run()
        self.assertEqual(result.task_collection, 3)

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

    def test_background_task_thread_critical(self):

        def t(a, x):
            result.background_task_thread = a + x

        background_task(t, priority=TASK_CRITICAL)(3, x=4)
        wait()
        self.assertEqual(result.background_task_thread, 7)

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

        task_supervisor.default_aloop = None
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
        t.put_threadsafe(2)
        t.put_threadsafe(3)
        t.put_threadsafe(4)
        wait()
        t.stop()
        self.assertEqual(result.background_queue_worker, 9)

    def test_background_event_worker(self):

        @background_worker(e=True)
        def t(**kwargs):
            result.background_event_worker += 1

        t.start()
        t.trigger_threadsafe()
        wait()
        t.trigger_threadsafe()
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

    def test_locker(self):

        with_lock = Locker(mod='test (broken is fine!)',
                           relative=False,
                           timeout=0.5)

        @with_lock
        def test_locker():
            result.locker_failed = True

        def locker_ok():
            result.locker_success = True

        with_lock.critical = locker_ok
        with_lock.lock.acquire()
        test_locker()
        self.assertTrue(result.locker_success)
        self.assertFalse(result.locker_failed)

    def test_supervisor(self):
        result = task_supervisor.get_info()

        self.assertEqual(result.thread_tasks_count, 0)
        self.assertEqual(result.mp_tasks_count, 0)

    def test_aloop(self):

        @background_worker(interval=0.02)
        async def t(**kwargs):
            result.test_aloop = threading.current_thread().getName()

        task_supervisor.create_aloop('test1', default=True)
        t.start()
        wait()
        t.stop()
        self.assertEqual(result.test_aloop, 'supervisor_default_aloop_test1')

    def test_result_async(self):

        def t1():
            return 555

        aloop = task_supervisor.create_aloop('test3')
        t = background_task(t1, loop='test3')()
        wait_completed([t])
        self.assertEqual(t.result, 555)

    def test_result_thread(self):

        def t1():
            return 777

        def t2():
            return 111

        task1 = background_task(t1)()
        task2 = background_task(t2)()
        self.assertEqual(wait_completed((task1, task2)), [777, 111])

    def test_result_mp(self):

        from mp import test2

        t = background_task(test2, tt=TT_MP)()
        self.assertEqual(wait_completed(t), 999)

    def test_aloop_run(self):

        async def t1():
            result.test_aloop_background_task = 1

        async def t2(x):
            return x * 2

        a = task_supervisor.create_aloop('test2')
        t = background_task(t1, loop='test2')()
        wait_completed([t])
        self.assertEqual(result.test_aloop_background_task, 1)
        self.assertEqual(a.run(t2(2)), 4)

    def test_wait_completed(self):

        @background_task
        def t1():
            time.sleep(0.1)
            result.wait1 = 1

        @background_task
        def t2():
            time.sleep(0.2)
            result.wait2 = 2

        @background_task
        def t3():
            time.sleep(0.3)
            result.wait3 = 3

        tasks = [t1(), t2(), t3()]
        wait_completed(tasks)
        self.assertEqual(result.wait1 + result.wait2 + result.wait3, 6)

    def test_async_job_scheduler(self):

        async def test1():
            result.async_js1 += 1

        async def test2():
            result.async_js2 += 1

        task_supervisor.create_aloop('jobs')
        task_supervisor.create_async_job_scheduler('default',
                                                   aloop='jobs',
                                                   default=True)
        j1 = task_supervisor.create_async_job(target=test1, interval=0.01)
        j2 = task_supervisor.create_async_job(target=test2, interval=0.01)

        time.sleep(0.1)

        task_supervisor.cancel_async_job(job=j2)

        r1 = result.async_js1
        r2 = result.async_js2

        self.assertGreater(r1, 9)
        self.assertGreater(r2, 9)

        time.sleep(0.1)

        self.assertLess(r1, result.async_js1)
        self.assertEqual(r2, result.async_js2)


task_supervisor.set_thread_pool(pool_size=20, reserve_normal=5, reserve_high=5)
task_supervisor.set_mp_pool(pool_size=20, reserve_normal=5, reserve_high=5)

if __name__ == '__main__':
    try:
        if sys.argv[1] == 'debug':
            logging.basicConfig(level=logging.DEBUG)
            set_debug()
    except:
        pass
    task_supervisor.start()
    task_supervisor.poll_delay = 0.01
    test_suite = unittest.TestLoader().loadTestsFromTestCase(Test)
    test_result = unittest.TextTestRunner().run(test_suite)
    task_supervisor.stop(wait=3)
    sys.exit(not test_result.wasSuccessful())
