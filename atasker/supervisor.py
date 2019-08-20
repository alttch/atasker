__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.2.20"

import threading
import multiprocessing
import time
import logging
import asyncio
import uuid

from concurrent.futures import CancelledError

TASK_LOW = 200
TASK_NORMAL = 100
TASK_HIGH = 50
TASK_CRITICAL = 0

RQ_SCHEDULER = 1
RQ_TASK = 2

TT_COROUTINE = 0
TT_THREAD = 1
TT_MP = 2

logger = logging.getLogger('atasker/supervisor')

default_poll_delay = 0.1

thread_pool_default_size = multiprocessing.cpu_count()
mp_pool_default_size = multiprocessing.cpu_count()
default_reserve_normal = 5
default_reserve_high = 5

default_timeout_warning = 5
default_timeout_critical = 10


class TaskInfo:

    def __init__(self, tt, task_id, priority):
        self.tt = tt
        self.id = task_id
        self.priority = priority
        self.time_queued = None
        self.time_started = None


class TaskSupervisor:

    timeout_message = 'Task {} started in {:.3f} seconds. ' + \
            'Increase pool size or decrease number of workers'

    def __init__(self):

        self.poll_delay = default_poll_delay

        self.timeout_warning = default_timeout_warning
        self.timeout_warning_func = None
        self.timeout_critical = default_timeout_critical
        self.timeout_critical_func = None

        self._active_threads = set()
        self._active_threads_by_t = set()
        self._waiting_threads_by_t = set()
        self._active_mps = set()
        self._active = False
        self._main_loop_active = False
        self._started = threading.Event()
        self._lock = threading.Lock()
        self._max_threads = {}
        self._max_mps = {}
        self._schedulers = {}
        self._thread_queue = {TASK_LOW: [], TASK_NORMAL: [], TASK_HIGH: []}
        self._mp_queue = {TASK_LOW: [], TASK_NORMAL: [], TASK_HIGH: []}
        self._task_info = {}
        self.default_async_executor_loop = None
        self.mp_pool = None
        self.daemon = False

        self.set_thread_pool(pool_size=thread_pool_default_size,
                             reserve_normal=default_reserve_normal,
                             reserve_high=default_reserve_high)

    def set_thread_pool(self, **kwargs):
        for p in ['pool_size', 'reserve_normal', 'reserve_high']:
            if p in kwargs:
                setattr(self, 'thread_' + p, int(kwargs[p]))
        self._max_threads[TASK_LOW] = self.thread_pool_size
        self._max_threads[
            TASK_NORMAL] = self.thread_pool_size + self.thread_reserve_normal
        self._max_threads[TASK_HIGH] = self.thread_pool_size + \
                self.thread_reserve_normal + self.thread_reserve_high

    def set_mp_pool(self, **kwargs):
        for p in ['pool_size', 'reserve_normal', 'reserve_high']:
            if p in kwargs:
                setattr(self, 'mp_' + p, int(kwargs[p]))
        self._max_mps[TASK_LOW] = self.mp_pool_size
        self._max_mps[TASK_NORMAL] = self.mp_pool_size + self.mp_reserve_normal
        self._max_mps[TASK_HIGH] = self.mp_pool_size + \
                self.mp_reserve_normal + self.mp_reserve_high
        if not self.mp_pool:
            self.create_mp_pool(processes=self._max_mps[TASK_HIGH])

    def _higher_queues_busy(self, tt, task_priority):
        if task_priority == TASK_NORMAL:
            if tt == TT_THREAD:
                return len(self._thread_queue[TASK_HIGH]) > 0
            elif tt == TT_MP:
                return len(self._mp_queue[TASK_HIGH]) > 0
        elif task_priority == TASK_LOW:
            if tt == TT_THREAD:
                return len(self._thread_queue[TASK_NORMAL]) > 0 or \
                        len(self._thread_queue[TASK_HIGH]) > 0
            elif tt == TT_MP:
                return len(self._mp_queue[TASK_NORMAL]) > 0 or \
                        len(self._mp_queue[TASK_HIGH]) > 0
        else:
            return False

    def put_task(self,
                 task,
                 priority=TASK_NORMAL,
                 delay=None,
                 tt=TT_THREAD,
                 task_id=None):
        if task_id is None:
            task_id = str(uuid.uuid4())
        if not self._started.is_set():
            return
        if tt == TT_THREAD:
            task._atask_id = task_id
        ti = TaskInfo(tt, task_id, priority)
        ti.tt = tt,
        ti.time_queued = time.time(),
        with self._lock:
            self._task_info[task_id] = ti
            if tt == TT_THREAD:
                self._waiting_threads_by_t.add(task)
        asyncio.run_coroutine_threadsafe(self._Q.put(
            (RQ_TASK, (tt, task_id, task, priority, delay), time.time())),
                                         loop=self.event_loop)
        return task_id

    def create_mp_pool(self, *args, **kwargs):
        if args or kwargs:
            self.mp_pool = multiprocessing.Pool(*args, **kwargs)
        else:
            self.mp_pool = multiprocessing.Pool(
                processes=multiprocessing.cpu_count())

    def register_scheduler(self, scheduler):
        if not self._started.is_set():
            return False
        asyncio.run_coroutine_threadsafe(self._Q.put(
            (RQ_SCHEDULER, scheduler, time.time())),
                                         loop=self.event_loop)
        return True

    def register_sync_scheduler(self, scheduler):
        with self._lock:
            self._schedulers[scheduler] = None
        return True

    def unregister_sync_scheduler(self, scheduler):
        with self._lock:
            try:
                del self._schedulers[scheduler]
                return True
            except:
                return False

    def unregister_scheduler(self, scheduler):
        with self._lock:
            if scheduler not in self._schedulers:
                return False
            else:
                self._schedulers[scheduler][1].cancel()
                del self._schedulers[scheduler]
                return True

    def _get_active_count(self, tt):
        if tt == TT_THREAD:
            return len(self._active_threads)
        elif tt == TT_MP:
            return len(self._active_mps)

    def get_stats(self, tt=None):

        class SupervisorStats:
            pass

        result = SupervisorStats()
        with self._lock:
            for p in ['pool_size', 'reserve_normal', 'reserve_high']:
                if tt == TT_THREAD or tt is None:
                    setattr(result, 'thread_' + p, getattr(self, 'thread_' + p))
                if self.mp_pool and (tt == TT_MP or tt is None):
                    setattr(result, 'mp_' + p, getattr(self, 'mp_' + p))
            if tt == TT_THREAD or tt is None:
                result.thread_tasks = list(self._active_threads)
                result.threads_active = list(self._active_threads_by_t)
                result.threads_waiting = list(self._waiting_threads_by_t)
                result.thread_tasks_count = len(result.thread_tasks)
                result.thread_queue = self._thread_queue.copy()
            if tt == TT_MP or tt is None:
                result.mp_tasks = list(self._active_mps)
                result.mp_tasks_count = len(result.mp_tasks)
                result.mp_queue = self._mp_queue.copy()
            result.task_info = []
            for n, v in self._task_info.items():
                if tt is None or v.tt == tt:
                    result.task_info.append(v)
        return result

    async def _start_task(self,
                          tt,
                          task_id,
                          task,
                          task_priority=TASK_NORMAL,
                          time_put=None,
                          delay=None):
        if not self._active: return
        if tt == TT_THREAD:
            pool_size = self.thread_pool_size
        elif tt == TT_MP:
            pool_size = self.mp_pool_size
        self._lock.acquire()
        try:
            if task_priority != TASK_CRITICAL and pool_size:
                if tt == TT_THREAD:
                    q = self._thread_queue[task_priority]
                    mx = self._max_threads[task_priority]
                elif tt == TT_MP:
                    q = self._mp_queue[task_priority]
                    mx = self._max_mps[task_priority]
                q.append(task_id)
                while self._active and \
                        (self._get_active_count(tt) >= mx \
                        or q[0] != task_id or \
                        self._higher_queues_busy(tt, task_priority)):
                    self._lock.release()
                    await asyncio.sleep(self.poll_delay)
                    self._lock.acquire()
                q.pop(0)
                if not self._active:
                    return
            if tt == TT_THREAD:
                self._active_threads.add(task_id)
                self._waiting_threads_by_t.remove(task)
                self._active_threads_by_t.add(task)
                logger.debug(
                    'new task {}: {}, thread pool size: {} / {}'.format(
                        task_id, task, len(self._active_threads),
                        self.thread_pool_size))
            elif tt == TT_MP:
                self._active_mps.add(task_id)
                logger.debug('new task {}: {}, mp pool size: {} / {}'.format(
                    task_id, task, len(self._active_mps), self.mp_pool_size))
        finally:
            try:
                self._lock.release()
            except:
                pass
        if delay:
            await asyncio.sleep(delay)
        self._task_info[task_id].time_started = time.time()
        if self._active:
            if tt == TT_THREAD:
                task.start()
            elif tt == TT_MP:
                kw = task[2].copy() if task[2] else {}
                kw['_task_id'] = task_id
                self.mp_pool.apply_async(task[0], task[1], kw, task[3])
        time_started = time.time()
        time_spent = time_started - time_put
        if time_spent > self.timeout_critical:
            logger.critical(self.timeout_message.format(task, time_spent))
            if self.timeout_critical_func:
                self.timeout_critical_func(task)
        elif time_spent > self.timeout_warning:
            logger.warning(self.timeout_message.format(task, time_spent))
            if self.timeout_warning_func:
                self.timeout_warning_func(task)
        if tt == TT_THREAD:
            task.time_started = time_started

    def mark_task_completed(self, task=None, task_id=None, tt=None):
        with self._lock:
            if tt == TT_THREAD or (not task and not task_id) or isinstance(
                    task, threading.Thread):
                if task_id and task_id in self._active_threads:
                    task = self._active_threads[task_id]
                elif task is None and task_id is None:
                    task = threading.current_thread()
                if task in self._active_threads_by_t:
                    self._active_threads_by_t.remove(task)
                    self._active_threads.remove(task._atask_id)
                    logger.debug(
                        'removed task {}: {}, thread pool size: {} / {}'.format(
                            task._atask_id, task, len(self._active_threads),
                            self.thread_pool_size))
                    del self._task_info[task._atask_id]
            else:
                if task is None: task = task_id
                if task in self._active_mps:
                    self._active_mps.remove(task)
                    logger.debug(
                        'removed task {}: {} mp pool size: {} / {}'.format(
                            task, task, len(self._active_mps),
                            self.mp_pool_size))
                del self._task_info[task]
        return True

    def start(self):
        self._active = True
        self._main_loop_active = True
        t = threading.Thread(name='supervisor_event_loop',
                             target=self._start_event_loop,
                             daemon=self.daemon)
        t.start()
        self._started.wait()

    def block(self):
        while self._active:
            time.sleep(0.1)

    async def _main_loop(self):
        self._Q = asyncio.queues.Queue()
        self._started.set()
        logger.info('supervisor event loop started')
        while self._main_loop_active:
            data = await self._Q.get()
            try:
                if data is None: break
                r, res, t_put = data
                if r == RQ_SCHEDULER:
                    logger.debug('Supervisor: new scheduler {}'.format(res))
                    scheduler_task = self.event_loop.create_task(res.loop())
                    if hasattr(res, 'extra_loops'):
                        for l in res.extra_loops:
                            self.event_loop.create_task(getattr(res, l)())
                    with self._lock:
                        self._schedulers[res] = (res, scheduler_task)
                elif r == RQ_TASK:
                    logger.debug('Supervisor: new task {}'.format(res))
                    tt, task_id, target, priority, delay = res
                    self.event_loop.create_task(
                        self._start_task(tt, task_id, target, priority, t_put,
                                         delay))
            finally:
                self._Q.task_done()
        logger.info('supervisor event loop finished')

    def _start_event_loop(self):
        if self._active:
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)
            mp = ', mp pool: {} + {} RN + {} RH'.format(
                self.mp_pool_size, self.mp_reserve_normal,
                self.mp_reserve_high) if hasattr(self, 'mp_pool_size') else ''
            logger.info('supervisor started, thread pool: ' +
                        '{} + {} RN + {} RH{}'.format(
                            self.thread_pool_size, self.thread_reserve_normal,
                            self.thread_reserve_high, mp))
            try:
                self.event_loop.run_until_complete(self._main_loop())
            except CancelledError:
                logger.warning('supervisor loop had active tasks')

    def _cancel_all_tasks(self):
        with self._lock:
            for task in asyncio.Task.all_tasks(loop=self.event_loop):
                task.cancel()

    def _stop_schedulers(self, wait=True):
        with self._lock:
            schedulers = self._schedulers.copy()
        for s in schedulers:
            s.stop(wait=wait)

    def stop(self, wait=True, stop_schedulers=True, cancel_tasks=False):
        self._active = False
        if stop_schedulers:
            self._stop_schedulers(True if wait else False)
            logger.debug('schedulers stopped')
        if cancel_tasks:
            self._cancel_all_tasks()
            logger.debug('remaining tasks canceled')
        if isinstance(wait, bool):
            to_wait = None
        else:
            to_wait = time.time() + wait
        if wait is True or to_wait:
            logger.debug('waiting for tasks to finish')
            while True:
                if not self._active_threads:
                    break
                if to_wait and time.time() > to_wait:
                    logger.warning(
                        'wait timeout, skipping, hope threads will finish')
                    break
                time.sleep(self.poll_delay)
        logger.debug('stopping event loop')
        self._main_loop_active = False
        asyncio.run_coroutine_threadsafe(self._Q.put(None),
                                         loop=self.event_loop)
        if wait is True or to_wait:
            while True:
                if to_wait and time.time() > to_wait:
                    logger.warning('wait timeout, canceling all tasks')
                    self._cancel_all_tasks()
                    break
                else:
                    can_break = True
                    for t in asyncio.Task.all_tasks(self.event_loop):
                        if not t.cancelled() and not t.done():
                            can_break = False
                            break
                    if can_break: break
                time.sleep(self.poll_delay)
        self._started.clear()
        logger.info('supervisor stopped')
