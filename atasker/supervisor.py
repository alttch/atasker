__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.4.5"

import threading
import multiprocessing
import time
import logging
import asyncio
import uuid

from concurrent.futures import CancelledError

debug = False

TASK_LOW = 200
TASK_NORMAL = 100
TASK_HIGH = 50
TASK_CRITICAL = 0

RQ_SCHEDULER = 1

TT_COROUTINE = 0
TT_THREAD = 1
TT_MP = 2

TASK_STATUS_QUEUED = 0
TASK_STATUS_DELAYED = 2
TASK_STATUS_STARTED = 100
TASK_STATUS_COMPLETED = 200
TASK_STATUS_CANCELED = -1

logger = logging.getLogger('atasker')

default_poll_delay = 0.1

thread_pool_default_size = multiprocessing.cpu_count()
mp_pool_default_size = multiprocessing.cpu_count()
default_reserve_normal = 5
default_reserve_high = 5

default_timeout_warning = 5
default_timeout_critical = 10

_priorities = {
    TASK_LOW: 'TASK_LOW',
    TASK_NORMAL: 'TASK_NORMAL',
    TASK_HIGH: 'TASK_HIGH',
    TASK_CRITICAL: 'TASK_CRITICAL'
}


class Task:

    def __init__(self, tt, task_id, priority, task, delay=None, worker=None):
        self.id = task_id
        self.tt = tt
        self.task = task
        self.priority = priority
        self.time_queued = None
        self.time_started = None
        self.status = TASK_STATUS_QUEUED
        self.delay = delay
        self.worker = worker
        self.started = threading.Event()
        self.completed = threading.Event()
        self.result = None

    def __cmp__(self, other):
        return cmp(self.priority, other.priority) if \
                other is not None else 1

    def __lt__(self, other):
        return (self.priority < other.priority) if \
                other is not None else True

    def __gt__(self, other):
        return (self.priority > other.priority) if \
                other is not None else True

    def is_started(self):
        return self.started.is_set()

    def is_completed(self):
        return self.completed.is_set()

    def mark_started(self):
        self.status = TASK_STATUS_STARTED
        self.started.set()

    def mark_completed(self):
        self.status = TASK_STATUS_COMPLETED
        self.completed.set()


class ALoop:

    def __init__(self, name=None):
        self.name = name if name else str(uuid.uuid4())
        self._active = False
        self.daemon = False
        self.poll_delay = default_poll_delay
        self.thread = None
        self._started = threading.Event()

    async def _coro_task(self, task):
        task.time_queued = time.time()
        task.time_started = task.time_queued
        task.mark_started()
        task.result = await task.task
        task.mark_completed()

    def background_task(self, coro):
        if not self.is_active():
            raise RuntimeError('aloop {} is not active'.format(self.name))
        task = Task(TT_COROUTINE, str(uuid.uuid4()), TASK_NORMAL, coro)
        asyncio.run_coroutine_threadsafe(self._coro_task(task), loop=self.loop)
        return task

    def run(self, coro):
        if not self.is_active():
            raise RuntimeError('aloop {} is not active'.format(self.name))
        future = asyncio.run_coroutine_threadsafe(coro, loop=self.loop)
        return future.result()

    def start(self):
        if not self._active:
            self._started.clear()
            t = threading.Thread(name='supervisor_aloop_{}'.format(self.name),
                                 target=self._start_loop)
            t.setDaemon(self.daemon)
            t.start()
            self._started.wait()

    def get_loop(self):
        return None if not self._active else self.loop

    def _start_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._loop())
        except CancelledError:
            logger.warning('aloop {} had active tasks'.format(self.name))

    async def _loop(self):
        self._stop_event = asyncio.Event()
        self.thread = threading.current_thread()
        self._active = True
        logger.info('aloop {} started'.format(self.name))
        self._started.set()
        await self._stop_event.wait()
        logger.info('aloop {} finished'.format(self.name))

    def _cancel_all_tasks(self):
        for task in asyncio.Task.all_tasks(loop=self.loop):
            task.cancel()

    async def _set_stop_event(self):
        self._stop_event.set()

    def stop(self, wait=True, cancel_tasks=False):
        if self._active:
            if cancel_tasks:
                self._cancel_all_tasks()
                if debug:
                    logger.debug('aloop {} remaining tasks canceled'.format(
                        self.name))
            if isinstance(wait, bool):
                to_wait = None
            else:
                to_wait = time.perf_counter() + wait
            self._active = False
            asyncio.run_coroutine_threadsafe(self._set_stop_event(),
                                             loop=self.loop)
            while True:
                if to_wait and time.perf_counter() > to_wait:
                    logger.warning(
                        'aloop {} wait timeout, canceling all tasks'.format(
                            self.name))
                    self._cancel_all_tasks()
                    break
                else:
                    can_break = True
                    for t in asyncio.Task.all_tasks(self.loop):
                        if not t.cancelled() and not t.done():
                            can_break = False
                            break
                    if can_break: break
                time.sleep(self.poll_delay)
        if wait and self.thread:
            self.thread.join()

    def is_active(self):
        return self._active


class TaskSupervisor:

    timeout_message = 'task {}: {} started in {:.3f} seconds. ' + \
            'Increase pool size or decrease number of workers'

    def __init__(self):

        self.poll_delay = default_poll_delay

        self.timeout_warning = default_timeout_warning
        self.timeout_warning_func = None
        self.timeout_critical = default_timeout_critical
        self.timeout_critical_func = None

        self._active_threads = set()
        self._active_threads_by_t = set()
        self._active_mps = set()
        self._active = False
        self._main_loop_active = False
        self._started = threading.Event()
        self._lock = threading.Lock()
        self._max_threads = {}
        self._max_mps = {}
        self._schedulers = {}
        self._tasks = {}
        self._Qt = {}
        self._Qmp = {}
        self.default_aloop = None
        self.mp_pool = None
        self.daemon = False
        self._processors_stopped = {}
        self.aloops = {}

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
            setattr(self, 'mp_' + p, int(kwargs.get(p, 0)))
        self._max_mps[TASK_LOW] = self.mp_pool_size
        self._max_mps[TASK_NORMAL] = self.mp_pool_size + self.mp_reserve_normal
        self._max_mps[TASK_HIGH] = self.mp_pool_size + \
                self.mp_reserve_normal + self.mp_reserve_high
        if not self.mp_pool:
            self.create_mp_pool(processes=self._max_mps[TASK_HIGH])

    def timeout_warning_func(task):
        pass

    def timeout_critical_func(task):
        pass

    def _higher_queues_busy(self, tt, task_priority):
        if tt == TT_THREAD:
            q = self._Qt
        elif tt == TT_MP:
            q = self._Qmp
        if task_priority == TASK_NORMAL:
            return not q[TASK_HIGH].empty()
        elif task_priority == TASK_LOW:
            return not q[TASK_HIGH].empty() or not q[TASK_NORMAL].empty()
        else:
            return False

    def put_task(self,
                 task,
                 priority=TASK_NORMAL,
                 delay=None,
                 tt=TT_THREAD,
                 task_id=None,
                 worker=None):
        if not self._started.is_set() or not self._active or task is None:
            return
        if task_id is None:
            task_id = str(uuid.uuid4())
        if tt == TT_THREAD:
            task._atask_id = task_id
        ti = Task(tt, task_id, priority, task, delay, worker)
        ti.time_queued = time.time()
        with self._lock:
            self._tasks[task_id] = ti
        if priority == TASK_CRITICAL:
            self.mark_task_started(ti)
            asyncio.run_coroutine_threadsafe(self._start_task(ti),
                                             loop=self.event_loop)
        else:
            if tt == TT_THREAD:
                q = self._Qt[priority]
            else:
                q = self._Qmp[priority]
            asyncio.run_coroutine_threadsafe(q.put(ti), loop=self.event_loop)
        return ti

    async def _task_processor(self, queue, priority, tt):
        logger.debug('task processor {}/{} started'.format(tt, priority))
        while True:
            task = await queue.get()
            if task is None: break
            if tt == TT_THREAD:
                pool_size = self.thread_pool_size
            elif tt == TT_MP:
                pool_size = self.mp_pool_size
            self._lock.acquire()
            try:
                if tt == TT_THREAD:
                    mx = self._max_threads[priority]
                elif tt == TT_MP:
                    mx = self._max_mps[priority]
                while (self._get_active_count(tt) >= mx or
                       self._higher_queues_busy(tt, priority)):
                    self._lock.release()
                    await asyncio.sleep(self.poll_delay)
                    self._lock.acquire()
            finally:
                self._lock.release()
            self.mark_task_started(task)
            self.event_loop.create_task(self._start_task(task))
        logger.debug('task processor {}/{} finished'.format(
            tt, _priorities[priority]))
        self._processors_stopped[(tt, priority)].set()

    def get_task(self, task_id):
        with self._lock:
            return self._tasks.get(task_id)

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

    def create_aloop(self, name, daemon=False, start=True, default=False):
        if name == '__supervisor__':
            raise RuntimeError('Name "__supervisor__" is reserved')
        with self._lock:
            if name in self.aloops:
                logger.error('loop {} already exists'.format(name))
                return False
        l = ALoop(name)
        l.daemon = daemon
        l.poll_delay = self.poll_delay
        with self._lock:
            self.aloops[name] = l
            if start:
                l.start()
            if default:
                self.set_default_aloop(l)
        return l

    def set_default_aloop(self, aloop):
        self.default_aloop = aloop

    def get_aloop(self, name=None, default=True):
        with self._lock:
            if name is not None:
                return self.aloops.get(name)
            elif default:
                return self.default_aloop

    def start_aloop(self, name):
        with self._lock:
            if name not in self.aloops:
                logger.error('loop {} not found'.format(name))
                return False
            else:
                self.aloops[name].start()
                return True

    def stop_aloop(self, name, wait=True, cancel_tasks=False, _lock=True):
        if _lock:
            self._lock.acquire()
        try:
            if name not in self.aloops:
                logger.error('loop {} not found'.format(name))
                return False
            else:
                self.aloops[name].stop(wait=wait, cancel_tasks=cancel_tasks)
                return True
        finally:
            if _lock:
                self._lock.release()

    def get_info(self, tt=None, aloops=True, schedulers=True):

        class SupervisorInfo:
            pass

        result = SupervisorInfo()
        with self._lock:
            result.active = self._active
            result.started = self._started.is_set()
            for p in ['pool_size', 'reserve_normal', 'reserve_high']:
                if tt == TT_THREAD or tt is None or tt is False:
                    setattr(result, 'thread_' + p, getattr(self, 'thread_' + p))
                if self.mp_pool and (tt == TT_MP or tt is None or tt is False):
                    setattr(result, 'mp_' + p, getattr(self, 'mp_' + p))
            if tt == TT_THREAD or tt is None or tt is False:
                if not tt is False:
                    result.thread_tasks = list(self._active_threads)
                    result.threads_active = list(self._active_threads_by_t)
                result.thread_tasks_count = len(self._active_threads)
            if tt == TT_MP or tt is None or tt is False:
                if not tt is False:
                    result.mp_tasks = list(self._active_mps)
                result.mp_tasks_count = len(self._active_mps)
            if aloops:
                result.aloops = self.aloops.copy()
            if schedulers:
                result.schedulers = self._schedulers.copy()
            if tt != False:
                result.tasks = {}
                for n, v in self._tasks.items():
                    if tt is None or v.tt == tt:
                        result.tasks[n] = v
        return result

    def get_aloops(self):
        with self._lock:
            return self.aloops.copy()

    def get_schedulers(self):
        with self._lock:
            return self._schedulers.copy()

    def get_tasks(self, tt=None):
        result = {}
        with self._lock:
            for n, v in self._tasks.items():
                if tt is None or v.tt == tt:
                    result[n] = v
        return result

    def mark_task_started(self, task):
        with self._lock:
            if task.tt == TT_THREAD:
                self._active_threads.add(task.id)
                self._active_threads_by_t.add(task.task)
                if debug:
                    logger.debug(
                        'new task {}: {}, {} thread pool size: {} / {}'.format(
                            task.id, task.task, _priorities[task.priority],
                            len(self._active_threads), self.thread_pool_size))
            elif task.tt == TT_MP:
                self._active_mps.add(task.id)
                if debug:
                    logger.debug(
                        'new task {}: {}, {} mp pool size: {} / {}'.format(
                            task.id, task.task, _priorities[task.priority],
                            len(self._active_mps), self.mp_pool_size))

    async def _start_task(self, task):
        with self._lock:
            task.time_started = time.time()
            if not task.delay:
                task.mark_started()
        if task.delay:
            task.status = TASK_STATUS_DELAYED
            await asyncio.sleep(task.delay)
            task.mark_started()
        if task.tt == TT_THREAD:
            task.task.start()
        elif task.tt == TT_MP:
            kw = task.task[2].copy() if task.task[2] else {}
            kw['_task_id'] = task.id
            self.mp_pool.apply_async(task.task[0], task.task[1], kw,
                                     task.task[3])
        time_spent = task.time_started - task.time_queued
        if time_spent > self.timeout_critical:
            logger.critical(
                self.timeout_message.format(task_id, task.task, time_spent))
            self.timeout_critical_func(task)
        elif time_spent > self.timeout_warning:
            logger.warning(
                self.timeout_message.format(task_id, task.task, time_spent))
            self.timeout_warning_func(task)

    def mark_task_completed(self, task=None, task_id=None, tt=None):
        with self._lock:
            if isinstance(task, Task):
                task_id = task.id
                tt = task.tt
            if tt == TT_THREAD or (not task and not task_id) or isinstance(
                    task, threading.Thread):
                if task_id and task_id in self._active_threads:
                    task = self._active_threads[task_id]
                elif task is None and task_id is None:
                    task = threading.current_thread()
                if task in self._active_threads_by_t:
                    self._active_threads_by_t.remove(task)
                    self._active_threads.remove(task._atask_id)
                    if debug:
                        logger.debug(
                            'removed task {}: {}, thread pool size: {} / {}'.
                            format(task._atask_id, task,
                                   len(self._active_threads),
                                   self.thread_pool_size))
                    self._tasks[task._atask_id].mark_completed()
                    del self._tasks[task._atask_id]
            else:
                if task is None or isinstance(task, Task): task = task_id
                if task in self._active_mps:
                    self._active_mps.remove(task)
                    if debug:
                        logger.debug(
                            'removed task {}: {} mp pool size: {} / {}'.format(
                                task, task, len(self._active_mps),
                                self.mp_pool_size))
                self._tasks[task].mark_completed()
                del self._tasks[task]
        return True

    def start(self, daemon=None):
        self._active = True
        self._main_loop_active = True
        t = threading.Thread(
            name='supervisor_event_loop',
            target=self._start_event_loop,
            daemon=daemon if daemon is not None else self.daemon)
        t.start()
        self._started.wait()

    def block(self):
        while self._started.is_set():
            time.sleep(0.1)

    async def _launch_scheduler_loop(self, scheduler):
        try:
            t = self.event_loop.create_task(scheduler.loop())
            with self._lock:
                self._schedulers[scheduler] = (scheduler, t)
            if hasattr(scheduler, 'extra_loops'):
                for l in scheduler.extra_loops:
                    self.event_loop.create_task(getattr(scheduler, l)())
            await t
        except CancelledError:
            pass
        except Exception as e:
            logger.error(e)

    async def _main_loop(self):
        self._Q = asyncio.queues.Queue()
        for p in (TASK_LOW, TASK_NORMAL, TASK_HIGH):
            self._Qt[p] = asyncio.queues.Queue()
            self._processors_stopped[(TT_THREAD, p)] = asyncio.Event()
            self.event_loop.create_task(
                self._task_processor(self._Qt[p], p, TT_THREAD))
        if self.mp_pool:
            for p in (TASK_LOW, TASK_NORMAL, TASK_HIGH):
                self._Qmp[p] = asyncio.queues.Queue()
                self._processors_stopped[(TT_MP, p)] = asyncio.Event()
                self.event_loop.create_task(
                    self._task_processor(self._Qmp[p], p, TT_MP))
        self._started.set()
        logger.info('supervisor event loop started')
        while self._main_loop_active:
            data = await self._Q.get()
            try:
                if data is None: break
                r, res, t_put = data
                if r == RQ_SCHEDULER:
                    if debug:
                        logger.debug('new scheduler {}'.format(res))
                    self.event_loop.create_task(
                        self._launch_scheduler_loop(res))
            finally:
                self._Q.task_done()
        for i, t in self._processors_stopped.items():
            await t.wait()
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
            if debug: logger.debug('schedulers stopped')
        with self._lock:
            for i, l in self.aloops.items():
                self.stop_aloop(i,
                                wait=wait,
                                cancel_tasks=cancel_tasks,
                                _lock=False)
            if debug: logger.debug('async loops stopped')
        if isinstance(wait, bool):
            to_wait = None
        else:
            to_wait = time.perf_counter() + wait
        if (to_wait or wait is True) and not cancel_tasks:
            while True:
                with self._lock:
                    if not self._tasks:
                        break
                time.sleep(self.poll_delay)
                if to_wait and time.perf_counter() > to_wait: break
        if debug: logger.debug('no task in queues')
        if to_wait or wait is True:
            if debug: logger.debug('waiting for tasks to finish')
            while True:
                if not self._active_threads:
                    break
                if to_wait and time.perf_counter() > to_wait:
                    logger.warning(
                        'wait timeout, skipping, hope threads will finish')
                    break
                time.sleep(self.poll_delay)
        if cancel_tasks:
            self._cancel_all_tasks()
            if debug: logger.debug('remaining tasks canceled')
        if to_wait or wait is True:
            while True:
                with self._lock:
                    if (not self._active_threads and not self._active_mps) or (
                            to_wait and time.perf_counter() > to_wait):
                        break
                time.sleep(self.poll_delay)
        if debug: logger.debug('no active threads/mps')
        if debug: logger.debug('stopping event loop')
        asyncio.run_coroutine_threadsafe(self._Q.put(None),
                                         loop=self.event_loop)
        for p in (TASK_LOW, TASK_NORMAL, TASK_HIGH):
            asyncio.run_coroutine_threadsafe(self._Qt[p].put(None),
                                             loop=self.event_loop)
        if self.mp_pool:
            for p in (TASK_LOW, TASK_NORMAL, TASK_HIGH):
                asyncio.run_coroutine_threadsafe(self._Qmp[p].put(None),
                                                 loop=self.event_loop)
        self._main_loop_active = False
        if wait is True or to_wait:
            while True:
                if to_wait and time.perf_counter() > to_wait:
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
        with self._lock:
            for i, v in self._tasks.items():
                v.status = TASK_STATUS_CANCELED
        self._started.clear()
        logger.info('supervisor stopped')
