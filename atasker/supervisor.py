__author__ = 'Altertech Group, https://www.altertech.com/'
__copyright__ = 'Copyright (C) 2018-2019 Altertech Group'
__license__ = 'Apache License 2.0'
__version__ = "0.7.3"

import threading
import multiprocessing
import time
import logging
import asyncio
import uuid

from concurrent.futures import CancelledError, ThreadPoolExecutor
from aiosched import AsyncJobScheduler

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

thread_pool_default_size = multiprocessing.cpu_count() * 5
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

    def __init__(self,
                 tt,
                 task_id=None,
                 priority=TASK_NORMAL,
                 target=None,
                 args=(),
                 kwargs={},
                 callback=None,
                 delay=None,
                 worker=None,
                 _send_task_id=True):
        self.id = task_id if task_id is not None else str(uuid.uuid4())
        self.tt = tt
        self.target = target
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        if _send_task_id: self.kwargs['_task_id'] = self.id
        self.callback = callback
        self.priority = priority
        self.time_queued = None
        self.time_started = None
        self._tstarted = None
        self._tqueued = None
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

    def __init__(self, name=None, supervisor=None):
        self.name = name if name else str(uuid.uuid4())
        self._active = False
        self.daemon = False
        self.poll_delay = default_poll_delay
        self.thread = None
        self.supervisor = supervisor
        self._started = threading.Event()

    async def _coro_task(self, task):
        task.time_queued = time.time()
        task.time_started = task.time_queued
        task._tstarted = time.perf_counter()
        task._tqueued = task._tstarted
        task.mark_started()
        task.result = await task.target
        task.mark_completed()

    def background_task(self, coro):
        if not self.is_active():
            raise RuntimeError('{} aloop {} is not active'.format(
                self.supervisor.id, self.name))
        task = Task(TT_COROUTINE, str(uuid.uuid4()), TASK_NORMAL, coro)
        asyncio.run_coroutine_threadsafe(self._coro_task(task), loop=self.loop)
        return task

    def run(self, coro):
        if not self.is_active():
            raise RuntimeError('{} aloop {} is not active'.format(
                self.supervisor.id, self.name))
        future = asyncio.run_coroutine_threadsafe(coro, loop=self.loop)
        return future.result()

    def start(self):
        if not self._active:
            self._started.clear()
            t = threading.Thread(name='supervisor_{}_aloop_{}'.format(
                self.supervisor.id, self.name),
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
            logger.warning('supervisor {} aloop {} had active tasks'.format(
                self.supervisor.id, self.name))

    async def _loop(self):
        self._stop_event = asyncio.Event()
        self.thread = threading.current_thread()
        self._active = True
        logger.info('supervisor {} aloop {} started'.format(
            self.supervisor.id, self.name))
        self._started.set()
        await self._stop_event.wait()
        logger.info('supervisor {} aloop {} finished'.format(
            self.supervisor.id, self.name))

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
                    logger.debug(
                        'supervisor {} aloop {} remaining tasks canceled'.
                        format(self.supervisor.id, self.name))
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
                        ('supervisor {} aloop {} wait timeout, ' +
                         'canceling all tasks').format(self.supervisor.id,
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

    timeout_message = '{supervisor_id} task {task_id}: ' + \
            '{target} started in {time_spent:.3f} seconds. ' + \
            'Increase pool size or decrease number of workers'

    def __init__(self, supervisor_id=None):

        self.poll_delay = default_poll_delay

        self.timeout_warning = default_timeout_warning
        self.timeout_warning_func = None
        self.timeout_critical = default_timeout_critical
        self.timeout_critical_func = None
        self.id = supervisor_id if supervisor_id else str(uuid.uuid4())

        self._active_threads = set()
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
        self.default_async_job_scheduler = None
        self.mp_pool = None
        self.daemon = False
        self._processors_stopped = {}
        self.aloops = {}
        self.async_job_schedulers = {}

        self.set_thread_pool(pool_size=thread_pool_default_size,
                             reserve_normal=default_reserve_normal,
                             reserve_high=default_reserve_high,
                             max_size=None)

    def set_thread_pool(self, **kwargs):
        for p in ['pool_size', 'reserve_normal', 'reserve_high']:
            if p in kwargs:
                setattr(self, 'thread_' + p, int(kwargs[p]))
        self._max_threads[TASK_LOW] = self.thread_pool_size
        self._max_threads[
            TASK_NORMAL] = self.thread_pool_size + self.thread_reserve_normal
        thc= self.thread_pool_size + \
                self.thread_reserve_normal + self.thread_reserve_high
        self._max_threads[TASK_HIGH] = thc
        self._prespawn_threads = kwargs.get('min_size', 0)
        max_size = kwargs.get('max_size')
        if not max_size: max_size = thc
        if max_size < self._prespawn_threads:
            raise ValueError(
                'min pool size ({}) can not be larger than max ({})'.format(
                    self._prespawn_threads, max_size))
        self.thread_pool = ThreadPoolExecutor(
            max_workers=max_size,
            thread_name_prefix='supervisor_{}_pool'.format(self.id))
        if self._max_threads[TASK_HIGH] > max_size:
            logger.warning(
                ('supervisor {} executor thread pool max size ({}) is ' +
                 'lower than reservations ({})').format(
                     self.id, max_size, self._max_threads[TASK_HIGH]))

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
                 target,
                 args=(),
                 kwargs={},
                 callback=None,
                 priority=TASK_NORMAL,
                 delay=None,
                 tt=TT_THREAD,
                 task_id=None,
                 worker=None,
                 _send_task_id=True):
        if not self._started.is_set() or not self._active or target is None:
            return
        ti = Task(tt,
                  task_id,
                  priority=priority,
                  target=target,
                  args=args,
                  kwargs=kwargs,
                  callback=callback,
                  delay=delay,
                  worker=worker,
                  _send_task_id=_send_task_id)
        ti.time_queued = time.time()
        ti._tqueued = time.perf_counter()
        with self._lock:
            self._tasks[ti.id] = ti
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
        logger.debug('supervisor {} task processor {}/{} started'.format(
            self.id, tt, priority))
        while True:
            task = await queue.get()
            if task is None: break
            if tt == TT_THREAD:
                pool_size = self.thread_pool_size
            elif tt == TT_MP:
                pool_size = self.mp_pool_size
            if pool_size:
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
        logger.debug('supervisor {} task processor {}/{} finished'.format(
            self.id, tt, _priorities[priority]))
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

    def create_async_job(self, scheduler=None, **kwargs):
        if scheduler is None:
            scheduler = self.default_async_job_scheduler
        elif isinstance(scheduler, str):
            scheduler = self.async_job_schedulers[scheduler]
        return scheduler.create_threadsafe(**kwargs)

    def cancel_async_job(self, scheduler=None, job=None):
        if job:
            if scheduler is None:
                scheduler = self.default_async_job_scheduler
            elif isinstance(scheduler, str):
                scheduler = self.async_job_schedulers[scheduler]
            scheduler.cancel(job)
        else:
            logger.warning('supervisor {} async job cancellation ' +
                           'requested but job not specified'.format(self.id))

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
                logger.error('supervisor {} loop {} already exists'.format(
                    self.id, name))
                return False
        l = ALoop(name, supervisor=self)
        l.daemon = daemon
        l.poll_delay = self.poll_delay
        with self._lock:
            self.aloops[name] = l
            if start:
                l.start()
            if default:
                self.set_default_aloop(l)
        return l

    def create_async_job_scheduler(self,
                                   name,
                                   aloop=None,
                                   start=True,
                                   default=False):
        """
        Create async job scheduler (aiosched.scheduler)

        ALoop must always be specified or default ALoop defined
        """
        if name == '__supervisor__':
            raise RuntimeError('Name "__supervisor__" is reserved')
        with self._lock:
            if name in self.async_job_schedulers:
                logger.error(
                    'supervisor {} async job_scheduler {} already exists'.
                    format(self.id, name))
                return False
        l = AsyncJobScheduler(name)
        if aloop is None:
            aloop = self.default_aloop
        elif not isinstance(aloop, ALoop):
            aloop = self.get_aloop(aloop)
        loop = aloop.get_loop()
        with self._lock:
            self.async_job_schedulers[name] = l
            if default:
                self.set_default_async_job_scheduler(l)
        if start:
            l.set_loop(loop)
            l._aloop = aloop
            aloop.background_task(l.scheduler_loop())
        else:
            l.set_loop(loop)
        return l

    def set_default_aloop(self, aloop):
        self.default_aloop = aloop

    def set_default_async_job_scheduler(self, scheduler):
        self.default_async_job_scheduler = scheduler

    def get_aloop(self, name=None, default=True):
        with self._lock:
            if name is not None:
                return self.aloops.get(name)
            elif default:
                return self.default_aloop

    def start_aloop(self, name):
        with self._lock:
            if name not in self.aloops:
                logger.error('supervisor {} loop {} not found'.format(
                    self.id, name))
                return False
            else:
                self.aloops[name].start()
                return True

    def stop_aloop(self, name, wait=True, cancel_tasks=False, _lock=True):
        if _lock:
            self._lock.acquire()
        try:
            if name not in self.aloops:
                logger.error('supervisor {} loop {} not found'.format(
                    self.id, name))
                return False
            else:
                self.aloops[name].stop(wait=wait, cancel_tasks=cancel_tasks)
                return True
        finally:
            if _lock:
                self._lock.release()

    def get_info(self,
                 tt=None,
                 aloops=True,
                 schedulers=True,
                 async_job_schedulers=True):

        class SupervisorInfo:
            pass

        result = SupervisorInfo()
        with self._lock:
            result.id = self.id
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
                result.thread_tasks_count = len(self._active_threads)
            if tt == TT_MP or tt is None or tt is False:
                if not tt is False:
                    result.mp_tasks = list(self._active_mps)
                result.mp_tasks_count = len(self._active_mps)
            if aloops:
                result.aloops = self.aloops.copy()
            if schedulers:
                result.schedulers = self._schedulers.copy()
            if async_job_schedulers:
                result.async_job_schedulers = self.async_job_schedulers.copy()
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
                if debug:
                    logger.debug(
                        ('supervisor {} new task {}: {}, {}' +
                         ' thread pool size: {} / {}').format(
                             self.id, task.id, task.target,
                             _priorities[task.priority],
                             len(self._active_threads), self.thread_pool_size))
            elif task.tt == TT_MP:
                self._active_mps.add(task.id)
                if debug:
                    logger.debug(('supervisor {} new task {}: {}, {}' +
                                  ' mp pool size: {} / {}').format(
                                      self.id, task.id, task.target,
                                      _priorities[task.priority],
                                      len(self._active_mps), self.mp_pool_size))

    async def _start_task(self, task):
        with self._lock:
            task.time_started = time.time()
            task._tstarted = time.perf_counter()
            if not task.delay:
                task.mark_started()
        if task.delay:
            task.status = TASK_STATUS_DELAYED
            await asyncio.sleep(task.delay)
            task.mark_started()
        if task.tt == TT_THREAD:
            self.thread_pool.submit(task.target, *task.args, **task.kwargs)
        elif task.tt == TT_MP:
            self.mp_pool.apply_async(task.target, task.args, task.kwargs,
                                     task.callback)
        time_spent = task._tstarted - task._tqueued
        if time_spent > self.timeout_critical:
            logger.critical(
                self.timeout_message.format(supervisor_id=supervisor_id,
                                            task_id=task_id,
                                            target=task.target,
                                            time_spent=time_spent))
            self.timeout_critical_func(task)
        elif time_spent > self.timeout_warning:
            logger.warning(
                self.timeout_message.format(supervisor_id=supervisor_id,
                                            task_id=task_id,
                                            target=task.target,
                                            time_spent=time_spent))
            self.timeout_warning_func(task)

    def mark_task_completed(self, task=None, task_id=None):
        with self._lock:
            if task is None:
                try:
                    task = self._tasks[task_id]
                except:
                    raise LookupError('supervisor {} task {} not found'.format(
                        self.id, task_id))
            task_id = task.id
            tt = task.tt
            if tt == TT_THREAD:
                if task_id in self._active_threads:
                    self._active_threads.remove(task_id)
                    if debug:
                        logger.debug(('supervisor {} removed task {}:' +
                                      ' {}, thread pool size: {} / {}').format(
                                          self.id, task_id, task,
                                          len(self._active_threads),
                                          self.thread_pool_size))
                    task.mark_completed()
                    del self._tasks[task_id]
            elif tt == TT_MP:
                if task_id in self._active_mps:
                    self._active_mps.remove(task_id)
                    if debug:
                        logger.debug(('supervisor {} removed task {}:' +
                                      ' {} mp pool size: {} / {}').format(
                                          self.id, task_id, task,
                                          len(self._active_mps),
                                          self.mp_pool_size))
                task.mark_completed()
                del self._tasks[task_id]
        return True

    def start(self, daemon=None):

        def _prespawn():
            pass

        self._active = True
        self._main_loop_active = True
        t = threading.Thread(
            name='supervisor_{}_event_loop'.format(self.id),
            target=self._start_event_loop,
            daemon=daemon if daemon is not None else self.daemon)
        t.start()
        for i in range(self._prespawn_threads):
            self.thread_pool.submit(_prespawn)
        self._started.wait()

    def block(self):
        while self._started.is_set():
            time.sleep(0.1)

    async def _launch_scheduler_loop(self, scheduler):
        try:
            t = scheduler.worker_loop.create_task(scheduler.loop())
            with self._lock:
                self._schedulers[scheduler] = (scheduler, t)
            if hasattr(scheduler, 'extra_loops'):
                for l in scheduler.extra_loops:
                    scheduler.worker_loop.create_task(getattr(scheduler, l)())
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
        logger.info('supervisor {} event loop started'.format(self.id))
        while self._main_loop_active:
            data = await self._Q.get()
            try:
                if data is None: break
                r, res, t_put = data
                if r == RQ_SCHEDULER:
                    if debug:
                        logger.debug('supervisor {} new scheduler {}'.format(
                            self.id, res))
                    asyncio.run_coroutine_threadsafe(
                        self._launch_scheduler_loop(res), loop=res.worker_loop)
            finally:
                self._Q.task_done()
        for i, t in self._processors_stopped.items():
            await t.wait()
        logger.info('supervisor {} event loop finished'.format(self.id))

    def _start_event_loop(self):
        if self._active:
            self.event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.event_loop)
            mp = ', mp pool: {} + {} RN + {} RH'.format(
                self.mp_pool_size, self.mp_reserve_normal,
                self.mp_reserve_high) if hasattr(self, 'mp_pool_size') else ''
            logger.info(
                ('supervisor {} started, thread pool: ' +
                 '{} + {} RN + {} RH{}').format(self.id, self.thread_pool_size,
                                                self.thread_reserve_normal,
                                                self.thread_reserve_high, mp))
            try:
                self.event_loop.run_until_complete(self._main_loop())
            except CancelledError:
                logger.warning('supervisor {} loop had active tasks'.format(
                    self.id))

    def _cancel_all_tasks(self):
        with self._lock:
            for task in asyncio.Task.all_tasks(loop=self.event_loop):
                task.cancel()

    def _stop_schedulers(self, wait=True):
        with self._lock:
            schedulers = self._schedulers.copy()
        for s in schedulers:
            s.stop(wait=wait)

    def _stop_async_job_schedulers(self, wait=True):
        with self._lock:
            schedulers = self.async_job_schedulers.copy().items()
        for i, s in schedulers:
            try:
                s.stop(wait=wait)
            except:
                pass

    def stop(self, wait=True, stop_schedulers=True, cancel_tasks=False):
        self._active = False
        if isinstance(wait, bool):
            to_wait = None
        else:
            to_wait = time.perf_counter() + wait
        if stop_schedulers:
            self._stop_async_job_schedulers(wait)
            self._stop_schedulers(True if wait else False)
            if debug:
                logger.debug('supervisor {} schedulers stopped'.format(self.id))
        with self._lock:
            for i, l in self.aloops.items():
                self.stop_aloop(i,
                                wait=wait,
                                cancel_tasks=cancel_tasks,
                                _lock=False)
            if debug:
                logger.debug('supervisor {} async loops stopped'.format(
                    self.id))
        if (to_wait or wait is True) and not cancel_tasks:
            while True:
                with self._lock:
                    if not self._tasks:
                        break
                time.sleep(self.poll_delay)
                if to_wait and time.perf_counter() > to_wait: break
        if debug:
            logger.debug('supervisor {} no task in queues'.format(self.id))
        if to_wait or wait is True:
            if debug:
                logger.debug('supervisor {} waiting for tasks to finish'.format(
                    self.id))
            while True:
                if not self._active_threads:
                    break
                if to_wait and time.perf_counter() > to_wait:
                    logger.warning(
                        ('supervisor {} wait timeout, ' +
                         'skipping, hope threads will finish').format(self.id))
                    break
                time.sleep(self.poll_delay)
        if cancel_tasks:
            self._cancel_all_tasks()
            if debug:
                logger.debug('supervisor {} remaining tasks canceled'.format(
                    self.id))
        if to_wait or wait is True:
            while True:
                with self._lock:
                    if (not self._active_threads and not self._active_mps) or (
                            to_wait and time.perf_counter() > to_wait):
                        break
                time.sleep(self.poll_delay)
        if debug:
            logger.debug('supervisor {} no active threads/mps'.format(self.id))
        if debug:
            logger.debug('supervisor {} stopping event loop'.format(self.id))
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
                    logger.warning(
                        'supervisor {} wait timeout, canceling all tasks'.
                        format(self.id))
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
        logger.info('supervisor {} stopped'.format(self.id))
