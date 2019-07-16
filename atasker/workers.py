__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.1.7"

import threading
import logging
import uuid
import time
import asyncio
import queue
import types

from atasker import task_supervisor

from atasker import TASK_NORMAL

logger = logging.getLogger('atasker/workers')


class BackgroundWorker:

    # ----- override this -----

    def run(self, *args, **kwargs):
        raise Exception('not implemented')

    def before_start(self):
        pass

    def send_stop_events(self):
        pass

    def after_start(self):
        pass

    def before_stop(self):
        pass

    def after_stop(self):
        pass

    # -----------------------

    def __init__(self, worker_name=None, executor_func=None, **kwargs):
        if executor_func:
            self.run = executor_func
            self._can_use_mp_pool = False
        else:
            self._can_use_mp_pool = not asyncio.iscoroutinefunction(self.run)
        self._current_executor = None
        self._active = False
        self._started = False
        self._stopped = True
        self.daemon = kwargs.get('daemon', True)
        self.priority = kwargs.get('priority', TASK_NORMAL)
        self.o = kwargs.get('o')
        self.on_error = kwargs.get('on_error')
        self.on_error_kwargs = kwargs.get('on_error_kwargs', {})
        self.supervisor = kwargs.get('supervisor', task_supervisor)
        self.poll_delay = kwargs.get('poll_delay', self.supervisor.poll_delay)
        self.set_name(worker_name)
        self._task_args = ()
        self._task_kwargs = {}
        self.start_stop_lock = threading.Lock()
        self._suppress_sleep = False
        self.last_executed = 0
        self._executor_stop_event = threading.Event()

    def set_name(self, name):
        self.name = '_background_worker_%s' % (name if name is not None else
                                               uuid.uuid4())

    def restart(self, *args, **kwargs):
        self.stop(wait=kwargs.get('wait'))
        self.start(*args, **kwargs)

    def is_active(self):
        return self._active

    def is_started(self):
        return self._started

    def error(self, e):
        if self.on_error:
            kwargs = self.on_error_kwargs.copy()
            kwargs['e'] = e
            self.on_error(**kwargs)
        else:
            raise

    def _send_executor_stop_event(self):
        self._executor_stop_event.set()

    def start(self, *args, **kwargs):
        if self._active:
            return False
        self.start_stop_lock.acquire()
        try:
            self.before_start()
            self._active = True
            self._started = False
            self._stopped = False
            kw = kwargs.copy()
            if '_priority' in kw:
                self.priority = kw['_priority']
            self._run_in_mp = isinstance(
                self.run, types.FunctionType
            ) and self.supervisor.mp_pool and self._can_use_mp_pool
            if self._run_in_mp:
                logger.debug(self.name + ' will use mp pool')
            else:
                kw['_worker'] = self
            self.daemon = kwargs.get('daemon', True)
            if not '_worker_name' in kw:
                kw['_worker_name'] = self.name
            if not 'o' in kw:
                kw['o'] = self.o
            self._task_args = args
            self._task_kwargs = kw
            self._start()
            while not self._started:
                time.sleep(self.poll_delay)
            self.after_start()
            return True
        finally:
            self.start_stop_lock.release()

    def _start(self, *args, **kwargs):
        t = threading.Thread(
            name=self.name,
            target=self.loop,
            args=self._task_args,
            kwargs=self._task_kwargs)
        if self.daemon:
            t.setDaemon(True)
        self.supervisor.put_task(t, self.priority)
        while not self._started:
            time.sleep(self.poll_delay)
        self.supervisor.register_sync_scheduler(self)

    def _abort(self):
        self.mark_stopped()
        self.stop(wait=False)

    def _cb_mp(self, result):
        if self.process_result(result) is False:
            self._abort()
        self._current_executor = None
        self._send_executor_stop_event()

    def loop(self, *args, **kwargs):
        self.mark_started()
        while self._active:
            try:
                self.last_executed = time.time()
                if self._run_in_mp:
                    self._current_executor = self.run
                    self.supervisor.mp_pool.apply_async(self.run, args, kwargs,
                                                        self._cb_mp)
                    self._executor_stop_event.wait()
                    self._executor_stop_event.clear()
                else:
                    if self.run(*args, **kwargs) is False:
                        return self._abort()
            except Exception as e:
                self.error(e)
        self.mark_stopped()
        self.supervisor.mark_task_completed()

    def mark_started(self):
        self._started = True
        logger.debug(self.name + ' started')

    def mark_stopped(self):
        self._stopped = True
        logger.debug(self.name + ' stopped')

    def stop(self, wait=True):
        self.start_stop_lock.acquire()
        try:
            self.before_stop()
            self._active = False
            self.send_stop_events()
            if wait:
                self.wait_until_stop()
            self._stop(wait=wait)
            self.after_stop()
        finally:
            self.start_stop_lock.release()

    def _stop(self, **kwargs):
        self.supervisor.unregister_sync_scheduler(self)

    def wait_until_stop(self):
        if isinstance(self._current_executor, threading.Thread):
            try:
                self._current_executor.join()
            except:
                pass
        while not self._stopped:
            time.sleep(self.poll_delay)


class BackgroundAsyncWorker(BackgroundWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.executor_loop = kwargs.get(
            'loop', self.supervisor.default_async_executor_loop)

    def _register(self):
        self.supervisor.register_scheduler(self)
        while not self._started:
            time.sleep(self.poll_delay)

    def _start(self, *args, **kwargs):
        self.executor_loop = kwargs.get('_loop', self.executor_loop)
        self._register()

    def _stop(self, *args, **kwargs):
        self.supervisor.unregister_scheduler(self)

    def mark_started(self):
        self._executor_stop_event = asyncio.Event()
        super().mark_started()

    async def loop(self, *args, **kwargs):
        self.mark_started()
        while self._active:
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self._active:
                if not await self.launch_executor():
                    break
            else:
                break
            await asyncio.sleep(self.supervisor.poll_delay)
        self.mark_stopped()

    def _run(self, *args):
        try:
            try:
                if self.run(*(args + self._task_args), **
                            self._task_kwargs) is False:
                    self._abort()
            except Exception as e:
                self.error(e)
        finally:
            self.supervisor.mark_task_completed()
            self._current_executor = None
            self._send_executor_stop_event()

    async def _run_coroutine(self, *args, **kwargs):
        if await self.run(*(args + self._task_args), **
                          self._task_kwargs) is False:
            self._abort()
        self._current_executor = None
        self._send_executor_stop_event()

    def _send_executor_stop_event(self):
        asyncio.run_coroutine_threadsafe(
            self._set_stop_event(), loop=self.supervisor.event_loop)

    async def _set_stop_event(self):
        self._executor_stop_event.set()

    async def launch_executor(self, *args, **kwargs):
        self.last_executed = time.time()
        if asyncio.iscoroutinefunction(self.run):
            self._current_executor = self.run
            if self.executor_loop:
                asyncio.run_coroutine_threadsafe(
                    self._run_coroutine(*args), loop=self.executor_loop)
                return True
            else:
                result = await self.run(*(args + self._task_args),
                                        **self._task_kwargs)
            self._current_executor = None
            return result is not False and self._active
        elif self._run_in_mp:
            self._current_executor = self.run
            self.supervisor.mp_pool.apply_async(self.run,
                                                args + self._task_args,
                                                self._task_kwargs, self._cb_mp)
            return self._active
        else:
            t = threading.Thread(
                target=self._run,
                name=self.name + '_run',
                args=args,
                kwargs=kwargs)
            self._current_executor = t
            if self.daemon:
                t.setDaemon(True)
            return self.supervisor.put_task(t, self.priority) and self._active


class BackgroundQueueWorker(BackgroundAsyncWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        q = kwargs.get('q', kwargs.get('queue'))
        if isinstance(q, type):
            self._qclass = q
        else:
            self._qclass = asyncio.queues.Queue

    def put(self, t):
        asyncio.run_coroutine_threadsafe(
            self._Q.put(t), loop=self.supervisor.event_loop)

    def send_stop_events(self):
        try:
            self.put(None)
        except:
            pass

    def _stop(self, *args, **kwargs):
        super()._stop(*args, **kwargs)

    async def loop(self, *args, **kwargs):
        self._Q = self._qclass()
        self.mark_started()
        while self._active:
            task = await self._Q.get()
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self._active and task is not None:
                if not await self.launch_executor(task):
                    break
            else:
                break
            if not self._suppress_sleep:
                await asyncio.sleep(self.supervisor.poll_delay)
        self.mark_stopped()

    def get_queue_obj(self):
        return self._Q


class BackgroundEventWorker(BackgroundAsyncWorker):

    def trigger(self):
        asyncio.run_coroutine_threadsafe(
            self._set_event(), loop=self.supervisor.event_loop)

    async def _set_event(self):
        self._E.set()

    async def loop(self, *args, **kwargs):
        self._E = asyncio.Event()
        self.mark_started()
        while self._active:
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            await self._E.wait()
            self._E.clear()
            if not self._active or not await self.launch_executor():
                break
            if not self._suppress_sleep:
                await asyncio.sleep(self.supervisor.poll_delay)
        self.mark_stopped()

    def send_stop_events(self, *args, **kwargs):
        try:
            self.trigger()
        except:
            pass

    def get_event_obj(self):
        return self._E


class BackgroundIntervalWorker(BackgroundEventWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delay_before = kwargs.get('delay_before')
        self.delay = kwargs.get(
            'interval', kwargs.get('delay', kwargs.get('delay_after', 1)))
        if 'interval' in kwargs:
            self.keep_interval = True
        else:
            self.keep_interval = False
        self.extra_loops = ['interval_loop']
        self._suppress_sleep = True
        self._interval_loop_stopped = False

    def _start(self, *args, **kwargs):
        self.delay_before = kwargs.get('_delay_before', self.delay_before)
        self.delay = kwargs.get(
            '_interval',
            kwargs.get('_delay', kwargs.get('_delay_after', self.delay)))
        if '_interval' in kwargs:
            self.keep_interval = True
        super()._start(*args, **kwargs)
        return True

    def before_start(self):
        super().before_start()
        self._interval_loop_stopped = False

    def wait_until_stop(self):
        super().wait_until_stop()
        while not self._interval_loop_stopped:
            time.sleep(self.poll_delay)

    async def interval_loop(self, *args, **kwargs):
        while self._active:
            if self.keep_interval: tstart = time.time()
            if self._current_executor:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self.delay_before:
                await asyncio.sleep(self.delay_before)
            if not self._active:
                break
            if not self._active or not await self.launch_executor():
                break
            if not self.delay and not self.delay_before:
                tts = self.poll_delay
            elif self.keep_interval:
                tts = self.delay + tstart - time.time()
            else:
                tts = self.delay
                if self._current_executor:
                    await self._executor_stop_event.wait()
                    self._executor_stop_event.clear()
            if tts > 0:
                if tts < 0.1:
                    await asyncio.sleep(tts)
                else:
                    ttsi = int(tts)
                    while self.last_executed + ttsi >= time.time():
                        await asyncio.sleep(0.1)
                        if not self._active:
                            self._interval_loop_stopped = True
                            return
                    await asyncio.sleep(tts - ttsi)
        self._interval_loop_stopped = True


def background_worker(*args, **kwargs):

    def decorator(f, **kw):
        func = f
        kw = kw.copy() if kw else kwargs
        if kwargs.get('q') or kwargs.get('queue'):
            C = BackgroundQueueWorker
        elif kwargs.get('e') or kwargs.get('event'):
            C = BackgroundEventWorker
        elif kwargs.get('i') or \
                kwargs.get('interval') or \
                kwargs.get('delay') or kwargs.get('delay_before'):
            C = BackgroundIntervalWorker
        elif asyncio.iscoroutinefunction(func):
            C = BackgroundAsyncWorker
        else:
            C = BackgroundWorker
        if 'name' in kw:
            name = kw['name']
            del kw['name']
        else:
            name = func.__name__
        f = C(worker_name=name, **kw)
        f.run = func
        f._can_use_mp_pool = False
        return f

    return decorator if not args else decorator(args[0], **kwargs)
