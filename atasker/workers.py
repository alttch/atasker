__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.0.3"

import threading
import logging
import uuid
import time
import asyncio
import queue

from atasker import task_supervisor

from atasker import TASK_NORMAL

logger = logging.getLogger('atasker/workers')


class BackgroundWorker:

    # ----- override this -----

    def run(self, *args, **kwargs):
        raise Exception('not implemented')

    def before_start(self):
        pass

    def after_start(self):
        pass

    def before_stop(self):
        pass

    def after_stop(self):
        pass

    # -----------------------

    def __init__(self, name=None, func=None, **kwargs):
        if func: self.run = func
        self._executor_thread = None
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
        self.set_name(name)
        self.thread_args = ()
        self.thread_kw = {}
        self.start_stop_lock = threading.Lock()

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
            kw['_worker'] = self
            self.daemon = kwargs.get('daemon', True)
            if not '_worker_name' in kw:
                kw['_worker_name'] = self.name
            if not 'o' in kw:
                kw['o'] = self.o
            self.thread_args = args
            self.thread_kw = kw
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
            args=self.thread_args,
            kwargs=self.thread_kw)
        if self.daemon:
            t.setDaemon(True)
        self.supervisor.put_task(t, self.priority)
        while not self._started:
            time.sleep(self.poll_delay)
        self.supervisor.register_sync_scheduler(self)

    def _abort(self):
        self.mark_stopped()
        self.stop(wait=False)

    def loop(self, *args, **kwargs):
        self.mark_started()
        while self._active:
            try:
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
            if wait:
                if self._executor_thread:
                    try:
                        self._executor_thread.join()
                    except:
                        pass
                while not self._stopped:
                    time.sleep(self.poll_delay)
            self._stop(wait=wait)
            self.after_stop()
        finally:
            self.start_stop_lock.release()

    def _stop(self, **kwargs):
        self.supervisor.unregister_sync_scheduler(self)


class _BackgroundAsyncWorkerAbstract(BackgroundWorker):

    def _register(self):
        self.supervisor.register_scheduler(self)
        while not self._started:
            time.sleep(self.poll_delay)

    def _start(self, *args, **kwargs):
        self._register()

    def _stop(self, *args, **kwargs):
        self.supervisor.unregister_scheduler(self)

    def before_stop(self):
        self._executor_stop_event.set()

    def mark_started(self):
        self._executor_stop_event = asyncio.Event()
        super().mark_started()

    def _run(self, *args):
        try:
            try:
                if self.run(*(args + self.thread_args), **
                            self.thread_kw) is False:
                    self._abort()
            except Exception as e:
                self.error(e)
        finally:
            self.supervisor.mark_task_completed()
            self._executor_thread = None
            asyncio.run_coroutine_threadsafe(
                self._set_stop_event(), loop=self.supervisor.event_loop)

    async def _set_stop_event(self):
        self._executor_stop_event.set()

    def launch_run_task(self, *args, **kwargs):
        t = threading.Thread(
            target=self._run, name=self.name + '_run', args=args, kwargs=kwargs)
        self._executor_thread = t
        if self.daemon:
            t.setDaemon(True)
        return self.supervisor.put_task(t, self.priority) and self._active


class BackgroundIntervalWorker(_BackgroundAsyncWorkerAbstract):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delay_before = kwargs.get('delay_before')
        self.delay = kwargs.get(
            'interval', kwargs.get('delay', kwargs.get('delay_after', 1)))
        if 'interval' in kwargs:
            self.keep_interval = True
        else:
            self.keep_interval = False

    def _start(self, *args, **kwargs):
        self.delay_before = kwargs.get('_delay_before', self.delay_before)
        self.delay = kwargs.get(
            '_interval',
            kwargs.get('_delay', kwargs.get('_delay_after', self.delay)))
        if '_interval' in kwargs:
            self.keep_interval = True
        super()._start(*args, **kwargs)
        return True

    async def loop(self, *args, **kwargs):
        self.mark_started()
        while self._active:
            if self.keep_interval: tstart = time.time()
            if self._executor_thread:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self.delay_before:
                await asyncio.sleep(self.delay_before)
            if not self._active or not self.launch_run_task():
                break
            if not self.delay and not self.delay_before:
                tts = self.poll_delay
            elif self.keep_interval:
                tts = self.delay + tstart - time.time()
            else:
                tts = self.delay
                if self._executor_thread:
                    await self._executor_stop_event.wait()
                    self._executor_stop_event.clear()
            if tts > 0:
                await asyncio.sleep(tts)
        self.mark_stopped()


class BackgroundQueueWorker(_BackgroundAsyncWorkerAbstract):

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

    async def loop(self, *args, **kwargs):
        self._Q = self._qclass()
        self.mark_started()
        while self._active:
            task = await self._Q.get()
            if self._executor_thread:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            if self._active and task is not None:
                if not self.launch_run_task(task):
                    break
            else:
                break
            await asyncio.sleep(self.supervisor.poll_delay)
        self.mark_stopped()

    def get_queue_obj(self):
        return self._Q

    def before_stop(self):
        try:
            self.put(None)
        except:
            pass
        super().before_stop()


class BackgroundEventWorker(_BackgroundAsyncWorkerAbstract):

    def trigger(self):
        asyncio.run_coroutine_threadsafe(
            self._set_event(), loop=self.supervisor.event_loop)

    async def _set_event(self):
        self._E.set()

    async def loop(self, *args, **kwargs):
        self._E = asyncio.Event()
        self.mark_started()
        while self._active:
            if self._executor_thread:
                await self._executor_stop_event.wait()
                self._executor_stop_event.clear()
            await self._E.wait()
            self._E.clear()
            if not self._active or not self.launch_run_task():
                break
            await asyncio.sleep(self.supervisor.poll_delay)
        self.mark_stopped()

    def get_event_obj(self):
        return self._E

    def before_stop(self):
        try:
            self.trigger()
        except:
            pass
        super().before_stop()


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
        else:
            C = BackgroundWorker
        if 'name' in kw:
            name = kw['name']
            del kw['name']
        else:
            name = func.__name__
        f = C(name=name, **kw)
        f.run = func
        return f

    return decorator if not args else decorator(args[0], **kwargs)
