__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.3.20"

import threading
import time
import uuid
import logging
import asyncio

from functools import wraps

from atasker import task_supervisor

from atasker import TASK_NORMAL
from atasker import TT_THREAD, TT_MP, TT_COROUTINE

from atasker.supervisor import ALoop


class LocalProxy(threading.local):
    """
    Simple proxy for threading.local namespace
    """

    def get(self, attr, default=None):
        """
        Get thread-local attribute

        Args:
            attr: attribute name
            default: default value if attribute is not set

        Returns:
            attribute value or default value
        """
        return getattr(self, attr, default)

    def has(self, attr):
        """
        Check if thread-local attribute exists

        Args:
            attr: attribute name

        Returns:
            True if attribute exists, False if not
        """
        return hasattr(self, attr)

    def set(self, attr, value):
        """
        Set thread-local attribute

        Args:
            attr: attribute name
            value: attribute value to set
        """
        return setattr(self, attr, value)

    def clear(self, attr):
        """
        Clear (delete) thread-local attribute

        Args:
            attr: attribute name
        """
        return delattr(self, attr) if hasattr(self, attr) else True


class Locker:
    """
    Locker helper/decorator

    Args:
        mod: module name (for logging only)
        timeout: max lock timeout before critical (default: 5 sec)
        relative: True for RLock (default), False for Lock
    """

    def __init__(self, mod='main', timeout=5, relative=True):
        self.lock = threading.RLock() if relative else threading.Lock()
        self.logger = logging.getLogger('atasker')
        self.mod = mod
        self.relative = relative
        self.timeout = timeout

    def __call__(self, f):

        @wraps(f)
        def do(*args, **kwargs):
            if not self.lock.acquire(timeout=self.timeout):
                self.logger.critical('{}/{} locking broken'.format(
                    self.mod, f.__name__))
                self.critical()
                return None
            try:
                return f(*args, **kwargs)
            finally:
                self.lock.release()

        return do

    def __enter__(self):
        """
        Raises:
            TimeoutError: if lock not acquired
        """
        if not self.lock.acquire(timeout=self.timeout):
            self.logger.critical('{} locking broken'.format(self.mod))
            self.critical()
            raise TimeoutError

    def __exit__(self, *args, **kwargs):
        self.lock.release()

    def critical(self):
        """
        Override this
        """
        pass


def background_task(f, *args, **kwargs):
    """
    Wrap function to a task

    Args:
        f: task function
        group: put task thread in the specified group
        name: set task thread name (default: function name)
        daemon: if True, task thread will be launched as daemon
        priority: task :ref:`priority<priorities>`
        delay: startup delay
        supervisor: custom :doc:`task supervisor<supervisor>`
        tt: TT_THREAD (default) or TT_MP (TT_COROUTINE is detected
            automatically)
        callback: callback function for TT_MP
        loop: asyncio loop or aloop object (optional)

    Raises:
        RuntimeError: if coroutine function is used but loop is not specified
            and supervisor doesn't have default aloop
    """

    def gen_mp_callback(task_id, callback, supervisor):

        def cbfunc(*args, **kwargs):
            if callable(callback):
                callback(*args, **kwargs)
            supervisor.mark_task_completed(task_id=task_id)

        return cbfunc

    @wraps(f)
    def start_task(*args, **kw):
        tt = kwargs.get('tt', TT_THREAD)
        supervisor = kwargs.get('supervisor', task_supervisor)
        if tt == TT_COROUTINE or asyncio.iscoroutinefunction(f):
            loop = kwargs.get('loop')
            if isinstance(loop, str) or loop is None:
                loop = supervisor.get_aloop(loop)
            if not loop:
                raise RuntimeError('loop not specified')
            if isinstance(loop, ALoop):
                return loop.background_task(f(*args, **kw))
            else:
                return asyncio.run_coroutine_threadsafe(f(*args, **kw),
                                                        loop=loop)
        elif tt == TT_THREAD:
            t = threading.Thread(group=kwargs.get('group'),
                                 target=_background_task_thread_runner,
                                 name=kwargs.get('name'),
                                 args=(f, supervisor) + args,
                                 kwargs=kw)
            if kwargs.get('daemon'): t.setDaemon(True)
            return supervisor.put_task(t, kwargs.get('priority', TASK_NORMAL),
                                kwargs.get('delay'))
            return t
        elif tt == TT_MP:
            task_id = str(uuid.uuid4())
            task = (f, args, kw,
                    gen_mp_callback(task_id, kwargs.get('callback'),
                                    supervisor))
            return supervisor.put_task(task,
                                kwargs.get('priority', TASK_NORMAL),
                                kwargs.get('delay'),
                                tt=TT_MP,
                                task_id=task_id)

    return start_task


def _background_task_thread_runner(f, supervisor, *args, **kwargs):
    try:
        f(*args, **kwargs)
    finally:
        supervisor.mark_task_completed()
