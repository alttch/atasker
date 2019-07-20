__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.2.8"

import threading
import time
import uuid

from functools import wraps

from atasker import task_supervisor

from atasker import TASK_NORMAL
from atasker import TT_THREAD, TT_MP


class LocalProxy(threading.local):

    def get(self, attr, default=None):
        return getattr(self, attr, default)

    def has(self, attr):
        return hasattr(self, attr)

    def set(self, attr, value):
        return setattr(self, attr, value)

    def clear(self, attr):
        return delattr(self, attr) if hasattr(self, attr) else True


def background_task(f, *args, **kwargs):

    def gen_mp_callback(task_id, callback, supervisor):

        def cbfunc(*args, **kwargs):
            supervisor.mark_task_completed(task_id)
            if callable(callback):
                callback(*args, **kwargs)

        return cbfunc

    @wraps(f)
    def start_task(*args, **kw):
        tt = kwargs.get('tt', TT_THREAD)
        supervisor = kwargs.get('supervisor', task_supervisor)
        if tt == TT_THREAD:
            t = threading.Thread(
                group=kwargs.get('group'),
                target=_background_task_thread_runner,
                name=kwargs.get('name'),
                args=(f, supervisor) + args,
                kwargs=kw)
            if kwargs.get('daemon'): t.setDaemon(True)
            supervisor.put_task(t, kwargs.get('priority', TASK_NORMAL),
                                kwargs.get('delay'))
            return t
        elif tt == TT_MP:
            task_id = str(uuid.uuid4())
            task = (task_id, f, args, kw,
                    gen_mp_callback(task_id, kwargs.get('callback'),
                                    supervisor))
            supervisor.put_task(
                task,
                kwargs.get('priority', TASK_NORMAL),
                kwargs.get('delay'),
                tt=TT_MP)

    return start_task


def _background_task_thread_runner(f, supervisor, *args, **kwargs):
    try:
        f(*args, **kwargs)
    finally:
        supervisor.mark_task_completed()
