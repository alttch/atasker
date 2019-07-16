__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.1.7"

import threading
import time

from functools import wraps

from atasker import task_supervisor

from atasker import TASK_NORMAL


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

    @wraps(f)
    def start_task(*args, **kw):
        t = threading.Thread(
            group=kwargs.get('group'),
            target=_background_task_runner,
            name=kwargs.get('name'),
            args=(f,) + args,
            kwargs=kw)
        if kwargs.get('daemon'): t.setDaemon(True)
        task_supervisor.put_task(
            t, kwargs.get('priority', TASK_NORMAL), kwargs.get('delay'))
        return t

    return start_task


def _background_task_runner(f, *args, **kwargs):
    try:
        f(*args, **kwargs)
    finally:
        task_supervisor.mark_task_completed()
