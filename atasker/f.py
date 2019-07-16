__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.1.7"

import traceback
import threading
import queue
import time

from atasker import task_supervisor


class FunctionCollection:

    def __init__(self, **kwargs):
        self._functions = []
        self._functions_with_priorities = []
        self.on_error = kwargs.get('on_error')
        self.on_error_kwargs = kwargs.get('on_error_kwargs', {})
        self.include_exceptions = True if kwargs.get(
            'include_exceptions') else False
        self.default_priority = 100

    def __call__(self, f=None, **kwargs):

        def wrapper(f, **kw):
            self.append(f, **kwargs)

        if f:
            self.append(f)
            return f
        elif kwargs:
            return wrapper
        else:
            return self.run()

    def append(self, f, priority=None):
        if f not in self._functions:
            self._functions.append(f)
            self._functions_with_priorities.append({
                'p':
                priority if priority else self.default_priority,
                'f':
                f
            })

    def remove(self, f):
        try:
            self._functions.remove(f)
        except Exception as e:
            self.error(e)

    def run(self):
        return self.execute()[0]

    def execute(self):
        result = {}
        all_ok = True
        funclist = sorted(self._functions_with_priorities, key=lambda k: k['p'])
        for fn in funclist:
            f = fn['f']
            k = '{}.{}'.format(f.__module__, f.__name__)
            try:
                result[k] = f()
            except Exception as e:
                if self.include_exceptions:
                    result[k] = (e, traceback.format_exc())
                else:
                    result[k] = None
                self.error(e)
                all_ok = False
        return result, all_ok

    def error(self, e):
        if self.on_error:
            kwargs = self.on_error_kwargs.copy()
            kwargs['e'] = e
            self.on_error(**kwargs)
        else:
            raise


class TaskCollection(FunctionCollection):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.lock = threading.Lock()
        self.result_queue = queue.Queue()
        self.threads = set()
        self.poll_delay = kwargs.get('poll_delay')

    def execute(self):
        with self.lock:
            poll_delay = self.poll_delay if self.poll_delay else \
                    task_supervisor.poll_delay
            result = {}
            self.threads.clear()
            all_ok = True
            funclist = sorted(
                self._functions_with_priorities, key=lambda k: k['p'])
            for fn in funclist:
                f = fn['f']
                t = threading.Thread(target=self._run_task, args=(f,))
                self.threads.add(t)
                task_supervisor.put_task(t, fn['p'])
            while True:
                try:
                    for t in self.threads:
                        t.join()
                    break
                except RuntimeError:
                    time.sleep(poll_delay)
            while True:
                try:
                    k, res, ok = self.result_queue.get(block=False)
                    result[k] = res
                    if not ok:
                        all_ok = False
                except queue.Empty:
                    break
            return result, all_ok

    def _run_task(self, f):
        k = '{}.{}'.format(f.__module__, f.__name__)
        try:
            result = f()
            ok = True
        except Exception as e:
            if self.include_exceptions:
                result = (e, traceback.format_exc())
            else:
                result = None
            self.error(e)
            ok = False
        self.result_queue.put((k, result, ok))
        task_supervisor.mark_task_completed()
