__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.2.11"

from atasker import task_supervisor

from atasker import TASK_NORMAL, TT_MP

import uuid
import asyncio


async def co_mp_apply(f,
                      args=(),
                      kwargs={},
                      priority=None,
                      delay=None,
                      supervisor=None):
    """
    Async task execution inside multiprocessing pool

    Args:
        f: module.function (function must be located in external module)
        args: function arguments
        kwargs: function keyword arguments
        priority: task :ref:`priority<priorities>` (default: TASK_NORMAL)
        delay: delay before execution
        supervisor: custom :doc:`task supervisor<supervisor>`
    """

    class CO:

        async def run(self, *args, **kwargs):
            self._event = asyncio.Event()
            task = (self.task_id, self.func, args, kwargs, self.callback)
            return self.supervisor.put_task(
                task, self.priority, self.delay, tt=TT_MP)

        async def _set_event(self):
            self._event.set()

        def callback(self, result):
            self.supervisor.mark_task_completed(self.task_id)
            self._result = result
            asyncio.run_coroutine_threadsafe(self._set_event(), loop=self._loop)

        async def get_result(self):
            await self._event.wait()
            self._event.clear()
            return self._result

    co = CO()
    co.task_id = str(uuid.uuid4())
    co.priority = priority if priority is not None else TASK_NORMAL
    co.delay = delay
    co.supervisor = supervisor if supervisor else task_supervisor
    co.func = f
    co._loop = asyncio.get_event_loop()
    if not await co.run(args, kwargs):
        return None
    return await co.get_result()
