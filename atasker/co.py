from atasker import task_supervisor

from atasker import TASK_NORMAL, TT_MP

import uuid
import asyncio

async def co_apply(f, args=(), kwargs={}, priority=TASK_NORMAL, delay=None):

    class CO:

        async def run(self, *args, **kwargs):
            self._event = asyncio.Event()
            task = (
                    self.task_id,
                    self.func,
                    args,
                    kwargs,
                    self.callback)
            return task_supervisor.put_task(task, self.priority, self.delay, tt=TT_MP)

        async def _set_event(self):
            self._event.set()

        def callback(self, result):
            task_supervisor.mark_task_completed(self.task_id)
            self._result = result
            asyncio.run_coroutine_threadsafe(self._set_event(), loop=self._loop)

        async def get_result(self):
            await self._event.wait()
            self._event.clear()
            return self._result

    co = CO()
    co.task_id = str(uuid.uuid4())
    co.priority = priority
    co.delay=delay
    co.func = f
    co._loop = asyncio.get_event_loop()
    if not await co.run(args, kwargs):
        return None
    return await co.get_result()
