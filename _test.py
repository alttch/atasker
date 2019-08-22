import time
import logging

# import asyncio

# loop = asyncio.get_event_loop()

from atasker import background_worker
from atasker import background_task
from atasker import task_supervisor

from atasker import TaskCollection
from atasker import FunctionCollection

import atasker

logging.basicConfig(level=logging.DEBUG)

logging.getLogger('atasker/workers').setLevel(logging.DEBUG)
logging.getLogger('atasker/supervisor').setLevel(logging.DEBUG)

import threading

from queue import Queue

myevent = threading.Event()

Q = Queue()

c = 0

async def calc(a):
    print(a)
    await asyncio.sleep(1)
    print(a*2)
    return a * 3

task_supervisor.set_thread_pool(pool_size=20, reserve_normal=0, reserve_high=0)
# task_supervisor.create_mp_pool()
task_supervisor.set_mp_pool(pool_size=2, reserve_normal=0, reserve_high=0)
task_supervisor.poll_delay = 0.01
# task_supervisor.default_executor_loop = loop
task_supervisor.start()

al = task_supervisor.create_aloop('myworkers', default=True, daemon=True)
# task_supervisor.start_aloop('myworkers')

f = TaskCollection()
# from multiprocessing import Pool

# p = Pool(processes = 8)

from tests.mpworker import MPWorker

# mpw = MPWorker(interval=0.5)

# mpw.start()


@background_worker(interval=0.5)
async def myworker(*args, **kwargs):
    global c
    print('worker is running {}'.format(threading.current_thread()))
    # print(args)
    # print(kwargs)
    c += 1
    # time.sleep(0.3)
    # print(c)
    # return False


# task_supervisor.mp_pool.apply_async(myworker)
# p.apply_async(func=myworker)
# time.sleep(1)
# task_supervisor.stop()
# exit()


def e(*args, **kwargs):
    print(kwargs['e'])


import asyncio

import tests.mp

from atasker import co_mp_apply


@background_worker(q=asyncio.queues.PriorityQueue, on_error=e)
async def myqueuedworker(task, **kwargs):
    print('queued worker is running, queue task: {}'.format(task))
    try:
        result = await co_mp_apply(
            tests.mp.test,
            args=(1, 2, 3),
            kwargs={'x': 2},
            priority=atasker.TASK_HIGH)
        print('RESULT: {}'.format(result))
    except:
        print('fucked')
        import traceback
        print(traceback.format_exc())
    # time.sleep(0.4)


@background_worker(event=myevent, on_error=e)
def myeventworker(**kwargs):
    print('event worker is running')
    # time.sleep(1)


# @background_task
async def test(*args, **kwargs):
    print('job ttt: test', args, kwargs)
    await asyncio.sleep(1)
    return '12345'


@f(priority=atasker.TASK_CRITICAL)
def start1():
    print('start1')


@f
def start2():
    print('start2')


import queue


def ttt():
    q = queue.Queue()
    q.get()


# def stop():
# print('STOPPING')
# print('stopping supervisor')
# myworker.stop(wait=True)
# return
# task_supervisor.stop(wait=2)


@background_worker
def someworker(**kwargs):
    print('i am some worker')
    # time.sleep(0.5)
    # return False


# print(f())
# time.sleep(1)
# task_supervisor.stop(wait=2)
# exit()
myworker.start(123, x=2)#, _loop=al)
# myqueuedworker.start()
# myeventworker.start()
# someworker.start()
# w2=atasker.W2() #interval=0.1)
# w2.start()
# w2.trigger()
# w2.put('xxx')
# w2.put('xxx')
# w2.put('xxx')
# w2.put('xxx')
# myqueuedworker.put('task1')
# myevent.set()
# time.sleep(2)
# myqueuedworker.put('task2')
background_task(test, loop=al.get_loop())(1,2,3, x='test')
print(al.run(calc(1)))
# myqueuedworker.put('task3')
# myqueuedworker.put('task4')
# for i in range(100):
# myqueuedworker.put(i)
# myevent.set()
# myeventworker.trigger()
# time.sleep(0.5)
# myeventworker.trigger()
# myeventworker.trigger()
# time.sleep(1)
# myeventworker.restart(wait=True)
# myeventworker.trigger()
# myeventworker.trigger()
# time.sleep(1)
# myeventworker.trigger()
# print('ALL SET')
# time.sleep(0.1)
# myworker.stop()
# myworker.start()


def cb(result):
    print('func result: {}'.format(result))


# background_task(
# tests.mp.test,
# name='ttt',
# priority=atasker.TASK_CRITICAL,
# tt=atasker.TT_MP,
# callback=cb)()
# background_task(
# tests.mp.test, name='ttt', priority=atasker.TASK_HIGH, tt=atasker.TT_MP)(
# 1, x=2)
# test()
# test(1, a=2)
# background_task(test, name='ttt')()
# time.sleep(0.01)
# background_task(
# test, name='ttt', priority=atasker.TASK_CRITICAL)()
# time.sleep(2)
# background_task(test, name='ttt')()
# background_task(test, name='ttt')()
# background_task(test, name='ttt')()
# background_task(test, name='ttt')()
# for i in range(100):
# t = threading.Thread(target=ttt)
# t.start()
# x = atasker.BackgroundQueueWorker()
# x.start()
# print('xxx')
# background_task(stop, delay=1, priority=atasker.TASK_CRITICAL)()
print('waiting...')
# time.sleep(0.5)
# myworker.trigger()
# time.sleep(1)
# myworker.stop(wait=True)
# someworker.stop(wait=True)
# print('worker stopped')
for x in range(0,30):
    print(task_supervisor.get_info().aloops)
    time.sleep(0.1)
# task_supervisor.block()
# loop.run_forever()
task_supervisor.stop(wait=2)

print(c)
