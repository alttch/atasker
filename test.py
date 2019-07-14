import time
import logging

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


@background_worker(delay=1)
def myworker(*args, **kwargs):
    global c
    print('worker is running')
    print(args)
    print(kwargs)
    c += 1
    time.sleep(1)
    # print(c)
    # return False


import asyncio


def e(*args, **kwargs):
    print(kwargs['e'])

import asyncio

@background_worker(q=asyncio.queues.PriorityQueue, on_error=e)
def myqueuedworker(task, **kwargs):
    print('queued worker is running, queue task: {}'.format(task))
    # time.sleep(0.4)


@background_worker(event=myevent, on_error=e)
def myeventworker(**kwargs):
    print('event worker is running')
    # time.sleep(1)


task_supervisor.set_config(pool_size=0, reserve_normal=0, reserve_high=0)
task_supervisor.poll_delay = 0.01
task_supervisor.start()

f = TaskCollection()


#@background_task
def test(*args, **kwargs):
    print('job ttt: test', args, kwargs)
    # time.sleep(3)


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
    time.sleep(0.5)
    # return False

class W2(atasker.BackgroundIntervalWorker):

    def run(self, **kwargs):
        print(self)

# print(f())
# time.sleep(1)
# task_supervisor.stop(wait=2)
# exit()
myworker.start(123,x=2)
# myqueuedworker.start()
# myeventworker.start()
# someworker.start()
# w2=W2(interval=1, name='w2')
# w2.start()
# myqueuedworker.put('task1')
# myevent.set()
# time.sleep(2)
# myqueuedworker.put('task2')
# myqueuedworker.put('task3')
# myqueuedworker.put('task4')
# for i in range(100):
# myqueuedworker.put(i)
# myevent.set()
# myeventworker.trigger()
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
# background_task(test, name='ttt', priority=atasker.TASK_CRITICAL)()
# background_task(test, name='ttt', priority=atasker.TASK_HIGH)(1,a=2)
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
# time.sleep(1)
# myworker.stop(wait=True)
# someworker.stop(wait=True)
# print('worker stopped')
time.sleep(5)
# task_supervisor.block()
task_supervisor.stop(wait=True)
# print(c)
