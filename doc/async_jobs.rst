Async jobs
**********

**atasker** has built-in integration with `aiosched
<https://github.com/alttch/aiosched>`_ - simple and fast async job scheduler.

**aiosched** schedulers can be automatically started inside
:ref:`aloop<aloops>`:

.. code:: python

   async def test1():
      print('I am lightweight async job')

   task_supervisor.create_aloop('jobs')
   # if aloop id not specified, default aloop is used
   task_supervisor.create_async_job_scheduler('default', aloop='jobs',
      default=True)
   # create async job
   job1 = task_supervisor.create_async_job(target=test1, interval=0.1)
   # cancel async job
   task_supervisor.cancel_async_job(job=job1)

.. note::
   **aiosched** jobs are lightweight, don't report any statistic data and don't
   check is the job already running.
