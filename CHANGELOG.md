## 0.7 (2019-12-09)

* to speed up thread spawn, thread tasks execution moved to
  *ThreadPoolExecutor*. Due to this, tasks as ready-made thread objects are no
  longer supported.

* **put_task()** method arguments now should be target, args, kwargs and
  callback

* **mark_task_completed** now always requires either task or task_id

* **daemon** parameter is now obsolete

* supervisors have got IDs (used in logging and thread names only)

