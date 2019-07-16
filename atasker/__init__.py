__author__ = "Altertech Group, http://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.1.7"

from atasker.supervisor import TaskSupervisor
from atasker.supervisor import TASK_LOW
from atasker.supervisor import TASK_NORMAL
from atasker.supervisor import TASK_HIGH
from atasker.supervisor import TASK_CRITICAL

task_supervisor = TaskSupervisor()

from atasker.workers import background_worker

from atasker.workers import BackgroundWorker
from atasker.workers import BackgroundIntervalWorker
from atasker.workers import BackgroundQueueWorker
from atasker.workers import BackgroundEventWorker

from atasker.f import FunctionCollection
from atasker.f import TaskCollection

from atasker.threads import LocalProxy
from atasker.threads import background_task

g = LocalProxy()
