__author__ = "Altertech Group, https://www.altertech.com/"
__copyright__ = "Copyright (C) 2018-2019 Altertech Group"
__license__ = "Apache License 2.0"
__version__ = "0.4.4"

from atasker import BackgroundIntervalWorker


class MPWorker(BackgroundIntervalWorker):

    @staticmethod
    def run(**kwargs):
        print(kwargs)


class TestMPWorker(BackgroundIntervalWorker):

    a = 0

    @staticmethod
    def run(**kwargs):
        return 1

    def process_result(self, result):
        self.a += result
