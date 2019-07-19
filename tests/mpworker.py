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
