from atasker import BackgroundIntervalWorker

class MPWorker(BackgroundIntervalWorker):

    @staticmethod
    def run(**kwargs):
        print(kwargs)
