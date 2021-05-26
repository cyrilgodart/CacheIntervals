import time
import logging

class Timer:
    def __enter__(self):
        self.interval = 0
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end = time.perf_counter()
        self.interval = self.end - self.start

    def log(self, level=logging.INFO):
        msg = self.message()
        logging.log(level, msg)

    def print(self ):
        print(self.message())

    def display(self, level=logging.INFO, printQ=False):
        msg = self.message()
        self.log(level)
        if printQ:
            self.print()

    def __str__(self):
        return '{}'.format(self.interval)

    def message(self):
        return 'Wall clock: {}'.format(self.interval)

    def __repr__(self):
        return '<Timer, start: {}, end: {}, Wall clock: {}>'.format(self.start, self.end, self.interval)

if __name__=="__main__":
    import logging
    import daiquiri
    daiquiri.setup(logging.DEBUG)
    import time

    with Timer() as t:
        time.sleep(1)
    print('{}'.format(t.interval))
    t.display()
    t.display(printQ=True)
    print(t)
    print(repr(t))
