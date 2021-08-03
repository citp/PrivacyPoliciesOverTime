#Adapted from https://realpython.com/python-timer/#a-python-timer-class
import time

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

timers = []
    
class Timer:
    def __init__(self, name):
        self._start_time = None
        self.cumulative = 0
        self.name = name
        timers.append(self)

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")
        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        self.cumulative += elapsed_time

    def __str__(self):
        return "%s: %0.2f" % (self.name, self.cumulative)


def timeme(*oargs):
    def wrapper(func):
        if len(oargs) == 1:
            name = oargs[0]
        else:
            name = func.__str__()
        def timefunc(*args,**kwargs):
            t = Timer(name)
            t.start()
            ret = func(*args,**kwargs)
            t.stop()
            print("%s: %ds" % (t.name, t.cumulative))
            return ret
        return timefunc
    return wrapper
            

def get_all_timers():
    return timers

def dump_timers():
    global timers
    timers = []
