#!/usr/bin/env python

# times are passed around as datetime.datetime objects
# "Zero" time is datetime.datetime.fromtimestamp(0), e.g. 1 January, 1970
import datetime

class Scheduler(object):
    def new_job_submitted(self, event, when):
        pass # TODO

class Job(object):
    def __init__(self, estimated_run_time, actual_run_time, num_required_processors):
        self.estimated_run_time = estimated_run_time
        self.actual_run_time = actual_run_time
        self.num_required_processors = num_required_processors

class Schedule(object):
    def add_job(self, job, start_time):
        pass # TODO

def workload():
    "a generator that yield jobs"
    for i in xrange(100): # TODO: magic number
        yield Job(
                estimated_run_time = random.randrange(100,1000),
                actual_run_time = random.randrange(90,400),
                num_required_processors = random.randrange(10,100)
            )

class Cluster(object):
    class Processor(object):
        def __init__(self, id):
            self.id = id
            self.busy = False

    def __init__(self, num_processors):
        self.processors = [self.Processor(i) for i in xrange(num_processors)]

    idle_processors = property(
            lambda self: [p for p in self.processors if not p.busy]
        )

    def run_job(self, job):
        assert len(self.idle_processors) >= job.num_required_processors
        # log an event for the job completion

class Statistics(object):
    pass

class JobEvent(object):
    def __init__(self, job): self.job = job
class JobStartedEvent(JobEvent): pass
class JobEndedEvent(JobEvent): pass

class EventQueue(object):
    def __init__(self):
        self.queue = [] # sorted list of (when, event) tuples
        self.observers_by_event_type = {}

    def add_observer(self, event_type, observer):
        "observer is a callable that will be called with the event"
        self.observers_by_event_type.setdefault(event_type, [])
        self.observers_by_event_type[event_type].append( observer )

    def add_event(self, event, when):
        "inserts the event into its proper place in the queue"
        # Note: can use module heapq for efficiency if necessary
        self.queue.append( (when, event) )
        self.queue.sort()

    def _has_observers(self, event):
        return type(event) in self.observers_by_event_type

    def _observers_for(self, event):
        assert self._has_observers(event)
        return self.observers_by_event_type[type(event)]

    def _notify_observers(self, event, when):
        if not self._has_observers(event):
            import warnings
            warnings.warn("Got event '%s' without observers" % event)

        for observer in self.observers_by_event_type[type(event)]:
            observer(event, when)

    def handle_next_event(self):
        assert len(self.queue) > 0
        event, when = self.queue.pop(0)
        self._notify_observers(event, when)

# globals
g_event_queue = EventQueue()

def main():
    cluster = Cluster(num_processors = 1000)
    scheduler = Scheduler()
    g_event_queue.add_observer(JobStartedEvent, scheduler.new_job_submitted)

if __name__ == '__main__':
    main()
