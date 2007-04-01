#! /usr/bin/env python

import bisect

class JobEvent(object):
    def __init__(self, timestamp, job):
        self.timestamp = timestamp
        self.job = job

    def __repr__(self):
        return type(self).__name__ + "<timestamp=%(timestamp)s, job=%(job)s>" % vars(self)

    def __cmp__(self, other):
        "compare by timestamp first, job second"
        return cmp((self.timestamp, self.job), (other.timestamp, other.job))

class JobSubmitEvent(JobEvent): pass
class JobStartEvent(JobEvent): pass
class JobEndEvent(JobEvent): pass

class EventQueue(object):
    class EmptyQueue(Exception): pass

    def __init__(self):
        self._sorted_events = []
        self._handlers = {}

    def add_event(self, event):
        # insert mainting sort
        bisect.insort(self._sorted_events, event)

    empty = property(lambda self: len(self._sorted_events) == 0)

    def _assert_not_empty(self):
        if self.empty:
            raise self.EmptyQueue()

    def pop(self):
        self._assert_not_empty()
        return self._sorted_events.pop(0)

    def _get_event_handlers(self, event_type):
        if event_type in self._handlers:
            return self._handlers[event_type]
        else:
            return []

    def advance(self):
        self._assert_not_empty()
        event = self.pop()
        for handler in self._get_event_handlers( type(event) ):
            handler(event)

    def add_handler(self, event_type, handler):
        self._handlers.setdefault(event_type, [])
        self._handlers[event_type].append(handler)

class Job(object):
    def __init__(self,
            id, estimated_run_time, actual_run_time, num_required_processors
        ):
        self.id = id
        self.estimated_run_time = estimated_run_time
        self.actual_run_time = actual_run_time
        self.num_required_processors = num_required_processors

class StupidScheduler(object):
    # TODO: this does nothing yet
    def __init__(self, event_queue):
        self.event_queue = event_queue
        self.next_free_time = 0

    def job_submitted(self, event):
        self.event_queue.add_event(
            JobStartEvent(timestamp=self.next_free_time, job=event.job)
        )
        self.next_free_time += job.estimated_run_time

class Simulator(object):
    def __init__(self, job_source):
        self.event_queue = EventQueue()
        self.jobs = {}

        for start_time, job in job_source:
            self.jobs[job.id] = job
            self.event_queue.add_event(
                    JobStartEvent(timestamp = start_time, job = job)
                )

        self.event_queue.add_handler(JobStartEvent, self.job_started_handler)

    def job_started_handler(self, event):
        assert event.job.id in self.jobs
        job = self.jobs[event.job.id]
        self.event_queue.add_event(
            JobEndEvent(
                timestamp = event.timestamp + job.actual_run_time,
                job = job,
            )
        )

    def run(self):
        while not self.event_queue.empty:
            self.event_queue.advance()

def simple_job_generator(num_jobs):
    import random
    start_time = 0
    for id in xrange(num_jobs):
        start_time += random.randrange(0, 15)
        yield start_time, Job(
            id=id,
            estimated_run_time=random.randrange(400, 2000),
            actual_run_time=random.randrange(30, 1000),
            num_required_processors=random.randrange(2,100),
        )
