#! /usr/bin/env python

class JobEvent(object):
    def __init__(self, timestamp, job_id):
        self.timestamp = timestamp
        self.job_id = job_id

    def __cmp__(self, other):
        "Sort by timestamp"
        return cmp(self.timestamp, other.timestamp)

class JobStartEvent(JobEvent): pass
class JobEndEvent(JobEvent): pass

class EventQueue(object):
    class EmptyQueue(Exception): pass

    def __init__(self):
        self._events = []

    def add_event(self, event):
        self._events.append(event)

    def pop(self):
        return self._events.pop(0)

