#! /usr/bin/env python

import bisect

class JobEvent(object):
    def __init__(self, timestamp, job_id):
        self.timestamp = timestamp
        self.job_id = job_id

    def __cmp__(self, other):
        "Sort by timestamp"
        return cmp(self.timestamp, other.timestamp)

    def __repr__(self):
        return "JobEvent<timestamp=%(timestamp)s, job_id=%(job_id)s>" % vars(self)

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

    _empty = property(lambda self: len(self._sorted_events) == 0)

    def pop(self):
        if self._empty:
            raise self.EmptyQueue()
        return self._sorted_events.pop(0)

    def _get_event_handlers(self, event_type):
        if event_type in self._handlers:
            return self._handlers[event_type]
        else:
            return []

    def advance(self):
        event = self.pop()
        for handler in self._get_event_handlers( type(event) ):
            handler(event)

    def add_handler(self, event_type, handler):
        self._handlers.setdefault(event_type, [])
        self._handlers[event_type].append(handler)
