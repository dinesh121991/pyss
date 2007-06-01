#!/usr/bin/env python2.4

from common import * 
 
from base.prototype import JobSubmissionEvent, JobTerminationEvent

class Events:
    
    def __init__(self):
        from base.simple_heap import Heap
        self._events_heap = Heap()

    def add_event(self, event): 
        self._events_heap.push( (event.timestamp, event) )

    @property
    def is_empty(self):
        return len(self._events_heap) == 0

    def pop_min_event(self):
        assert not self.is_empty
        timestamp, event = self._events_heap.pop()
        return event
