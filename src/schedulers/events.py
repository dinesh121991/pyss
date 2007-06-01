#!/usr/bin/env python2.4

from common import * 
 
from base.prototype import JobSubmissionEvent, JobTerminationEvent

class Events:
    
    def __init__(self):
        from base.simple_heap import Heap
        self._events_heap = Heap()

    def _addEvent(self, event): 
        self._events_heap.push( (event.timestamp, event) )

    def add_job_submission_event(self, timestamp, job): # adds a single submission event to the collection
        self._addEvent(JobSubmissionEvent(timestamp, job))

    @property
    def is_empty(self):
        return len(self._events_heap) == 0

    def pop_min_event(self):
        assert not self.is_empty
        timestamp, event = self._events_heap.pop()
        return event
        
    def add_job_termination_event(self, timestamp, job):
        # makes sure that there will be a single termination event for this job
        # assert timestamp >= 0
        self._addEvent(JobTerminationEvent(timestamp, job))

    def addEvents(self, new_events): # combines a new collection of events with the self collection        
        for (timestamp, new_event) in new_events._events_heap:
            self._addEvent(new_event)                     
