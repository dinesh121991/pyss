#!/usr/bin/env python2.4

from common import * 
 
from base.prototype import JobSubmissionEvent, JobTerminationEvent

class Events:
    
    def __init__(self):
        self.collection = {}

    def _addEvent(self, event): 
         if self.collection.has_key(event.timestamp):
             self.collection[event.timestamp].insert(0, event)
         else:
             self.collection[event.timestamp] = []
             self.collection[event.timestamp].insert(0, event)

    def add_job_submission_event(self, timestamp, job): # adds a single submission event to the collection
        self._addEvent(JobSubmissionEvent(timestamp, job))

    @property
    def _min_event_time(self):
        return min(self.collection.keys())

    @property
    def is_empty(self):
        return len(self.collection) == 0

    def pop_min_event(self):
        assert not self.is_empty
        assert len(self.collection[self._min_event_time]) > 0
        result = self.collection[self._min_event_time].pop()
        if len(self.collection[self._min_event_time]) == 0:
            del self.collection[self._min_event_time]
        return result
        
    def add_job_termination_event(self, timestamp, job):
        # makes sure that there will be a single termination event for this job
        # assert timestamp >= 0
        self._addEvent(JobTerminationEvent(timestamp, job))

    def addEvents(self, new_events): # combines a new collection of events with the self collection        
         for timestamp, new_list_of_events_at_this_time in new_events.collection.iteritems():
             for new_event in new_list_of_events_at_this_time:         
                 self._addEvent(new_event)                     

    def printEvents(self): # SHOULD IT BE __STR__????
        times = self.collection.keys()
        times.sort()
        for t in times:
            for event in self.collection[t]: 
                print event 
        print
         
