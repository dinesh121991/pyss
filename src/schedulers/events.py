#!/usr/bin/env python2.4

from common import * 
 

class Event:
    def __init__(self, job=None):
        self.job = job

class JobSubmissionEvent(Event):
    def __str__(self):
        return "Job Submission Event: " + str(self.job)

class JobTerminationEvent(Event):
     def __str__(self):
        return "Job Termination Event: " + str(self.job)

class Events:
    
    def __init__(self):
        self.collection = {}

    def _addEvent(self, time, event): 
         if self.collection.has_key(time):
             self.collection[time].insert(0, event)
         else:
             self.collection[time] = []
             self.collection[time].insert(0, event)


    def add_job_submission_event(self, time, job): # adds a single submission event to the collection
        self._addEvent(time, JobSubmissionEvent(job))
        
    def add_job_termination_event(self, time, job):
        # makes sure that there will be a single termination event for this job
        # assert time >= 0
        self._addEvent(time, JobTerminationEvent(job))

    def addEvents(self, new_events): # combines a new collection of events with the self collection        
         for time, new_list_of_events_at_this_time in new_events.collection.iteritems():
             for new_event in new_list_of_events_at_this_time:         
                 self._addEvent(time, new_event)                     

    def printEvents(self): # SHOULD IT BE __STR__????
        times = self.collection.keys()
        times.sort()
        for t in times:
            for event in self.collection[t]: 
                print event 
        print
         
