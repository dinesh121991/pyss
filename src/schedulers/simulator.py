#!/usr/bin/env python2.4

from base.prototype import JobSubmissionEvent, JobTerminationEvent, JobPredictionIsOverEvent
from base.prototype import ValidatingMachine
from base.event_queue import EventQueue
from common import CpuSnapshot, list_print
from easy_plus_plus_scheduler import EasyPlusPlusScheduler
from shrinking_easy_scheduler import ShrinkingEasyScheduler

import sys

class Simulator(object):
    """
    Assumption 1: The simulation clock goes only forward. Specifically,
    an event on time t can only produce future events with time t' = t or t' > t.
    Assumption 2: self.jobs holds every job that was introduced to the simulation.
    """

    def __init__(self, jobs, num_processors, scheduler):
        self.num_processors = num_processors
        self.jobs = jobs
        self.terminated_jobs=[]
        self.scheduler = scheduler
        self.time_of_last_job_submission = 0
        self.event_queue = EventQueue()

        self.machine = ValidatingMachine(num_processors=num_processors, event_queue=self.event_queue)

        self.event_queue.add_handler(JobSubmissionEvent, self.handle_submission_event)
        self.event_queue.add_handler(JobTerminationEvent, self.handle_termination_event)

        if isinstance(scheduler, EasyPlusPlusScheduler) or isinstance(scheduler, ShrinkingEasyScheduler):
            self.event_queue.add_handler(JobPredictionIsOverEvent, self.handle_prediction_event)
            
        for job in self.jobs:
            self.event_queue.add_event( JobSubmissionEvent(job.submit_time, job) )
        

    def handle_submission_event(self, event):
        assert isinstance(event, JobSubmissionEvent)
        self.time_of_last_job_submission = event.timestamp
        newEvents = self.scheduler.new_events_on_job_submission(event.job, event.timestamp)
        for event in newEvents:
            self.event_queue.add_event(event)

    def handle_termination_event(self, event):
        assert isinstance(event, JobTerminationEvent)
        newEvents = self.scheduler.new_events_on_job_termination(event.job, event.timestamp)
        self.terminated_jobs.append(event.job)
        for event in newEvents:
            self.event_queue.add_event(event)

    def handle_prediction_event(self, event):
        assert isinstance(event, JobPredictionIsOverEvent)
        newEvents = self.scheduler.new_events_on_job_under_prediction(event.job, event.timestamp)
        for event in newEvents:
            self.event_queue.add_event(event)

            
    def run(self):
        while not self.event_queue.is_empty:
            self.event_queue.advance()

def run_simulator(num_processors, jobs, scheduler):
    simulator = Simulator(jobs, num_processors, scheduler)
    simulator.run()
    print_simulator_stats(simulator)
    return simulator

def print_simulator_stats(simulator):
    simulator.scheduler.cpu_snapshot._restore_old_slices()
    # simulator.scheduler.cpu_snapshot.printCpuSlices()
    print_statistics(simulator.terminated_jobs, simulator.time_of_last_job_submission)


by_finish_time_sort_key = (
    lambda job : job.finish_time
)
    
def print_statistics(jobs, time_of_last_job_submission):
    assert jobs is not None, "Input file is probably empty."
    
    sigma_waits = sigma_run_times = 0
    sigma_slowdowns = sigma_bounded_slowdowns = 0.0
    counter = tmp_counter = 0
    
    size = len(jobs)

    for job in sorted(jobs, key=by_finish_time_sort_key):
        tmp_counter += 1
        
        #if size >= 100 and tmp_counter * 100 <= size:
            #continue
        
        #if job.finish_time > time_of_last_job_submission:
            #break
        
        counter += 1
        
        wait_time = float(job.start_to_run_at_time - job.submit_time)
        run_time  = float(job.actual_run_time)

        sigma_waits += wait_time
        sigma_run_times += run_time
        sigma_slowdowns += ((wait_time + run_time) / run_time)
        sigma_bounded_slowdowns += max( 1,  ( (wait_time + run_time) / max(run_time, 10) ) )


    print
    print "STATISTICS: "
    
    print "Average wait (Tw) [minutes]: ", float(sigma_waits) / (60 * max(counter, 1))

    print "Average response time (Tw + Tr) [minutes]: ", float(sigma_waits + sigma_run_times) / (60 * max(counter, 1))
    
    print "Average slowdown (Tw + Tr) / Tr:  ", sigma_slowdowns / max(counter, 1)

    print "Average bounded slowdown max(1, (Tw+Tr) / max(10, Tr):  ", sigma_bounded_slowdowns / max(counter, 1)
    
    print "Total Number of jobs: ", size
    
    print "Number of jobs used to calculate statistics: ", counter
    print

