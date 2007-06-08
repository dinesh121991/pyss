#!/usr/bin/env python2.4

from base.prototype import Job
from base.prototype import JobSubmissionEvent, JobTerminationEvent
from base.event_queue import EventQueue
from base.prototype import Machine

from common import CpuSnapshot

import sys

def parse_jobs(input_file_name):
    """
    Assumption: Job details are 'correct': submit_time,
    num_required_processors and duration are non-negative, job id is
    unique, and the amount of processors requested by the job is never more
    than the total available processors
    """
    input_file = open(input_file_name) # openning of the specified file for reading 
    jobs = []
    
    for line in input_file:
        if len(line.strip()) == 0: # skip empty lines (.strip() removes leading and trailing whitspace)
            continue
        if line.startswith('#'): # skip comments
            continue

        (str_j_submit_time, j_id, str_j_estimated_run_time, \
         str_j_nodes, str_j_actual_run_time, str_j_admin_QoS, str_j_user_QoS) = line.split()

        j_submit_time = int(str_j_submit_time)
        j_estimated_run_time = int(str_j_estimated_run_time)
        j_actual_run_time = int(str_j_actual_run_time)
        j_nodes = int(str_j_nodes)


        if j_estimated_run_time >= j_actual_run_time and j_submit_time >= 0 and j_nodes > 0 and j_actual_run_time >= 0:
            j_admin_QoS = int(str_j_admin_QoS)
            j_user_QoS = int(str_j_user_QoS)
            newJob = Job(j_id, j_estimated_run_time, j_actual_run_time, j_nodes, j_submit_time, j_admin_QoS, j_user_QoS)
            jobs.append(newJob)

    input_file.close()

    return jobs

class Simulator(object):
    """ Assumption 1: The simulation clock goes only forward. Specifically,
    an event on time t can only produce future events with time t' = t or t' > t.
    Assumption 2: self.jobs holds every job that was introduced to the simulation. """ 
        
    def __init__(self, jobs, num_processors, scheduler):
        self.num_processors = num_processors
        self.jobs = jobs
        self.scheduler = scheduler

        self.event_queue = EventQueue()

        # TODO: use ValidatingMachine here
        self.machine = Machine(num_processors=num_processors, event_queue=self.event_queue)
        
        self.event_queue.add_handler(JobSubmissionEvent, self.handle_submission_event)
        self.event_queue.add_handler(JobTerminationEvent, self.handle_termination_event)

        for job in self.jobs:
            self.event_queue.add_event( JobSubmissionEvent(job.submit_time, job) )

    def handle_submission_event(self, event):
        assert isinstance(event, JobSubmissionEvent)
        newEvents = self.scheduler.handleSubmissionOfJobEvent(event.job, event.timestamp)
        for event in newEvents:
            self.event_queue.add_event(event)

    def handle_termination_event(self, event):
        assert isinstance(event, JobTerminationEvent)
        if event.job.start_to_run_at_time + event.job.actual_run_time != event.timestamp:
          return # redundant JobTerminationEvent, TODO: maybe require no redundant events
        newEvents = self.scheduler.handleTerminationOfJobEvent(event.job, event.timestamp)
        for event in newEvents:
            self.event_queue.add_event(event)

    def run(self):        
        while not self.event_queue.is_empty:
            self.event_queue.advance()

def run_simulator(num_processors, input_file, scheduler):
    simulator = Simulator(parse_jobs(input_file), num_processors, scheduler)

    simulator.run()

    # simulation is done
    scheduler.cpu_snapshot.printCpuSlices()
    print_statistics(simulator.jobs)

    return simulator


def print_statistics(jobs):

    if len(jobs) == 0:
        print
        print "STATISTICS: "
        print "Input file is probably empty"
        return
    
    wait_time = sigma_wait_time = flow_time = sigma_flow_time = 0 
    counter = 0
    
    for job in jobs:
        counter += 1
        
        wait_time = job.start_to_run_at_time - job.submit_time
        sigma_wait_time += wait_time
        
        flow_time = wait_time + job.actual_run_time
        sigma_flow_time += flow_time
        
    print
    print "STATISTICS: "
    print "Average wait time is: ", sigma_wait_time / counter
    print "Average flow time is: ", sigma_flow_time / counter 
    print "Number of jobs: ", counter
        
