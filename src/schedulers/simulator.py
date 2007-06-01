#!/usr/bin/env python2.4

from base.prototype import Job
from base.prototype import JobSubmissionEvent, JobTerminationEvent
from base.event_queue import EventQueue

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

class Simulator:
    """ Assumption 1: The simulation clock goes only forward. Specifically,
    an event on time t can only produce future events with time t' = t or t' > t.
    Assumption 2: self.jobs holds every job that was introduced to the simulation. """ 
        
    def __init__(self, total_nodes, input_file, scheduler):
        self.total_nodes = total_nodes
        self.events = None
        self.jobs = None
        self.input_file = input_file
        self.scheduler = scheduler
        
        self.startSimulation() 
   
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

    def startSimulation(self):        
        self.jobs = parse_jobs(self.input_file)

        self.event_queue = EventQueue()

        self.event_queue.add_handler(JobSubmissionEvent, self.handle_submission_event)
        self.event_queue.add_handler(JobTerminationEvent, self.handle_termination_event)

        for job in self.jobs:
            self.event_queue.add_event( JobSubmissionEvent(job.submit_time, job) )

        while not self.event_queue.is_empty:
            self.event_queue.advance()

        # simulation is done
        print
        print
        print "______________ last snapshot, before the simulation ends ________" 
        self.scheduler.cpu_snapshot.printCpuSlices()
        
        self.calculate_statistics()  



    def calculate_statistics(self):

        if len(self.jobs) == 0:
            print
            print "STATISTICS: "
            print "Input file is probably empty"
            return
        
        wait_time = sigma_wait_time = flow_time = sigma_flow_time = 0 
        counter = 0
        
        for job in self.jobs:
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
        

###############

# (w_wtime, w_sld, w_user, w_bypass, w_admin, w_size)
#w_l = Weights(0, 0, 0, 1, 1, 0) 
#w_b = Weights(0, 1.0, 0, 0, 0, 0) 

#simulation = Simulator(scheduler ="Maui", maui_list_weights = w_l, maui_backfill_weights = w_b)
#simulation = Simulator(scheduler ="Fcfs", total_nodes = 1024)
#simulation = Simulator(scheduler ="Conservative", total_nodes = 1024)
#simulation = Simulator(scheduler ="EasyBackfill", total_nodes = 1024)
#profile.run('simulation.startSimulation()')
#simulation.startSimulation()

#simulation = Simulator(input_file = "./Input_test_files/basic_input.1", scheduler ="Conservative")
#simulation = Simulator(scheduler ="Fcfs")
#simulation = Simulator(scheduler ="Conservative")
