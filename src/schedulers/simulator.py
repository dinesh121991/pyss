#!/usr/bin/env python2.4

from common  import *
from events import *

from fcfs_scheduler import FcfsScheduler
from conservative_scheduler import ConservativeScheduler
from easy_scheduler import EasyBackfillScheduler
from maui_scheduler import MauiScheduler, Weights

import sys
#import profile

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

def create_submission_events(jobs):
    events = Events()
    for job in jobs:
        events.add_job_submission_event(job.submit_time, job)
    return events
        
class Simulator:
    """ Assumption 1: The simulation clock goes only forward. Specifically,
    an event on time t can only produce future events with time t' = t or t' > t.
    Assumption 2: self.jobs holds every job that was introduced to the simulation. """ 
        
    def __init__(self, total_nodes=100, input_file="input", scheduler=None, maui_list_weights=None, maui_backfill_weights=None):
        self.total_nodes = total_nodes
        self.current_time = 0
        self.events = None
        self.jobs = None
        self.input_file = input_file
        
        
        if scheduler ==  "Conservative":
            self.scheduler =  ConservativeScheduler(total_nodes)

        elif scheduler ==  "EasyBackfill":
            self.scheduler =  EasyBackfillScheduler(total_nodes)
            
        elif scheduler ==  "Maui":
            self.scheduler =  MauiScheduler(total_nodes)
            if maui_list_weights != None:  
                self.scheduler.weights_list = maui_list_weights
            else:
                self.scheduler.weights_list = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of submission
            if maui_backfill_weights != None: 
                self.scheduler.weights_backfill = maui_backfill_weights
            else:
                self.scheduler.weights_backfill = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of submission
                
        elif scheduler ==  "Fcfs":
            self.scheduler = FcfsScheduler(total_nodes)
        else:
            print ">>> Problem: No such scheduling Policy"
            return

        self.startSimulation() 
   
       

    def startSimulation(self):        
        self.jobs = parse_jobs(self.input_file)
        self.events = create_submission_events(self.jobs)
        
        while len(self.events.collection) > 0:
            
            current_time = min(self.events.collection.keys())
            
            while len(self.events.collection[current_time]) > 0:

                # print "Current Known Events:"
                # for tmp_event in self.events.collection[current_time]:
                    # print current_time, str(tmp_event)
                # print
                
                event = self.events.collection[current_time].pop()
                # print str(event)

                if isinstance(event, JobSubmissionEvent):
                    newEvents = self.scheduler.handleSubmissionOfJobEvent(event.job, int(current_time))
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    self.events.addEvents(newEvents) 
                    continue

                elif isinstance(event, JobTerminationEvent):
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    if event.job.start_to_run_at_time + event.job.actual_run_time != current_time:
                      continue # redundant JobTerminationEvent
                    newEvents = self.scheduler.handleTerminationOfJobEvent(event.job, current_time)
                    self.events.addEvents(newEvents)
                    continue

                else:
                    assert False # should never reach here
                
            del self.events.collection[current_time] # removing the current events

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
        
        

            
    def feasibilty_check_of_data(self):
        """ Reconstructs a schedule from the jobs (using the values:
        job.submit_time, job.start_to_run_at_time, job_actual_run_time for each job),
        and then checks the feasibility of this schedule.
        Then check the actual slices of the scheduler itself. Then deletes the jobs from the
        actual scheduler expecting to see slices with free_nodes == total_nodes"""

        print "@@@ Fesibilty Test"
        cpu_snapshot = CpuSnapshot(self.total_nodes)

        j = Job(1, 1, 1, 1, 1)

        for job in self.jobs:
            if job.submit_time > job.start_to_run_at_time:
                print ">>> PROBLEM: job starts before submission...."
                return False
                
            if job.actual_run_time > 0:
                j.id = job.id
                j.num_required_processors = job.num_required_processors
                j.submit_time = job.submit_time
                j.actual_run_time = job.actual_run_time
                cpu_snapshot.assignJob(j, job.start_to_run_at_time)
        
        if not cpu_snapshot.CpuSlicesTestFeasibility():
            return False
        
        if not self.scheduler.cpu_snapshot.CpuSlicesTestFeasibility():
            return False

        # self.scheduler.cpu_snapshot.printCpuSlices()
        
        for job in self.jobs:
            # print job
            j.num_required_processors = job.num_required_processors
            j.start_to_run_at_time = job.start_to_run_at_time
            j.estimated_run_time = j.actual_run_time = job.actual_run_time
            self.scheduler.cpu_snapshot.delJobFromCpuSlices(j)
    
        if not self.scheduler.cpu_snapshot.CpuSlicesTestEmptyFeasibility():
            return False
  
        return True                
          
        
        

        

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
