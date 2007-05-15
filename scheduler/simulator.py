#!/usr/bin/env python2.3

from sim  import *
from sim1 import *
from sim2 import *
from sim3 import * 

import sys
#import profile

class JobArrivalEventGeneratorViaLogFile:
    
    def __init__(self, input_file):
        """Assumption: Job details are 'correct': arrival_time, nodes and duration are non-negative, job id is unique, 
        and the amount of nodes requested by the job is never more than the total available nodes"""
        self.file = file(input_file) # openning of the specified file for reading 
        self.events = Events()
        self.jobs = []
        
        while True: 
            line = self.file.readline()
            # print line
            if len(line) == 0: # zero length indicates end-of-file
                break
            if line.startswith('#'):
                continue # skipping a comment in the input_file 

            (str_j_arrival_time, j_id, str_j_user_predicted_duration, \
             str_j_nodes, str_j_actual_duration, str_j_admin_QoS, str_j_user_QoS) = line.split()

            j_arrival_time = int(str_j_arrival_time)
            j_user_predicted_duration = int(str_j_user_predicted_duration)
            j_actual_duration = int(str_j_actual_duration)
            j_nodes = int(str_j_nodes)
  
 
            if j_user_predicted_duration >= j_actual_duration and j_arrival_time >= 0 and j_nodes > 0 and j_actual_duration >= 0:
                j_admin_QoS = int(str_j_admin_QoS)
                j_user_QoS = int(str_j_user_QoS)
                newJob = Job(j_id, j_user_predicted_duration, j_nodes, j_arrival_time, j_actual_duration, j_admin_QoS, j_user_QoS)
                self.jobs.append(newJob)
                self.events.add_job_arrival_event(int(j_arrival_time), newJob)

        self.file.close()

        
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
                self.scheduler.weights_list = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of arrival
            if maui_backfill_weights != None: 
                self.scheduler.weights_backfill = maui_backfill_weights
            else:
                self.scheduler.weights_backfill = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of arrival
                
        elif scheduler ==  "Fcfs":
            self.scheduler = FcfsScheduler(total_nodes)
        else:
            print ">>> Problem: No such scheduling Policy"
            return

        # self.startSimulation() 
   
       

    def startSimulation(self):
        
        events_generated_by_input_file = JobArrivalEventGeneratorViaLogFile(self.input_file)
        self.events = events_generated_by_input_file.events
        self.jobs = events_generated_by_input_file.jobs
        
        
        self.events.add_end_of_simulation_event(sys.maxint) #generates a deafult end_of_simulation_event at "maxint" time
         
        end_of_simulation_event_has_not_occured = True 

        while end_of_simulation_event_has_not_occured and len(self.events.collection) > 0:
            
            current_time = min(self.events.collection.keys())

            
            while len(self.events.collection[current_time]) > 0:

                # print "Current Known Events:"
                # for tmp_event in self.events.collection[current_time]:
                    # print current_time, str(tmp_event)
                # print
                
                event = self.events.collection[current_time].pop()
                # print str(event)

                if isinstance(event, JobArrivalEvent):
                    newEvents = self.scheduler.handleArrivalOfJobEvent(event.job, int(current_time))
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    self.events.addEvents(newEvents) 
                    continue

                elif isinstance(event, JobTerminationEvent):
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    if event.job.start_to_run_at_time + event.job.actual_duration < current_time:
                      continue # redundant JobTerminationEvent 
                    newEvents = self.scheduler.handleTerminationOfJobEvent(event.job, current_time)
                    #self.scheduler.cpu_snapshot.printCpuSlices()
                    self.events.addEvents(newEvents)
                    continue

                elif isinstance(event, EndOfSimulationEvent):
                    end_of_simulation_event_has_not_occured = False
                    self.scheduler.handleEndOfSimulationEvent(current_time)
                    print "______________ last snapshot, before the simulation ends ________" 
                    self.scheduler.cpu_snapshot.printCpuSlices()
                    # self.feasibilty_check_of_jobs_data(current_time)
                    break

                else:
                    assert False # should never reach here
                
            
            del self.events.collection[current_time] # removing the current events

            # print "______________ right after calling to archive removal________" 
            # self.scheduler.cpu_snapshot.printCpuSlices()

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
            
            wait_time = job.start_to_run_at_time - job.arrival_time
            sigma_wait_time += wait_time
            
            flow_time = wait_time + job.actual_duration
            sigma_flow_time += flow_time
            
        print
        print "STATISTICS: "
        print "Average wait time is: ", sigma_wait_time / counter
        print "Average flow time is: ", sigma_flow_time / counter 
        print "Number of jobs: ", counter
        
        

            
    def feasibilty_check_of_jobs_data(self, current_time):
        """ Reconstructs a schedule from the jobs (using the values:
        job.arrival time, job.start_to_run_at_time, job_actual_duration for each job),
        and then checks the feasibility of this schedule. """

        if current_time < sys.maxint:
            print ">>> Simulation ends perhaps before all the jobs were scheduled properly"
            return

        print "@@@ Fesibilty Test"
        cpu_snapshot = CpuSnapshot(self.total_nodes)

        every_job_starts_after_its_arrival_time= True
        cpu_snapshot_is_feasible = True
        
        for job in self.jobs:
            # print str(job)
            if job.arrival_time > job.start_to_run_at_time:
                print ">>> PROBLEM: job starts before arrival...."
                every_job_starts_after_its_arrival_time = False
                
            if job.actual_duration > 0:
                new_job = Job(job.id, job.actual_duration, job.nodes, job.arrival_time, job.actual_duration)
                cpu_snapshot.assignJob(new_job, job.start_to_run_at_time)
                # cpu_snapshot.printCpuSlices()
                
        
        cpu_snapshot_is_feasible = cpu_snapshot.CpuSlicesTestFeasibility()
        # cpu_snapshot.printCpuSlices()
        
        if every_job_starts_after_its_arrival_time and cpu_snapshot_is_feasible:  
            print "Feasibility Test is OK!!!!!"
        else: 
            print ">>> There was a problem with the feasibilty of the simulator/schedule!!!!!!!!"

        

###############

# (w_wtime, w_sld, w_user, w_bypass, w_admin, w_size)
#w_l = Weights(0, 0, 0, 1, 1, 0) 
#w_b = Weights(0, 1.0, 0, 0, 0, 0) 

#simulation = Simulator(scheduler ="Maui", maui_list_weights = w_l, maui_backfill_weights = w_b)
simulation = Simulator(scheduler ="Conservative", total_nodes = 1024)
#simulation = Simulator(scheduler ="Fcfs", total_nodes = 1024)
#simulation = Simulator(scheduler ="EasyBackfill", total_nodes = 1024)
#profile.run('simulation.startSimulation()')
simulation.startSimulation()

#simulation = Simulator(input_file = "./Input_test_files/basic_input.1", scheduler ="Conservative")
#simulation = Simulator(scheduler ="Fcfs")
#simulation = Simulator(scheduler ="Conservative")
