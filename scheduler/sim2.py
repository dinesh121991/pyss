#!/usr/bin/env python2.4

from sim import *
from sim1 import *
import sys

class JobArrivalEventGeneratorViaLogFile:
    
    def __init__(self, input_file):
        """Assumption: Job details are 'correct': arrival_time, nodes and duration are non-negative, job id is unique, 

        and the amount of nodes requested by the job is never more than the total available nodes"""
        
        self.file = file(input_file) # open the specified file for reading 
        self.events = Events()
        self.jobs = []
        
        while True: 
            line = self.file.readline()
            print line
            if len(line) == 0: # zero length indicates end-of-file
                break
            if line.startswith('#'):
                continue # we should skip a comment in the input_file 
            (job_arrival_time, job_id, job_duration, job_nodes, job_actual_duration ) = line.split()
            

            newJob = Job(job_id, int(job_duration), int(job_nodes), int(job_arrival_time), int(job_actual_duration))

            self.jobs.append(newJob)
            self.events.add_job_arrival_event(int(job_arrival_time), newJob)


        self.file.close()

        
class Simulator:
    """ Assumption 1: The simulation clock goes only forward. Specifically,
    an event on time t can only produce future events with time t' = t or t' > t.
    Assumption 2: self.jobs holds every job that was introduced to the simulation. """ 
        
    def __init__(self, total_nodes=100, input_file="input", scheduler=None):
        self.total_nodes = total_nodes
        self.current_time = 0
        events_generated_by_input_file = JobArrivalEventGeneratorViaLogFile(input_file)
        self.events = events_generated_by_input_file.events
        self.jobs = events_generated_by_input_file.jobs



        if scheduler ==  "Conservative":
            self.scheduler =  ConservativeScheduler(total_nodes)
        elif scheduler ==  "EasyBackfill":
            self.scheduler =  EasyBackfillScheduler(total_nodes)
        elif scheduler ==  "Fcfs":
            self.scheduler = FcfsScheduler(total_nodes)
        else:
            print ">>> Problem: No such scheduling Policy"
            return 

        self.startSimulation()
         
   

    def startSimulation(self):
        
        self.events.add_end_of_simulation_event(sys.maxint) #generates a deafult end_of_simulation_event at "max_int" time
         
        end_of_simulation_event_has_not_occured = True 

        while end_of_simulation_event_has_not_occured and len(self.events.collection) > 0:
 
            current_time = sorted(self.events.collection.keys()).pop(0)

            while len(self.events.collection[current_time]) > 0:

                # print "Current Known Events:"
                # for tmp_event in self.events.collection[current_time]:
                    # print current_time, str(tmp_event)
                # print
                
                event = self.events.collection[current_time].pop()
                print str(event)

                if isinstance(event, JobArrivalEvent):
                    newEvents = self.scheduler.handleArrivalOfJobEvent(event.job, int(current_time))
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    self.events.addEvents(newEvents) 
                    continue

                elif isinstance(event, JobTerminationEvent):
                    print "TERMINATION EVENT", event
                    newEvents = self.scheduler.handleTerminationOfJobEvent(event.job, current_time)
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    self.events.addEvents(newEvents)
                    continue

                elif isinstance(event, EndOfSimulationEvent):
                    end_of_simulation_event_has_not_occured = False
                    # print "______________ last snapshot, before the simulation ends ________" 
                    # self.scheduler.cpu_snapshot.printCpuSlices()
                    self.scheduler.handleEndOfSimulationEvent(current_time)
                    self.feasibilty_check_of_jobs_data(current_time)
 
                    break

                else:
                    assert False # should never reach here
                
            
            del self.events.collection[current_time] #removing the events that were just handled
          
        self.calculate_statistics()  



    def calculate_statistics(self):

        wait_time = sigma_wait_time = flow_time = sigma_flow_time = counter = 0.0
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
            return

        print ">>> Fesibilty Test >>>"
        cpu_snapshot = CpuSnapshot(self.total_nodes)

        every_job_starts_after_its_arrival_time= True
        cpu_snapshot_is_feasible = True
        
        for job in self.jobs:
            print str(job)
            if job.arrival_time > job.start_to_run_at_time:
                print ">>> PROBLEM: job starts before arrival...."
                every_job_starts_after_its_arrival_time = False
                
            if job.actual_duration > 0:
                new_job = Job(job.id, job.actual_duration, job.nodes, job.arrival_time, job.actual_duration)
                cpu_snapshot.assignJob(new_job, job.start_to_run_at_time)
                cpu_snapshot.printCpuSlices()
                
        cpu_snapshot.printCpuSlices()
        
        cpu_snapshot_is_feasible = cpu_snapshot.CpuSlicesTestFeasibility()
        
        if every_job_starts_after_its_arrival_time and cpu_snapshot_is_feasible:  
            print "Feasibility Test is OK!!!!!"
        else: 
            print "There was a problem with the feasibilty of the simulator/schedule!!!!!!!!"

        

class Scheduler:
     """" Assumption: every handler returns a (possibly empty) collection of new events"""
    
     def handleArrivalOfJobEvent(self, job, time):
         pass
     
     def handleTerminationOfJobEvent(self, job, time):
         pass
     
     def handleEndOfSimulationEvent(self, time):
         pass
     
   
             

class FcfsScheduler(Scheduler):
    
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_queue_of_jobs = []

    def handleArrivalOfJobEvent(self, job, time):
        self.waiting_queue_of_jobs.append(job)
        newEvents = self._schedule_jobs(time)
        return newEvents

    def handleTerminationOfJobEvent(self, job, time):
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        newEvents = self._schedule_jobs(time)
        return newEvents

    def _schedule_jobs(self, time):
        newEvents = Events()
        first_failure_has_not_occured = True
        while len(self.waiting_queue_of_jobs) > 0 and first_failure_has_not_occured:
            job = self.waiting_queue_of_jobs[0]
            earliest_possible_time = self.cpu_snapshot.jobEarliestAssignment(job, time)
            if earliest_possible_time == time:
                del self.waiting_queue_of_jobs[0]
                self.cpu_snapshot.assignJob(job, time)     
                termination_time = time + job.actual_duration
                newEvents.add_job_termination_event(termination_time, job)
            else:
                first_failure_has_not_occured = False
        return newEvents

    def handleEndOfSimulationEvent(self, current_time):
        if current_time == sys.maxint:
            # otherewise, it might be the case that the simulation stoped
            # before some jobs were scheduled properly 
            self.cpu_snapshot.CpuSlicesTestFeasibility()            
        

    

class ConservativeScheduler(Scheduler):

    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.list_of_unfinished_jobs_arranged_by_arrival_times = []    
        
    def handleArrivalOfJobEvent(self, job, time):
        newEvents = Events()
        self.list_of_unfinished_jobs_arranged_by_arrival_times.append(job)        
        start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, time)
        self.cpu_snapshot.assignJob(job, start_time_of_job)
        termination_time = job.start_to_run_at_time + job.actual_duration
        newEvents.add_job_termination_event(termination_time, job)
        return newEvents
    
    def handleTerminationOfJobEvent(self, job, time):
        """ Here we delete the tail of job if it was ended before the duration declaration.
        It then reschedules the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        newEvents = Events()
        self.list_of_unfinished_jobs_arranged_by_arrival_times.remove(job)  
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._reschedule_jobs(time, newEvents)



    def _reschedule_jobs(self, time, newEvents):
        for job in self.list_of_unfinished_jobs_arranged_by_arrival_times:

            if job.start_to_run_at_time <= time:
                continue # job started to run before, so it cannot be rescheduled (preemptions are not allowed)

            prev_start_to_run_at_time = job.start_to_run_at_time
            self.cpu_snapshot.delJobFromCpuSlices(job)
            start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, time)
            self.cpu_snapshot.assignJob(job, start_time_of_job)
            if prev_start_to_run_at_time > job.start_to_run_at_time:
                new_termination_time = job.start_to_run_at_time + job.actual_duration
                newEvents.add_job_termination_event(new_termination_time, job)               
        return newEvents
    

    def handleEndOfSimulationEvent(self, current_time):
        if current_time == sys.maxint:
            # otherewise, it might be the case that the simulation stoped
            # before some jobs were scheduled properly 
            self.cpu_snapshot.CpuSlicesTestFeasibility()      

                


      
    

class EasyBackfillScheduler(Scheduler):
    
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times = []

        
    def handleArrivalOfJobEvent(self, just_arrived_job, time):
             
        if len(self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times) == 0:
            first_job = just_arrived_job
        else: 
            first_job = self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times[0]
            
        newEvents = Events()
        
        if first_job.id != just_arrived_job.id: # two distinct jobs
            if self.canBeBackfilled(first_job, just_arrived_job, time):
                print "JOB CAN BE BACKFILLED!!!! LA LA LA"
                self.cpu_snapshot.assignJob(just_arrived_job, time)
                termination_time = time + just_arrived_job.actual_duration
                newEvents.add_job_termination_event(termination_time, just_arrived_job)
                return newEvents  

            else:
                print "cannot be backfilled  111111"
                self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times.append(just_arrived_job)
                return newEvents 
 
        
        else: # the just arrived job is the only job (to be scheduled soon) that we now have in the waiting list
             print "cannot be backfilled  22222 (this is the only job in the waiting list)"
             start_time_of_just_arrived_job = self.cpu_snapshot.jobEarliestAssignment(just_arrived_job, time)
             if start_time_of_just_arrived_job == time:
                 print "cannot be backfilled  333333"
                 self.cpu_snapshot.assignJob(just_arrived_job, time)
                 termination_time = time + just_arrived_job.actual_duration
                 newEvents.add_job_termination_event(termination_time, just_arrived_job)
                 return newEvents
             else:
                 print "cannot be backfilled  444444"
                 self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times.append(just_arrived_job)
                 return newEvents
             
                 

    def handleTerminationOfJobEvent(self, job, time):
        """ this handler deletes the tail of job.
        It then reschedules the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(time)


    def _schedule_jobs(self, time):
        newEvents = Events()
                             
        if len(self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times) == 0:
            return newEvents # waiting list is empty        

        #first, try to schedule the head of the waiting list
        while len(self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times) > 0: 
            first_job = self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times[0]
            start_time_of_first_job = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
            if start_time_of_first_job == time:
                self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times.remove(first_job)
                self.cpu_snapshot.assignJob(first_job, time)
                termination_time = time + first_job.actual_duration
                newEvents.add_job_termination_event(termination_time, first_job) 
            else:
                break

        #then, try to backfill the tail of the waiting list
        if len(self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times) > 1:
            first_job = self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times[0]
            for next_job in self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times[1:] : 
                if self.canBeBackfilled(first_job, next_job, time):
                    self.waiting_list_of_unscheduled_jobs_arranged_by_arrival_times.remove(next_job)
                    start_time_of_next_job = self.cpu_snapshot.jobEarliestAssignment(next_job, time)
                    self.cpu_snapshot.assignJob(next_job, start_time_of_next_job)
                    termination_time = next_job.start_to_run_at_time + next_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, next_job)                    
        return newEvents

    

    def canBeBackfilled(self, first_job, second_job, time):
        print "... Let's check if the job can be backfilled"
        
        start_time_of_second_job = self.cpu_snapshot.jobEarliestAssignment(second_job, time)
        print "start time of the 2nd job: ", start_time_of_second_job, second_job.id

        if start_time_of_second_job > time:
            return False
    
        shadow_time = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
        print "shadow time, the reserved start time of the first job: ", shadow_time, first_job.id
        
        # TODO: shouldn't this method not change the state?
        self.cpu_snapshot.assignJob(second_job, time)
        start_time_of_1st_if_2nd_job_assigned = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
        print "start time of the 1st job after assigning the 2nd: ",  start_time_of_1st_if_2nd_job_assigned
        
        self.cpu_snapshot.delJobFromCpuSlices(second_job)
       
        if start_time_of_1st_if_2nd_job_assigned > shadow_time:
            print "reserved_start_time_of_first_job", shadow_time
            print "strat_time_of_1st_if_2nd_job_assigned", start_time_of_1st_if_2nd_job_assigned
            return False 
                #this means that assigning the second job at the earliest possible time postphones the
                #first job in the waiting list, and so the second job cannot be back filled 
        else:
            return True 
      

    def handleEndOfSimulationEvent(self, current_time):
        if current_time == sys.maxint:
            # otherewise, it might be the case that the simulation stoped
            # before some jobs were scheduled properly 
            self.cpu_snapshot.CpuSlicesTestFeasibility()      
        

###############
simulation = Simulator(scheduler ="Conservative")
#simulation = Simulator(input_file = "./Input_test_files/basic_input.1", scheduler ="Conservative")
#simulation = Simulator(scheduler ="EasyBackfill")
#simulation = Simulator(scheduler ="Fcfs")

            
