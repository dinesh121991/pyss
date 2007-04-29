#!/usr/bin/env python2.4

from sim import *
from sim1 import *
import sys



class Scheduler:
    """ Assumption: every handler returns a (possibly empty) collection of new events """
    
    def handleArrivalOfJobEvent(self, job, current_time):
        pass
    
    def handleTerminationOfJobEvent(self, job, current_time):
        pass
    
    def handleEndOfSimulationEvent(self, current_time):
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
        self.waiting_list_of_unscheduled_jobs = []

        
    def handleArrivalOfJobEvent(self, just_arrived_job, time):
         self.waiting_list_of_unscheduled_jobs.append(just_arrived_job)
         return self._schedule_jobs(time)  
             

    def handleTerminationOfJobEvent(self, job, time):
        """ this handler deletes the tail of job.
        It then reschedules the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(time)


    def _schedule_jobs(self, current_time):
        newEvents = Events()
                             
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return newEvents # waiting list is empty
        self._schedule_the_head_of_the_waiting_list(current_time, newEvents)
        self._backfill_the_tail_of_the_waiting_list(current_time, newEvents)
        return newEvents


    

    def _schedule_the_head_of_the_waiting_list(self, time, newEvents):
        while len(self.waiting_list_of_unscheduled_jobs) > 0: 
            first_job = self.waiting_list_of_unscheduled_jobs[0]
            start_time_of_first_job = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
            if start_time_of_first_job == time:
                self.waiting_list_of_unscheduled_jobs.remove(first_job)
                self.cpu_snapshot.assignJob(first_job, time)
                termination_time = time + first_job.actual_duration
                newEvents.add_job_termination_event(termination_time, first_job) 
            else:
                break

    def _backfill_the_tail_of_the_waiting_list(self, time, newEvents):
        if len(self.waiting_list_of_unscheduled_jobs) > 1:
            first_job = self.waiting_list_of_unscheduled_jobs[0]
            for next_job in self.waiting_list_of_unscheduled_jobs[1:] : 
                if self.canBeBackfilled(first_job, next_job, time):
                    self.waiting_list_of_unscheduled_jobs.remove(next_job)
                    time = self.cpu_snapshot.jobEarliestAssignment(next_job, time)
                    self.cpu_snapshot.assignJob(next_job, time)
                    termination_time = time + next_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, next_job)                    
 
    

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
                #this means that assigning the second job at current time postphones the
                #first job in the waiting list, and so the second job cannot be back filled 
        else:
            return True 
      

    def handleEndOfSimulationEvent(self, current_time):
         if current_time == sys.maxint:
            # otherewise, it might be the case that the simulation stoped
            # before some jobs were scheduled properly 
            self.cpu_snapshot.CpuSlicesTestFeasibility()
