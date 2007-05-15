#!/usr/bin/env python2.4

from sim  import *
from events import *
from sim2 import *
import sys




class EasyBackfillScheduler(Scheduler):
    
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_list_of_unscheduled_jobs = []

        
    def handleArrivalOfJobEvent(self, just_arrived_job, current_time):
        """ Here we first add the new job to the waiting list. We then try to schedule
        the jobs in the waiting list, returning a collection of new termination events """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.waiting_list_of_unscheduled_jobs.append(just_arrived_job)
        newEvents = Events()
        if len(self.waiting_list_of_unscheduled_jobs) == 1:  
            start_time = self.cpu_snapshot.jobEarliestAssignment(just_arrived_job, current_time)
            if start_time == current_time:
                self.waiting_list_of_unscheduled_jobs = []
                self.cpu_snapshot.assignJob(just_arrived_job, current_time)
                termination_time = current_time + just_arrived_job.actual_duration
                newEvents.add_job_termination_event(termination_time, just_arrived_job) 
        else: # there are at least 2 jobs in the waiting list  
            first_job = self.waiting_list_of_unscheduled_jobs[0]
            if self.canBeBackfilled(first_job, just_arrived_job, current_time):
                    self.waiting_list_of_unscheduled_jobs.pop()
                    self.cpu_snapshot.assignJob(just_arrived_job, current_time)
                    termination_time = current_time + just_arrived_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, just_arrived_job)                                
        return newEvents
    

    def handleTerminationOfJobEvent(self, job, current_time):
        """ Here we first delete the tail of the just terminated job (in case it's
        done before user estimation time). We then try to schedule the jobs in the waiting list,
        returning a collection of new termination events """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(current_time)
    
    
    def _schedule_jobs(self, current_time):       
        newEvents = Events()
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return newEvents
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
                    self.cpu_snapshot.assignJob(next_job, time)
                    termination_time = time + next_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, next_job)
 
    

    def canBeBackfilled(self, first_job, second_job, time):
        start_time_of_second_job = self.cpu_snapshot.jobEarliestAssignment(second_job, time)

        if start_time_of_second_job > time:
            return False

        shadow_time = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
        self.cpu_snapshot.assignJob(second_job, time)
        start_time_of_1st_if_2nd_job_assigned = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
        
        self.cpu_snapshot.delJobFromCpuSlices(second_job)
       
        if start_time_of_1st_if_2nd_job_assigned > shadow_time:
            return False 
        else:
            return True 
      







class Weights:
    # this class defines the configuration of weights for the MAUI 
    def __init__(self, w_wtime=1, w_sld=0, w_user=0, w_bypass=0, w_admin=0, w_size=0):
        self.wtime  = w_wtime  # weight of wait time since arrival  
        self.sld    = w_sld    # weight of slow down  
        self.user  = w_user   # weight of user desired quality of service 
        self.bypass = w_bypass # weight of being skipped over in the waiting list  
        self.admin  = w_admin  # weight of asmin desired quality of service
        self.size   = w_size   # weight of job size (= nodes) 


# a first toy version for the maui -- essentillay the diffrence between this simplified version of maui and easy
# backfilling is that the maui has more degree of freedom: maui may consider the jobs
# not necessarily by order of arrival, as opposed to the easy backfill.    

class MauiScheduler(EasyBackfillScheduler):
    def __init__(self, total_nodes = 100, weights_list=None, weights_backfill=None):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_list_of_unscheduled_jobs = []
        self.maui_timestamp = 0
        self.maui_current_time = 0

        # weights for calculation of priorities for the jobs in MAUI style
        self.weights_list = weights_list
        self.weights_backfill = weights_backfill

    
    def handleArrivalOfJobEvent(self, just_arrived_job, current_time):
        """ Here we first add the new job to the waiting list. We then try to schedule
        the jobs in the waiting list, returning a collection of new termination events """
        self.cpu_snapshot.archive_old_slices(current_time)
        just_arrived_job.maui_timestamp = self.maui_timestamp
        self.maui_timestamp += 1
        self.waiting_list_of_unscheduled_jobs.append(just_arrived_job)
        return self._schedule_jobs(current_time)  
        
        
    def _schedule_jobs(self, current_time):
        # Maui's scheduling methods are based on the analogue methods of EasyBackfill.
        # The different lines are marked with ## + 

        newEvents = Events()                  
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return newEvents         
        self.maui_current_time = current_time ## +
        self.waiting_list_of_unscheduled_jobs.sort(self.waiting_list_compare) ## + 
        self._schedule_the_head_of_the_waiting_list(current_time, newEvents)
        self._backfill_the_tail_of_the_waiting_list(current_time, newEvents)
        return newEvents

    
    def _backfill_the_tail_of_the_waiting_list(self, current_time, newEvents):
        if len(self.waiting_list_of_unscheduled_jobs) > 1:
            first_job = self.waiting_list_of_unscheduled_jobs.pop(0) ## + 
            # print "While trying to backfill...., first job is:", first_job
            # print ">>>> waiting list by waiting list priority:"
            # self.print_waiting_list()
            self.waiting_list_of_unscheduled_jobs.sort(self.backfilling_compare) ## + 
            # print ">>>> waiting list by backfilling priority:"
            # self.print_waiting_list()
            for next_job in self.waiting_list_of_unscheduled_jobs:
                if self.canBeBackfilled(first_job, next_job, current_time):
                    self.waiting_list_of_unscheduled_jobs.remove(next_job)
                    self.increament_bypass_counters_while_backfilling(first_job, next_job) ## +  
                    current_time = self.cpu_snapshot.jobEarliestAssignment(next_job, current_time)
                    self.cpu_snapshot.assignJob(next_job, current_time)
                    termination_time = current_time + next_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, next_job)
            self.waiting_list_of_unscheduled_jobs.append(first_job) # + 
        return newEvents


    def increament_bypass_counters_while_backfilling(self, first_job, backfilled_job):
        if first_job.maui_timestamp < backfilled_job.maui_timestamp:
            first_job.maui_bypass_counter += 1
            
        for job in self.waiting_list_of_unscheduled_jobs:
            if job.maui_timestamp < backfilled_job.maui_timestamp:
                job.maui_bypass_counter += 1
            

        
    def aggregated_weight_of_job(self, weights, job):
        wait = self.maui_current_time - job.arrival_time # wait time since arrival of job
        sld = (wait + job.user_predicted_duration) /  job.user_predicted_duration        
        w = weights
        
        weight_of_job = w.wtime  * wait + \
                     w.sld    * sld + \
                     w.user   * job.user_QoS + \
                     w.bypass * job.maui_bypass_counter + \
                     w.admin  * job.admin_QoS + \
                     w.size   * job.nodes
        return weight_of_job
    



    def waiting_list_compare(self, job_a, job_b): 
        w_a = self.aggregated_weight_of_job(self.weights_list, job_a)     
        w_b = self.aggregated_weight_of_job(self.weights_list, job_b)       
        if w_a < w_b:
            return  1
        elif w_a == w_b:
            return  0
        else:
            return -1
        

    def backfilling_compare(self, job_a, job_b):
        w_a = self.aggregated_weight_of_job(self.weights_backfill, job_a)     
        w_b = self.aggregated_weight_of_job(self.weights_backfill, job_b) 

        if w_a < w_b:
            return  1
        elif w_a == w_b:
            return  0
        else:
            return -1
        
    
    def print_waiting_list(self):
        for job in self.waiting_list_of_unscheduled_jobs:
            print job, "bypassed:", job.maui_bypass_counter
        print
        




