#!/usr/bin/env python2.4

from sim  import *
from sim1 import *
from sim2 import *
import sys


# a first toy version 


class MauiScheduler(EasyBackfillScheduler):
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_list_of_unscheduled_jobs = []
        
        
    def waiting_list_compare(self, job_a, job_b):
        weight_a = 1000 * job_a.admin_QoS + 0 * job_a.user_QoS
        weight_b = 1000 * job_b.admin_QoS + 0 * job_b.user_QoS
         
        if weight_a > weight_b:
            return -1
        elif weight_a == weight_b:
            return  0
        else:
            return  1
        
    def backfilling_compare(self, job_a, job_b):
        weight_a = 0 * job_a.admin_QoS + 1000 * job_a.user_QoS
        weight_b = 0 * job_b.admin_QoS + 1000 * job_b.user_QoS

        if weight_a > weight_b:
            return -1
        elif weight_a == weight_b:
            return  0
        else:
            return  1

        
    def handleArrivalOfJobEvent(self, just_arrived_job, time):
        """ Here we first add the new job to the waiting list. We then try to schedule
        the jobs in the waiting list, returning a collection of new termination events """
        self.waiting_list_of_unscheduled_jobs.append(just_arrived_job)
        return self._schedule_jobs(time)  


             
    def handleTerminationOfJobEvent(self, job, time):
        """ Here we first delete the tail of the just terminated job (in case it's
        done before user estimation time). We then try to schedule the jobs in the waiting list,
        returning a collection of new termination events """
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(time)
    

    def print_waiting_list(self):
        for job in self.waiting_list_of_unscheduled_jobs:
            print job
        print
        

    def _schedule_jobs(self, time):
        newEvents = Events()
                             
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return newEvents # waiting list is empty        
        
        #first, try to schedule the "head" of the waiting list ordered by the respective order
        self.waiting_list_of_unscheduled_jobs.sort(self.waiting_list_compare)
        
        while len(self.waiting_list_of_unscheduled_jobs) > 0: 
            first_job = self.waiting_list_of_unscheduled_jobs[0]
            start_time_of_first_job = self.cpu_snapshot.jobEarliestAssignment(first_job, time)
            if start_time_of_first_job == time:
                print ">>>> job has been scheduled:", first_job
                self.waiting_list_of_unscheduled_jobs.remove(first_job)
                self.cpu_snapshot.assignJob(first_job, time)
                termination_time = time + first_job.actual_duration
                newEvents.add_job_termination_event(termination_time, first_job) 
            else:
                break

        #then, try to backfill the "tail" of the waiting list ordered by the respective order
        if len(self.waiting_list_of_unscheduled_jobs) > 1:
            first_job = self.waiting_list_of_unscheduled_jobs.pop(0)
            print "While trying to backfill ...."
            print "first job is:", first_job 
            print ">>>> waiting list by waiting list priority:"
            self.print_waiting_list()
            self.waiting_list_of_unscheduled_jobs.sort(self.backfilling_compare)
            print ">>>> waiting list by backfilling prioroty:"
            self.print_waiting_list()
            for next_job in self.waiting_list_of_unscheduled_jobs: 
                if self.canBeBackfilled(first_job, next_job, time):
                    self.waiting_list_of_unscheduled_jobs.remove(next_job)
                    time = self.cpu_snapshot.jobEarliestAssignment(next_job, time)
                    self.cpu_snapshot.assignJob(next_job, time)
                    termination_time = time + next_job.actual_duration
                    newEvents.add_job_termination_event(termination_time, next_job)
            self.waiting_list_of_unscheduled_jobs.append(first_job)
        return newEvents

    

  
        
      




