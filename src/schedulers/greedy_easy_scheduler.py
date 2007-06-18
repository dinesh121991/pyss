from common import CpuSnapshot
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler

class  BasicCompareFunctions(object):

    def cmp0(self, job_a, job_b):
        cmp(job_b.submit_time, job_a.submit_time)
        
    def cmp1(self, job_a, job_b):
        cmp(job_a.submit_time, job_b.submit_time)

    def cmp2(self, job_a, job_b):
        cmp (job_a.num_processors, job_b.num_processors)

    def cmp3(self, job_a, job_b):
        cmp(job_a.estimated_run_time, job_b.estimated_run_time)

    def cmp4(self, job_a, job_b):
        cmp(job_a.num_processors * job_a.estimated_run_time, job_b.num_processors * job_b.estimated_run_time)


class BasicLocalEvaluationFuction(object):

    def value(self, list_of_jobs):
        val = 0.0
        for job in list_of_jobs:
             val += job.num_processors * job.estimated_run_time  
        return val
    
    
class  GreedyEasyBackFillScheduler(EasyBackfillScheduler):
    def __init__(self, num_processors, list_of_compare_functions=None, value_function=None):
        EasyBackfillScheduler.__init__(self, num_processors)
        
        self.list_of_compare_functions = []
        if list_of_compare_functions == None:
            bf = BasicCompareFunctions()
            self.list_of_compare_functions = [bf.cmp0, bf.cmp1, bf.cmp2, bf.cmp3]
        else:
            self.list_of_compare_functions = list_of_compare_functions
                
        if value_function == None:
            bv = BasicLocalEvaluationFuction()
            self.value_function = bv.value
        else:
            self.value_function = value_function
            

    def _schedule_jobs(self, current_time):
        # Maui's scheduling methods are based on the analogue methods of EasyBackfill.
        # The additonal or different code lines are marked with ## +
        newEvents = []
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return newEvents
        self.waiting_list_of_unscheduled_jobs.sort(self.submit_time_compare) ## +
        self._schedule_the_head_of_the_waiting_list(current_time, newEvents)  # call the method of EasyBackfill 
        self._schedule_the_tail_of_the_waiting_list(current_time, newEvents)  # overload the method of EasyBackfill (see below)
        self.waiting_list_of_unscheduled_jobs.sort(self.submit_time_compare) ## +
        return newEvents

    def _schedule_the_tail_of_the_waiting_list(self, current_time, newEvents):
        if len(self.waiting_list_of_unscheduled_jobs) > 1:
            first_job = self.waiting_list_of_unscheduled_jobs.pop(0) ## +
            self._find_an_approximate_best_order_of_the_jobs(current_time, first_job) ## + 
            for next_job in self.waiting_list_of_unscheduled_jobs:
                if self.canBeBackfilled(first_job, next_job, current_time):
                    self.waiting_list_of_unscheduled_jobs.remove(next_job)
                    self.cpu_snapshot.assignJob(next_job, current_time)
                    newEvents.append( JobStartEvent(current_time, next_job) )
            self.waiting_list_of_unscheduled_jobs.append(first_job) # +
        return newEvents


    def _find_an_approximate_best_order_of_the_jobs(current_time):
        shadow_time = self.cpu_snapshot.jobEarliestAssignment(first_job, current_time)
        self.cpu_snapshot.assignJob(first_job, shadow_time)
        index_of_rank_with_max_value = 0
        max_value = 0
        for index in range(len(self.list_of_compare_functions)):
            tmp_cpu_snapshot = self.cpu_snapshot.clone()
            tentative_list_of_jobs = []
            self.waiting_list_of_unscheduled_jobs.sort(self.list_of_compare_functions[index])
            for job in self.waiting_list_of_unscheduled_jobs:
                if current_time == tmp_cpu_snapshot.jobEarliestAssignment(job, current_time):
                    self.tmp_cpu_snapshot.assignJob(job, current_time)
                    tentative_list_of_jobs.append(job)

            value = self.value_function(tentative_list_of_jobs)
            if max_value < value:
                max_value = value
                index_of_rank_with_max_value = index
                
        self.cpu_snapshot.delJobFromCpuSlices(first_job)
        self.waiting_list_of_unscheduled_jobs.sort(self.list_of_compare_functions[index_of_rank_with_max_value])
        
            
      


    def submit_time_compare(self, job_a, job_b):
        cmp(job_b.submit_time, job_a.submit_time) 

    def num_processors_compare(self, job_a, job_b):
        cmp (job_a.num_processors, job_b.num_processors)

    def print_waiting_list(self):
        for job in self.waiting_list_of_unscheduled_jobs:
            print job
        print
