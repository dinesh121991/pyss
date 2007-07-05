from common import CpuSnapshot, list_copy
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler

default_sort_key_functions = (
    lambda job : -job.submit_time, # sort by reverse submission time
    lambda job :  job.submit_time,
    lambda job :  job.num_processors,
    lambda job :  job.estimated_run_time,
    lambda job :  job.num_processors * job.estimated_run_time,
)

def basic_score_function(list_of_jobs):
    return sum(job.num_required_processors * job.estimated_run_time for job in list_of_jobs)



class  GreedyEasyBackFillScheduler(EasyBackfillScheduler):
    """
    This scheduler uses a greedy method to decide which subset of jobs to backfill.
    Specifically, the algorithm sorts the jobs in the waiting list by several orders
    (e.g. the orders in the default_sort_key_functions) and score each subsets using the given scoring function.
    Then, the algorithm backfill the list of jobs given by the order with the best score.
    The implemelmentation of this scheduler is based mainly on the EasyBackfillScheduler class.
    The single difference is that we only overide the _backfill_jobs function.
    This function calls _reorder_jobs_in_approximate_best_order before the preforming backfilling itself.
    """
    def __init__(self, num_processors, sort_key_functions=None, score_function=None):
        super(GreedyEasyBackFillScheduler, self).__init__(num_processors)

        if sort_key_functions is None:
            self.sort_key_functions = default_sort_key_functions
        else:
            self.sort_key_functions = sort_key_functions

        if score_function is None:
            self.score_function = basic_score_function
        else:
            self.score_function = score_function


    def _backfill_jobs(self, current_time):
        "Overriding parent method"
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []
        
        best_tail_of_jobs = self._reorder_jobs_in_approximate_best_order(current_time)
        
        first_job = self.unscheduled_jobs[0]
        self.cpu_snapshot.assignJobEarliest(first_job, current_time)
        
        for job in best_tail_of_jobs:
            if self.cpu_snapshot.canJobStartNow(job, current_time): 
                self.unscheduled_jobs.remove(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
                
        self.cpu_snapshot.delJobFromCpuSlices(first_job)

        return result



    def _reorder_jobs_in_approximate_best_order(self, current_time):        
        first_job = self.unscheduled_jobs[0]
        cpu_snapshot_with_job = self.cpu_snapshot.quick_copy()
        cpu_snapshot_with_job.assignJobEarliest(first_job, current_time)
        tail =  list_copy(self.unscheduled_jobs[1:])

        # get tail from best (score, tail) tuple
        best_tail = max(
            self._scored_tail(cpu_snapshot_with_job, sort_key_func, current_time, tail)
            for sort_key_func in self.sort_key_functions
        )[1]
        
        return best_tail


    def _scored_tail(self, cpu_snapshot, sort_key_func, current_time, tail):
        tmp_cpu_snapshot = cpu_snapshot.quick_copy()
        tentative_list_of_jobs = []
        sorted_tail = sorted(tail, key=sort_key_func)
        for job in sorted_tail:
            if tmp_cpu_snapshot.canJobStartNow(job, current_time):
                tmp_cpu_snapshot.assignJob(job, current_time)
                tentative_list_of_jobs.append(job)

        return self.score_function(tentative_list_of_jobs), sorted_tail


   
