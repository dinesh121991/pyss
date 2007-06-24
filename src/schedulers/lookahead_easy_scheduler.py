from common import CpuSnapshot
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler


def basic_score_function(list_of_jobs):
    return sum(job.num_processors for job in list_of_jobs)

class Entry(object):
    def __init__(self, cpu_snapshot = None):
        self.utilization = 0
        self.cpu_snapshot = cpu_snapshot

    def __str__(self):
        return '%d' % (self.utilization)

  
        
class LookAheadEasyBackFillScheduler(EasyBackfillScheduler):
    """
    
    This scheduler implements the LOS Scheduling Algorithm [Edi Shmueli and Dror Feitelson, 2005]
    Essentially this algorithm uses a dynamic programing method to decide which subset of jobs to backfill
    """
    
    def __init__(self, num_processors, sort_key_functions=None, score_function=None):
        super(LookAheadEasyBackFillScheduler, self).__init__(num_processors)

        if score_function is None:
            self.score_function = basic_score_function
        else:
            self.score_function = score_function

    def _schedule_jobs(self, current_time):
        self.unscheduled_jobs.sort(key = self._submit_job_sort_key)

        result = super(LookAheadEasyBackFillScheduler, self)._schedule_jobs(current_time)

        self.unscheduled_jobs.sort(key = self._submit_job_sort_key)

        return result


    def _backfill_jobs(self, current_time):
        "Overriding parent method"
        self._reorder_jobs_in_look_ahead_best_order(current_time)
        return super(LookAheadEasyBackFillScheduler, self)._backfill_jobs(current_time)



    def _reorder_jobs_in_look_ahead_best_order(self, current_time):
        if len(self.unscheduled_jobs) == 0:
            return
        first_job = self.unscheduled_jobs[0]
        cpu_snapshot_with_job = self.cpu_snapshot.copy()
        cpu_snapshot_with_job.assignJobEarliest(first_job, current_time)
        cpu_snapshot_with_job._ensure_a_slice_starts_at(current_time)
        cpu_snapshot_with_job.archive_old_slices(current_time)
        free_processors = self.cpu_snapshot.free_processors_available_at(current_time)
        if free_processors == 0:
            return 
        M = {}
        # M[j, k] represents the best subset of the jobs {1...j} (according to the score function) if k processors are available

        jobs_queue_size =  len(self.unscheduled_jobs)
       
        
        
        for k in range(free_processors + 1):
            M[-1, k] = Entry(cpu_snapshot_with_job.copy())
            
        for j in range(jobs_queue_size):
            job = self.unscheduled_jobs[j]
            print "current_time:", current_time; print job
            for k in range(free_processors + 1):
                print "++++", j, k 
                M[j, k] = Entry()
                M[j, k].utilization  =  M[j-1, k].utilization
                M[j, k].cpu_snapshot =  M[j-1, k].cpu_snapshot.copy()

                if (k < job.num_required_processors):
                    continue
                
                tmp_cpu_snapshot = M[j-1, k - job.num_required_processors].cpu_snapshot.copy()
                if tmp_cpu_snapshot.canJobStartNow(job, current_time):
                    tmp_cpu_snapshot.assignJob(job, current_time)
                else:
                    continue
                
                U1 = M[j, k].utilization
                U2 = M[j-1, k - job.num_required_processors].utilization + job.num_required_processors
                if U1 <= U2:
                    M[j, k].utilization = U2
                    M[j, k].cpu_snapshot = tmp_cpu_snapshot

        
        for job in self.unscheduled_jobs:
            best_entry = M[jobs_queue_size -1, free_processors]
            if job.id in best_entry.cpu_snapshot.slices[0].job_ids:
        
                job.start_to_run_at_time = -2  ### should be replaced ..... 
            else:
                job.start_to_run_at_time = -1
       
        
        self.unscheduled_jobs.sort(key = self._backfill_sort_key)
        self.print_waiting_list()
        


    def _submit_job_sort_key(self, job):
        return job.submit_time

    def _backfill_sort_key(self, job):
        return -job.start_to_run_at_time


    def canBeBackfilled(self, job, current_time):
        "Overriding parent method"
        return self.cpu_snapshot.canJobStartNow(job, current_time)

