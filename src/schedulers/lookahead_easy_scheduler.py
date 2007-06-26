from common import CpuSnapshot
from base.prototype import JobStartEvent

from easy_scheduler import EasyBackfillScheduler


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
    
    def __init__(self, num_processors, sort_key_functions=None):
        super(LookAheadEasyBackFillScheduler, self).__init__(num_processors)



    def _backfill_jobs(self, current_time):
        "Overriding parent method"
        self._mark_jobs_in_look_ahead_best_order(current_time)
        result = []

        tail_of_waiting_list = self.unscheduled_jobs[1:]

        for job in tail_of_waiting_list:
            if job.backfill_flag == 1:
                self.unscheduled_jobs.remove(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)

        return result





    def _mark_jobs_in_look_ahead_best_order(self, current_time):
        print "current time (before reordering): ", current_time; self.cpu_snapshot.printCpuSlices()
        if len(self.unscheduled_jobs) <= 1:
            return

        free_processors = self.cpu_snapshot.free_processors_available_at(current_time)
        if free_processors == 0:
            return
        
        first_job = self.unscheduled_jobs[0]
        cpu_snapshot_with_first_job = self.cpu_snapshot.quick_copy()
        cpu_snapshot_with_first_job.assignJobEarliest(first_job, current_time)

        # M[j, k] represents the subset of jobs in {0...j} with the highest utilization if k processors are available
        M = {}  
        
        for k in range(free_processors + 1): 
            M[-1, k] = Entry(cpu_snapshot_with_first_job.copy())

        for j in range(len(self.unscheduled_jobs)):
            job = self.unscheduled_jobs[j]
            assert job.backfill_flag == 0 
            for k in range(free_processors + 1):
                print "++++", j, k 
                M[j, k] = Entry()
                M[j, k].utilization  =  M[j-1, k].utilization
                M[j, k].cpu_snapshot =  M[j-1, k].cpu_snapshot.copy()

                if (k < job.num_required_processors):
                    continue
                
                tmp_cpu_snapshot = M[j-1, k-job.num_required_processors].cpu_snapshot.copy()
                if tmp_cpu_snapshot.canJobStartNow(job, current_time):
                    tmp_cpu_snapshot.assignJob(job, current_time)
                else:
                    continue
                
                U1 = M[j, k].utilization
                U2 = M[j-1, k-job.num_required_processors].utilization + job.num_required_processors

                if U1 <= U2:
                    M[j, k].utilization = U2
                    M[j, k].cpu_snapshot = tmp_cpu_snapshot
                    
                print "the entry M[",j,",", k,"]: "; M[j,k].cpu_snapshot.printCpuSlices()


        best_entry = M[len(self.unscheduled_jobs) - 1, free_processors]
        print "______________the best entry:", best_entry.cpu_snapshot.printCpuSlices()
        for job in self.unscheduled_jobs:
            if job.id in best_entry.cpu_snapshot.slices[0].job_ids:        
                job.backfill_flag = 1  

        


