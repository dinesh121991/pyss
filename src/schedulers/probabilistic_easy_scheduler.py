from common import Scheduler, CpuSnapshot, list_copy
from base.prototype import JobStartEvent
from math import log

class Distribution(object):
    def __init__(self):
        self.bins = {}
        self.bins[1] = 0 # adding the first entry to the distribution main data structure
        self.number_of_jobs_added = 0
         
    def add_job(self, job):
        assert job.actual_run_time > 0
        
        rounded_run_time = int(log(job.actual_run_time, 2))
        self.number_of_jobs_added += 1

        if self.bins.has_key(rounded_run_time):
            self.bins[rounded_run_time] += 1 # incrementing the numbers of the numbers of terminated jobs encountered so far
            return
        
        # else: False == self.bins.has_key(rounded_run_time):
        self.bins[rounded_run_time] = 1   # we add a new entry initialized to 1
        tmp = rounded_run_time
        while tmp > 1:                    # and then we add entries with logarithmically smaller keys  
            tmp = tmp / 2
            if not self.bins.has_key(tmp):
                self.bins[tmp] = 0
            else:
                break
            


        
class  ProbabilisticEasyScheduler(Scheduler):
    """ This algorithm implements the algorithm in the paper of Tsafrir, Etzion, Feitelson, june 2007?
    """
    
    def __init__(self, num_processors, threshold = 0.05):
        super(ProbabilisticEasyScheduler, self).__init__(num_processors)
        self.threshold = threshold
        self.cpu_snapshot = CpuSnapshot(num_processors)
        self.unscheduled_jobs = []
        self.currently_running_jobs = []
        self.user_distribution = {}
    
    
    def new_events_on_job_submission(self, job, current_time):
        if not self.user_distribution.has_key(job.user_id): 
            self.user_distribution[job.user_id] = Distribution()
            
        self.cpu_snapshot.archive_old_slices(current_time)
        self.unscheduled_jobs.append(job)
        return [
            JobStartEvent(current_time, job)
            for job in self._schedule_jobs(current_time)
        ]


    def new_events_on_job_termination(self, job, current_time):
        self.user_distribution[job.user_id].add_job(job)
        self.currently_running_jobs.remove(job)
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return [
            JobStartEvent(current_time, job)
            for job in self._schedule_jobs(current_time)
        ]



    def _schedule_jobs(self, current_time):
        "Schedules jobs that can run right now, and returns them"
        jobs  = self._schedule_head_of_list(current_time)
        jobs += self._backfill_jobs(current_time)
        for job in jobs:
            self.currently_running_jobs.append(job)
        return jobs


    def _schedule_head_of_list(self, current_time):     
        result = []
        while True:
            if len(self.unscheduled_jobs) == 0:
                break
            # Try to schedule the first job
            if self.cpu_snapshot.free_processors_available_at(current_time) >= self.unscheduled_jobs[0].num_required_processors:
                job = self.unscheduled_jobs.pop(0)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
            else:
                # first job can't be scheduled
                break
        return result
    

    def _backfill_jobs(self, current_time):
        if len(self.unscheduled_jobs) <= 1:
            return []

        result = []  
        first_job = self.unscheduled_jobs[0]        
        tail =  self.unscheduled_jobs[1:]
                
        for job in tail:
            if self.can_be_probabilistically_backfilled(job, current_time): 
                self.unscheduled_jobs.remove(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
                
        return result

    


    def can_be_probabilistically_backfilled(self, second_job, current_time):
        assert len(self.unscheduled_jobs) >= 2
        assert second_job in self.unscheduled_jobs[1:]

        if self.cpu_snapshot.free_processors_available_at(current_time) < second_job.num_required_processors:
            return False

        first_job = self.unscheduled_jobs[0]
        second_job_distribution = self.user_distribution[second_job.user_id]

        if second_job_distribution.number_of_jobs_added == 0: # we still haven't collected any information about the user
            upper_bound = second_job.user_estimated_run_time
            if self.max_bottle_neck_up_to(upper_bound, second_job, current_time) < self.threshold:
                return True
            else:
                return False
            
    
        # main loop
        bad_prediction = 0
        for t in sorted(second_job_distribution.bins.keys()):
            second_job_probability_to_end_at_t = second_job_distribution.bins[t] / second_job_distribution.number_of_jobs_added
            
            bad_prediction += second_job_probability_to_end_at_t * self.max_bottle_neck_up_to(t, second_job, current_time)

        if bad_prediction < self.threshold:
            return True
        else:
            return False


    def max_bottle_neck_up_to(self, t, second_job, current_time):
        return 0
    
        
             

        """
        M = {}

        C = first_job.num_required_processors + second_job.num_required_processors
        
        for k in range(C + 1):
            M[0, k] = 0.0

        for j in range(len(self.running_jobs)):
            pass # bla bla bla 
        """
        return True ########################### ????????
