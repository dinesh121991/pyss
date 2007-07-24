from common import Scheduler, CpuSnapshot, list_copy
from base.prototype import JobStartEvent
from math import log

class Distribution(object):
    def __init__(self, job = None):
        self.bins = {}
        self.bins[1] = 0 # adding the first entry to the distribution main data structure
        self.number_of_jobs_added = 0

        if job is not None: # we init the distribution to be Uniform 
            self.touch(job.user_estimated_run_time)
            for key in self.bins.keys():
                self.bins[key] = 1
                self.number_of_jobs_added += 1 
            

    def touch(self, time): # just add empty entries
        rounded_up_time = pow(2, int(log(time, 2)) + 1)
        while rounded_up_time > 1: # we add entries with logarithmically smaller keys and zero values   
            rounded_up_time = rounded_up_time / 2
            if not self.bins.has_key(rounded_up_time):
                self.bins[rounded_up_time] = 0
            else:
                break

            
    def add_job(self, job):
        assert job.actual_run_time > 0
        
        rounded_up_run_time = pow(2, int(log(job.actual_run_time, 2)) + 1)
        self.number_of_jobs_added += 1
        
        if self.bins.has_key(rounded_up_run_time):
            self.bins[rounded_up_run_time] += 1 # incrementing the numbers of the numbers of terminated jobs encountered so far
        else: 
            self.bins[rounded_up_run_time]  = 1   # we add a new entry initialized to 1
            self.touch(rounded_up_run_time)
        
            
        
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
            self.user_distribution[job.user_id] = Distribution(job)            
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
        return jobs


    def _schedule_head_of_list(self, current_time):     
        result = []
        while True:
            if len(self.unscheduled_jobs) == 0:
                break
            # Try to schedule the first job
            if self.cpu_snapshot.free_processors_available_at(current_time) >= self.unscheduled_jobs[0].num_required_processors:
                job = self.unscheduled_jobs.pop(0)
                self.currently_running_jobs.append(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
            else:
                # first job can't be scheduled
                break
        return result
    

    def _backfill_jobs(self, current_time):
        if len(self.unscheduled_jobs) <= 1:
            return []

        result    = []  
        first_job = self.unscheduled_jobs[0]        
        tail      = self.unscheduled_jobs[1:]
                
        for job in tail:
            if self.can_be_probabilistically_backfilled(job, current_time): 
                self.unscheduled_jobs.remove(job)
                self.currently_running_jobs.append(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)
                
        return result


    def can_be_probabilistically_backfilled(self, second_job, current_time):
        assert len(self.unscheduled_jobs) >= 2
        assert second_job in self.unscheduled_jobs[1:]

        first_job = self.unscheduled_jobs[0]
        second_job_distribution = self.user_distribution[second_job.user_id]

        if self.cpu_snapshot.free_processors_available_at(current_time) < second_job.num_required_processors:
            return False

        bad_prediction = 0
        for t in sorted(second_job_distribution.bins.keys()):
            print t, second_job_distribution.bins[t]
            second_job_probability_to_end_at_t = second_job_distribution.bins[t] / second_job_distribution.number_of_jobs_added
            bad_prediction += second_job_probability_to_end_at_t * self.max_bottle_neck_up_to(t, second_job, first_job, current_time)
    
        if bad_prediction < self.threshold:
            return True
        else:
            return False


    def probability_to_end_upto(self, time, job):
            return 1.0
                     

    def max_bottle_neck_up_to(self, time, second_job, first_job, current_time):
        
        for job in self.currently_running_jobs:
            self.user_distribution[job.user_id].touch(time)

        result = 0.0
        tmp_time = 1

        while tmp_time <= time:
            M = {}
            C = first_job.num_required_processors + second_job.num_required_processors

            #M[n,c] denotes the probablity that at tmp_time the first n jobs among those that
            # are currently running have released at least c processors

            for c in range(C + 1): 
                M[-1, c] = 0.0

            for n in range(len(self.currently_running_jobs)):
                M[n, 0] = 1.0

            for n in range(len(self.currently_running_jobs)):
                job = self.currently_running_jobs[n]

                Pn = self.probability_to_end_upto(tmp_time, job)
                
                for c in range (C + 1):
                    if c >= job.num_required_processors:  
                        M[n, c] = M[n-1, c] + (M[n-1, c-job.num_required_processors] - M[n-1, c]) * Pn 
                    else:
                        M[n, c] = M[n-1, c] + (1 - M[n-1, c]) * Pn

                        
            print M 
            result = max (M[n, first_job.num_required_processors] - M[n, C], result)
            
            tmp_time *= 2

        return result 


            
        #rounded_down_run_time = pow(2, int(log(current_time - job.start_to_run_at_time, 2)))
        #self.user_distribution[job.user_id].bins
        # for int(log(job.actual_run_time, 2))
        # return 0

     
