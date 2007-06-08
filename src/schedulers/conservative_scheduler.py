from common import Scheduler, CpuSnapshot
from base.prototype import JobTerminationEvent

class ConservativeScheduler(Scheduler):

    def __init__(self, num_processors):
        Scheduler.__init__(self, num_processors)
        self.cpu_snapshot = CpuSnapshot(num_processors)
        self.list_of_unfinished_jobs_arranged_by_submit_times = []    
        
    def handleSubmissionOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        self.list_of_unfinished_jobs_arranged_by_submit_times.append(job)        
        start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, current_time)
        self.cpu_snapshot.assignJob(job, start_time_of_job)
        termination_time = job.start_to_run_at_time + job.actual_run_time
        return [ JobTerminationEvent(termination_time, job) ]
    
    def handleTerminationOfJobEvent(self, job, current_time):
        """ Here we delete the tail of job if it was ended before the duration declaration.
        It then reschedules the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.list_of_unfinished_jobs_arranged_by_submit_times.remove(job)  
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._reschedule_jobs(current_time)
   

    def _reschedule_jobs(self, current_time):
        newEvents = []
        for job in self.list_of_unfinished_jobs_arranged_by_submit_times:
            if job.start_to_run_at_time <= current_time:
                continue # job started to run before, so it cannot be rescheduled (preemptions are not allowed)
            prev_start_to_run_at_time = job.start_to_run_at_time
            self.cpu_snapshot.delJobFromCpuSlices(job)
            start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, current_time)
            self.cpu_snapshot.assignJob(job, start_time_of_job)
            assert prev_start_to_run_at_time >= job.start_to_run_at_time
            if prev_start_to_run_at_time != job.start_to_run_at_time:
                new_termination_time = job.start_to_run_at_time + job.actual_run_time
                newEvents.append( JobTerminationEvent(new_termination_time, job) )
        return newEvents
