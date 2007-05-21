from common import *
from events import *

class ConservativeScheduler(Scheduler):

    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.list_of_unfinished_jobs_arranged_by_arrival_times = []    
        
    def handleArrivalOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        newEvents = Events()
        self.list_of_unfinished_jobs_arranged_by_arrival_times.append(job)        
        start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, current_time)
        self.cpu_snapshot.assignJob(job, start_time_of_job)
        termination_time = job.start_to_run_at_time + job.actual_duration
        newEvents.add_job_termination_event(termination_time, job)
        return newEvents
    
    def handleTerminationOfJobEvent(self, job, current_time):
        """ Here we delete the tail of job if it was ended before the duration declaration.
        It then reschedules the remaining jobs and returns a collection of new termination events
        (using the dictionary data structure) """
        self.cpu_snapshot.archive_old_slices(current_time)
        self.list_of_unfinished_jobs_arranged_by_arrival_times.remove(job)  
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        newEvents = Events()
        return self._reschedule_jobs(current_time, newEvents)
   

    def _reschedule_jobs(self, current_time, newEvents):
        for job in self.list_of_unfinished_jobs_arranged_by_arrival_times:
            if job.start_to_run_at_time <= current_time:
                continue # job started to run before, so it cannot be rescheduled (preemptions are not allowed)
            prev_start_to_run_at_time = job.start_to_run_at_time
            self.cpu_snapshot.delJobFromCpuSlices(job)
            start_time_of_job = self.cpu_snapshot.jobEarliestAssignment(job, current_time)
            self.cpu_snapshot.assignJob(job, start_time_of_job)
            assert prev_start_to_run_at_time >= job.start_to_run_at_time
            if prev_start_to_run_at_time != job.start_to_run_at_time:
                new_termination_time = job.start_to_run_at_time + job.actual_duration
                newEvents.add_job_termination_event(new_termination_time, job)
        return newEvents
