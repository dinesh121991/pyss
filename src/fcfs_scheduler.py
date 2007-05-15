from common import *
from events import *

class FcfsScheduler(Scheduler):
        
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_queue_of_jobs = []
        
    def handleArrivalOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        self.waiting_queue_of_jobs.append(job)
        return self._schedule_jobs(current_time)

    def handleTerminationOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(current_time)

 
    def _schedule_jobs(self, time):
        newEvents = Events()
        first_failure_has_not_occured = True
        while len(self.waiting_queue_of_jobs) > 0 and first_failure_has_not_occured:
            job = self.waiting_queue_of_jobs[0]
            earliest_possible_time = self.cpu_snapshot.jobEarliestAssignment(job, time)
            if earliest_possible_time == time:
                del self.waiting_queue_of_jobs[0]
                self.cpu_snapshot.assignJob(job, time)     
                termination_time = time + job.actual_duration
                newEvents.add_job_termination_event(termination_time, job)
            else:
                first_failure_has_not_occured = False
        return newEvents
