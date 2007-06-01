from common import Scheduler, CpuSnapshot
from events import JobTerminationEvent

class FcfsScheduler(Scheduler):
        
    def __init__(self, total_nodes = 100):
        self.cpu_snapshot = CpuSnapshot(total_nodes)
        self.waiting_queue_of_jobs = []
        
    def handleSubmissionOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        self.waiting_queue_of_jobs.append(job)
        return self._schedule_jobs(current_time)

    def handleTerminationOfJobEvent(self, job, current_time):
        self.cpu_snapshot.archive_old_slices(current_time)
        self.cpu_snapshot.delTailofJobFromCpuSlices(job)
        return self._schedule_jobs(current_time)

 
    def _schedule_jobs(self, time):
        newEvents = []
        first_failure_has_not_occured = True
        while len(self.waiting_queue_of_jobs) > 0 and first_failure_has_not_occured:
            job = self.waiting_queue_of_jobs[0]
            if self.cpu_snapshot.free_nodes_available_at(time) >= job.num_required_processors: 
                del self.waiting_queue_of_jobs[0]
                self.cpu_snapshot.assignJob(job, time)     
                termination_time = time + job.actual_run_time
                newEvents.append( JobTerminationEvent(termination_time, job) )
            else:
                first_failure_has_not_occured = False
        return newEvents
