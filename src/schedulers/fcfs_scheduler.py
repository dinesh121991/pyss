from common import Scheduler, CpuSnapshot
from base.prototype import JobTerminationEvent

class FcfsScheduler(Scheduler):
        
    def __init__(self, num_processors):
        Scheduler.__init__(self, num_processors)
        self.cpu_snapshot = CpuSnapshot(num_processors)
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
        while len(self.waiting_queue_of_jobs) > 0:
            job = self.waiting_queue_of_jobs[0]
            if self.cpu_snapshot.free_nodes_available_at(time) >= job.num_required_processors: 
                self.waiting_queue_of_jobs.pop(0)
                self.cpu_snapshot.assignJob(job, time)     
                termination_time = time + job.actual_run_time
                newEvents.append( JobTerminationEvent(termination_time, job) )
            else:
                break
        return newEvents
