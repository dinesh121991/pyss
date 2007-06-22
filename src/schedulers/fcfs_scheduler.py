from common import Scheduler, CpuSnapshot
from base.prototype import JobStartEvent

class FcfsScheduler(Scheduler):

    def __init__(self, num_processors):
        super(FcfsScheduler, self).__init__(num_processors)
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


    def _schedule_jobs(self, current_time):
        newEvents = []
        while len(self.waiting_queue_of_jobs) > 0:
            job = self.waiting_queue_of_jobs[0]
            if self.cpu_snapshot.free_processors_available_at(current_time) >= job.num_required_processors:
                self.waiting_queue_of_jobs.pop(0)
                self.cpu_snapshot.assignJob(job, current_time)
                newEvents.append( JobStartEvent(current_time, job) )
            else:
                break
        return newEvents
