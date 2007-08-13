from common import CpuSnapshot
from easy_scheduler import EasyBackfillScheduler


# this scheduler uses the actual run time as the prediction of the job and then apply the regular Easy Backfill Schedular

class DoublePerfectEasyBackfillScheduler(EasyBackfillScheduler):
    def __init__(self, num_processors):
        super(DoublePerfectEasyBackfillScheduler, self).__init__(num_processors)

    def new_events_on_job_submission(self, job, current_time):
        "Overriding parent method"
        job.predicted_run_time = 2 * job.actual_run_time
        return super(DoublePerfectEasyBackfillScheduler, self).new_events_on_job_submission(job, current_time)
