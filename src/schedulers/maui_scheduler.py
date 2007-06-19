from common import CpuSnapshot
from base.prototype import JobStartEvent

class Weights:
    # this class defines the configuration of weights for the MAUI
    def __init__(self, w_wtime=1, w_sld=0, w_user=0, w_bypass=0, w_admin=0, w_size=0):
        self.wtime  = w_wtime  # weight of wait time since submission
        self.sld    = w_sld    # weight of slow down
        self.user   = w_user   # weight of user desired quality of service
        self.bypass = w_bypass # weight of being skipped over in the waiting list
        self.admin  = w_admin  # weight of asmin desired quality of service
        self.size   = w_size   # weight of job size (= num_required_processors)


# a first toy version for the maui -- essentillay the diffrence between this simplified version of maui and easy
# backfilling is that the maui has more degree of freedom: maui may consider the jobs
# not necessarily by order of submission, as opposed to the easy backfill.

from easy_scheduler import EasyBackfillScheduler

class MauiScheduler(EasyBackfillScheduler):
    def __init__(self, num_processors, weights_list=None, weights_backfill=None):
        EasyBackfillScheduler.__init__(self, num_processors)
        self.maui_counter = 0
        self.current_time = 0

        # weights for calculation of priorities for the jobs in MAUI style
        if weights_list is not None:
            self.weights_list = weights_list
        else:
            self.weights_list = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of submission

        if weights_backfill is not None:
            self.weights_backfill = weights_backfill
        else:
            self.weights_backfill = Weights(1, 0, 0, 0, 0, 0) # sort the jobs by order of submission

    def handleSubmissionOfJobEvent(self, just_submitted_job, current_time):
        """ Here we first add the new job to the waiting list. We then try to schedule
        the jobs in the waiting list, returning a collection of new termination events """
        just_submitted_job.maui_counter = self.maui_counter; self.maui_counter += 1
        self.cpu_snapshot.archive_old_slices(current_time)
        self.waiting_list_of_unscheduled_jobs.append(just_submitted_job)
        return self._schedule_jobs(current_time)

    def _schedule_jobs(self, current_time):
        # Maui's scheduling methods are based on the analogue methods of EasyBackfill.
        # The additonal or different code lines are marked with ## +
        if len(self.waiting_list_of_unscheduled_jobs) == 0:
            return []
        self.current_time = current_time ## +
        self.waiting_list_of_unscheduled_jobs.sort(self.waiting_list_compare) ## +

        newEvents = self._schedule_the_head_of_the_waiting_list(current_time)  # call the method of EasyBackfill 
        newEvents += self._schedule_the_tail_of_the_waiting_list(current_time)  # overload the method of EasyBackfill (see below)
        return newEvents

    def _unscheduled_jobs_in_backfilling_order(self):
        # sort the tail, keep the first job first
        return self.waiting_list_of_unscheduled_jobs[0:1] + \
            sorted(self.waiting_list_of_unscheduled_jobs[1:], self.backfilling_compare)

    def _schedule_the_tail_of_the_waiting_list(self, current_time):
        self.waiting_list_of_unscheduled_jobs = self._unscheduled_jobs_in_backfilling_order() ## +

        backfilled_jobs = self._backfill_jobs(current_time)

        for job in backfilled_jobs:
            self.increment_bypass_counters_while_backfilling(job) ## +

        return [
            JobStartEvent(current_time, job)
            for job in backfilled_jobs
        ]

    def increment_bypass_counters_while_backfilling(self, backfilled_job):
        for job in self.waiting_list_of_unscheduled_jobs:
            if job.maui_counter < backfilled_job.maui_counter:
                job.maui_bypass_counter += 1

    def aggregated_weight_of_job(self, weights, job):
        wait = self.current_time - job.submit_time # wait time since submission of job
        sld = (wait + job.estimated_run_time) /  job.estimated_run_time
        w = weights

        weight_of_job = w.wtime  * wait + \
                     w.sld    * sld + \
                     w.user   * job.user_QoS + \
                     w.bypass * job.maui_bypass_counter + \
                     w.admin  * job.admin_QoS + \
                     w.size   * job.num_required_processors
        return weight_of_job

    def waiting_list_compare(self, job_a, job_b):
        w_a = self.aggregated_weight_of_job(self.weights_list, job_a)
        w_b = self.aggregated_weight_of_job(self.weights_list, job_b)
        return cmp(w_b, w_a)
    
    def backfilling_compare(self, job_a, job_b):
        w_a = self.aggregated_weight_of_job(self.weights_backfill, job_a)
        w_b = self.aggregated_weight_of_job(self.weights_backfill, job_b)
        return cmp(w_b, w_a)

    
    def print_waiting_list(self):
        for job in self.waiting_list_of_unscheduled_jobs:
            print job, "bypassed:", job.maui_bypass_counter
        print
