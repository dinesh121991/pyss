from orig_probabilistic_easy_scheduler import OrigProbabilisticEasyScheduler

    
class  OrigSingleDistProbabilisticEasyScheduler(OrigProbabilisticEasyScheduler):
    """ This algorithm implements a version of Feitelson and Nissimov, June 2007
        In this simplified version we have a single common distribution for all the jobs and users 
    """
    def new_events_on_job_submission(self, job, current_time):
        # TODO: Ugly patch - make all jobs have the same user
        # Warning - loses the original user_id, don't use the same jobs with other schedulers after
        job.user_id = "common_user"
        return super(OrigSingleDistProbabilisticEasyScheduler, self).new_events_on_job_submission(job, current_time)
