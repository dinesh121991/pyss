class Scheduler(object):
    """
    Assumption: every handler returns a (possibly empty) collection of new events
    """

    def __init__(self, num_processors):
        self.num_processors = num_processors

    def handleSubmissionOfJobEvent(self, job, current_time):
        raise NotImplementedError()

    def handleTerminationOfJobEvent(self, job, current_time):
        raise NotImplementedError()

class CpuTimeSlice(object):
    """
    represents a "tentative feasible" snapshot of the cpu between the
    start_time until start_time + dur_time.  It is tentative since a job might
    be rescheduled to an earlier slice. It is feasible since the total demand
    for processors ba all the jobs assigned to this slice never exceeds the
    amount of the total processors available.
    Assumption: the duration of the slice is never changed.
    We can replace this slice with a new slice with shorter duration.
    """

    def __init__(self, free_processors, start_time, duration, total_processors):
        assert duration > 0
        assert start_time >= 0
        assert total_processors > 0
        assert 0 <= free_processors <= total_processors

        self.total_processors = total_processors
        self.free_processors = free_processors
        self.start_time = start_time
        self.duration = duration

        self.job_ids = set()

    @property
    def end_time(self):
        return self.start_time + self.duration

    @property
    def busy_processors(self):
        return self.total_processors - self.free_processors

    def addJob(self, job):
        assert job.num_required_processors <= self.free_processors
        assert job.id not in self.job_ids, "job.id="+str(job.id)+", job_ids"+str(self.job_ids)
        self.free_processors -= job.num_required_processors
        self.job_ids.add(job.id)

    def delJob(self, job):
        #print "slice_id=%s, delJob(job_id=%s)" % (id(self), job.id)
        assert job.num_required_processors <= self.busy_processors
        self.free_processors += job.num_required_processors
        self.job_ids.remove(job.id)

    def __str__(self):
        return '%d %d %d %s' % (self.start_time, self.duration, self.free_processors, self.job_ids)

    def copy(self):
        result = CpuTimeSlice(
                free_processors = self.free_processors,
                start_time = self.start_time,
                duration = self.duration,
                total_processors = self.total_processors,
            )
        if len(self.job_ids) > 0:
            for j_id in self.job_ids:
                result.job_ids.add(j_id)

        return result

    def split(self, split_time):
        first = self.copy()
        first.duration = split_time - self.start_time

        second = self.copy()
        second.start_time = split_time
        second.duration = self.end_time - split_time

        return first, second


class CpuSnapshot(object):
    """
    represents the time table with the assignments of jobs to available processors
    """
    # Assumption: the snapshot always has at least one slice
    def __init__(self, total_processors):
        self.total_processors = total_processors
        self.slices=[]
        self.slices.append(CpuTimeSlice(self.total_processors, start_time=0, duration=1, total_processors=total_processors))
        self.archive_of_old_slices=[]

    def _slice_starts_at(self, time):
        for slice in self.slices:
            if slice.start_time == time:
                return True
        return False # no slice found

    def _slice_index_to_split(self, split_time):
        assert not self._slice_starts_at(split_time)

        for index, slice in enumerate(self.slices):
            if slice.start_time < split_time < slice.end_time:
                return index

        assert False # should never reach here

    @property
    def snapshot_end_time(self):
        assert len(self.slices) > 0
        return self.slices[-1].end_time

    def _append_time_slice(self, free_processors, duration):
        self.slices.append(CpuTimeSlice(free_processors, self.snapshot_end_time, duration, self.total_processors))

    def _ensure_a_slice_starts_at(self, start_time):
        """
        A preprocessing stage.
        
        Usage:
        First, to ensure that the assignment time of the new added job will
        start at a beginning of a slice.

        Second, to ensure that the actual end time of the job will end at the
        ending of slice.  we need this when we add a new job, or delete a tail
        of job when the user estimation is larger than the actual duration.

        The idea: we first append 2 slices, just to make sure that there's a
        slice which ends after the start_time.  We add one more slice just
        because we actually use list.insert() when we add a new slice.
        After that we iterate through the slices and split a slice if needed
        """

        if self._slice_starts_at(start_time):
            return # already have one

        snapshot_end_time = self.slices[-1].end_time

        if start_time < snapshot_end_time:
            # split an existing slice
            index = self._slice_index_to_split(start_time)

            # splitting slice s with respect to the start time
            slice = self.slices.pop(index)
            self.slices[index:index] = slice.split(start_time)
            return

        if start_time > snapshot_end_time:
            # add slice until start_time
            self._append_time_slice(self.total_processors, start_time - snapshot_end_time)
            assert self.snapshot_end_time == start_time

        # add a tail slice, duration is arbitrary
        self._append_time_slice(self.total_processors, 1000)

    def free_processors_available_at(self, time):
        for s in self.slices:
            if s.start_time <= time < s.end_time:
                return s.free_processors
        return self.total_processors

    def canJobStartNow(self, job, current_time):
        return self.jobEarliestAssignment(job, current_time) == current_time

    def jobEarliestAssignment(self, job, time):
        """
        returns the earliest time right after the given time for which the job
        can be assigned enough processors for job.estimated_run_time unit of
        times in an uninterrupted fashion.
        Assumptions: the given is greater than the submission time of the job >= 0.
        """
        assert job.num_required_processors <= self.total_processors

        # TODO: why +10?
        self._append_time_slice(self.total_processors, time + job.estimated_run_time + 10)

        partially_assigned = False
        tentative_start_time = accumulated_duration = 0

        assert time >= 0

        for s in self.slices: # continuity assumption: if t' is the successor of t, then: t' = t + duration_of_slice_t
            if s.end_time <= time or s.free_processors < job.num_required_processors:
                # the job can't be assigned to this slice, need to reset
                # partially_assigned and accumulated_duration
                partially_assigned = False
                accumulated_duration = 0

            elif not partially_assigned:
                # we'll check if the job can be assigned to this slice and perhaps to its successive
                partially_assigned = True
                tentative_start_time =  max(time, s.start_time)
                accumulated_duration = s.end_time - tentative_start_time

            else:
                # job is partially_assigned:
                accumulated_duration += s.duration

            if accumulated_duration >= job.estimated_run_time:
                self.slices[-1].duration = 1000 # making sure that the last "empty" slice we've just added will not be huge
                return tentative_start_time

        assert False # should never reach here

    def _slices_time_range(self, start, end):
        assert self._slice_starts_at(start)
        assert self._slice_starts_at(end)

        return (s for s in self.slices if start <= s.start_time < end)

    def assignJob(self, job, job_start):
        """
        assigns the job to start at the given job_start time.
        Important assumption: job_start was returned by jobEarliestAssignment.
        """
        job.start_to_run_at_time = job_start
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)

        for s in self._slices_time_range(job_start, job_estimated_finish_time):
            s.addJob(job)

    def assignJobEarliest(self, job, time):
        self.assignJob(job, self.jobEarliestAssignment(job, time))

    def delJobFromCpuSlices(self, job):
        """
        Deletes an _entire_ job from the slices.
        Assumption: job resides at consecutive slices (no preemptions), and
        nothing is archived!
        """
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        job_start = job.start_to_run_at_time
        self._ensure_a_slice_starts_at(job_start)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)

        for s in self._slices_time_range(job_start, job_estimated_finish_time):
            s.delJob(job)

    def delTailofJobFromCpuSlices(self, job):
        """
        This function is used when the actual duration is smaller than the
        estimated duration, so the tail of the job must be deleted from the
        slices.
        We iterate trough the sorted slices until the critical point is found:
        the point from which the tail of the job starts.
        Assumption: job is assigned to successive slices. Specifically, there
        are no preemptions.
        """

        if job.actual_run_time ==  job.estimated_run_time:
            return
        job_finish_time = job.start_to_run_at_time + job.actual_run_time
        job_estimated_finish_time = job.start_to_run_at_time + job.estimated_run_time
        self._ensure_a_slice_starts_at(job_finish_time)
        self._ensure_a_slice_starts_at(job_estimated_finish_time)

        for s in self._slices_time_range(job_finish_time, job_estimated_finish_time):
            s.delJob(job)

    def archive_old_slices(self, current_time):
        while self.slices[:-1] and self.slices[0].end_time < current_time:
            self.archive_of_old_slices.append( self.slices.pop(0) )
        self.unify_some_slices()

    def unify_some_slices(self):
        prev = self.slices[0]
        for s in self.slices[1: ]:
            if prev.free_processors == s.free_processors and s.job_ids == prev.job_ids:
                prev.duration += s.duration
                self.slices.remove(s)
            else:
                prev = s

    def _restore_old_slices(self):
        size = len(self.archive_of_old_slices)
        while size > 0:
            size -= 1
            s = self.archive_of_old_slices.pop()
            self.slices.insert(0, s)

    def printCpuSlices(self):
        print "start time | duration | #free processors | jobs"
        for s in self.slices:
            print s
        print

    def copy(self):
        result = CpuSnapshot(self.total_processors)
        result.slices = [slice.copy() for slice in self.slices]
        return result

    def CpuSlicesTestFeasibility(self):
        self._restore_old_slices()
        duration = 0
        time = 0

        for s in self.slices:
            prev_duration = duration
            prev_time = time

            if s.free_processors < 0 or s.free_processors > self.total_processors:
                print ">>> PROBLEM: number of free processors is either negative or huge", s
                return False

            if s.start_time != prev_time + prev_duration:
                print ">>> PROBLEM: non successive slices", s.start_time, prev_time
                return False

            duration = s.duration
            time = s.start_time

        return True

    def CpuSlicesTestEmptyFeasibility(self):
        self._restore_old_slices()
        duration = 0
        time = 0

        for s in self.slices:
            prev_duration = duration
            prev_time = time

            if s.free_processors != self.total_processors:
                print ">>> PROBLEM: number of free processors is not the total processors", s
                return False

            if s.start_time != prev_time + prev_duration:
                print ">>> PROBLEM: non successive slices", s.start_time, prev_time
                return False

            duration = s.duration
            time = s.start_time

        return True

