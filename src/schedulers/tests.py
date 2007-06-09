#!/usr/bin/env python2.4

import unittest
from simulator import run_simulator
from fcfs_scheduler import FcfsScheduler
from conservative_scheduler import ConservativeScheduler
from easy_scheduler import EasyBackfillScheduler
from maui_scheduler import MauiScheduler, Weights

import os
INPUT_FILE_DIR = os.path.dirname(__file__) + "/Input_test_files"

NUM_PROCESSORS=100

def feasibility_check_of_data(num_processors, jobs):
    """ Reconstructs a schedule from the jobs (using the values:
    job.submit_time, job.start_to_run_at_time, job_actual_run_time for each job),
    and then checks the feasibility of this schedule.
    Then check the actual slices of the scheduler itself. Then deletes the jobs from the
    actual scheduler expecting to see slices with free_processors == num_processors"""

    from common import CpuSnapshot
    cpu_snapshot = CpuSnapshot(num_processors)

    from base.prototype import Job
    j = Job(1, 1, 1, 1, 1)

    for job in jobs:
        assert job.submit_time <= job.start_to_run_at_time, "PROBLEM: job starts before submission...."

        if job.actual_run_time > 0:
            j.id = job.id
            j.num_required_processors = job.num_required_processors
            j.submit_time = job.submit_time
            j.actual_run_time = job.actual_run_time
            cpu_snapshot.assignJob(j, job.start_to_run_at_time)

    assert cpu_snapshot.CpuSlicesTestFeasibility()

def feasibility_check_of_cpu_snapshot(jobs, cpu_snapshot):
    assert cpu_snapshot.CpuSlicesTestFeasibility()

    from base.prototype import Job
    j = Job(1, 1, 1, 1, 1)

    for job in jobs:
        # print job
        j.num_required_processors = job.num_required_processors
        j.start_to_run_at_time = job.start_to_run_at_time
        j.estimated_run_time = j.actual_run_time = job.actual_run_time
        cpu_snapshot.delJobFromCpuSlices(j)

    assert cpu_snapshot.CpuSlicesTestEmptyFeasibility()

class test_Simulator(unittest.TestCase):

    def test_basic_fcfs(self):
        for i in range(25):
            simulator = run_simulator(scheduler=FcfsScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/basic_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                # the input jobs have their expected finish time encoded in their name.
                # to prevent id collisions their names are 'XX.YY' where XX is the expected time
                expected_finish_time = int(job.id.split(".")[0])

                self.assertEqual(expected_finish_time, job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

    def test_basic_conservative(self):
        for i in range(25):
            simulator = run_simulator(scheduler=ConservativeScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/basic_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

    def test_basic_easyBackfill(self):
        for i in range(25):
            simulator = run_simulator(scheduler =EasyBackfillScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/basic_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

    def test_basic_maui(self):
        for i in range(25):
            simulator = run_simulator(scheduler=MauiScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/basic_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))



    def test_fcfs(self):
        for i in range(8):
            simulator = run_simulator(scheduler=FcfsScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/fcfs_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))



    def test_conservative(self):
        for i in range(9):
            simulator = run_simulator(scheduler=ConservativeScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

        for i in range(2):
            simulator = run_simulator(scheduler=ConservativeScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/cons_bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))



    def test_easyBackfill(self):
        for i in range(9):
            simulator = run_simulator(scheduler=EasyBackfillScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

        for i in range(1):
            simulator = run_simulator(scheduler=EasyBackfillScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/easy_bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))


    # below we test the weigths of maui: w_wtime, w_sld, w_user, w_bypass, w_admin, w_size

    def test_maui_wtime(self):
        # here we basically test that the maui with the default weights behaves as the easybackfill
        for i in range(9):
            simulator = run_simulator(scheduler=MauiScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

        for i in range(1):
            simulator = run_simulator(scheduler=MauiScheduler(NUM_PROCESSORS), num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/easy_bf_input." + str(i))
            feasibility_check_of_data(simulator.num_processors, simulator.jobs)
            feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time, "i="+str(i))

    def test_maui_wait_and_size(self):
        # testing w_size = number of processors (vs. w_wait):
        # (w_wtime, w_sld, w_user, w_bypass, w_admin, w_size)
        w_l = Weights(1, 0, 0, 0, 0, 0)
        w_b = Weights(0, 0, 0, 0, 0, -1)

        scheduler = MauiScheduler(NUM_PROCESSORS, weights_list = w_l, weights_backfill = w_b)
        simulator = run_simulator(scheduler=scheduler, num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/maui.size")
        feasibility_check_of_data(simulator.num_processors, simulator.jobs)
        feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
        for job in simulator.jobs:
            self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time)

    def test_maui_admin_vs_userQoS(self):
        # testing the w_admin = admin QoS and w_user = user QoS:
        # (w_wtime, w_sld, w_user, w_bypass, w_admin, w_size)
        w_l = Weights(0, 0, 0, 0, 1, 0)
        w_b = Weights(0, 0, 1, 0, 0, 0)

        scheduler = MauiScheduler(NUM_PROCESSORS, weights_list = w_l, weights_backfill = w_b)
        simulator = run_simulator(scheduler=scheduler, num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/maui.admin_vs_user")
        feasibility_check_of_data(simulator.num_processors, simulator.jobs)
        feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
        for job in simulator.jobs:
            self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time)

    def test_maui_bypass_vs_slow_down(self):
        # testing the w_admin = admin QoS and w_user = user QoS:
        # (w_wtime, w_sld, w_user, w_bypass, w_admin, w_size)
        w_l = Weights(0, 0, 0, 1, 1, 0)
        w_b = Weights(0, 1.0, 0, 0, 0, 0)

        scheduler = MauiScheduler(NUM_PROCESSORS, weights_list = w_l, weights_backfill = w_b)
        simulator = run_simulator(scheduler=scheduler, num_processors=NUM_PROCESSORS, input_file = INPUT_FILE_DIR + "/maui.bypass_vs_sld")
        feasibility_check_of_data(simulator.num_processors, simulator.jobs)
        feasibility_check_of_cpu_snapshot(simulator.jobs, simulator.scheduler.cpu_snapshot)
        for job in simulator.jobs:
            self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_run_time)


###########





if __name__ == "__main__":
    try:
        import testoob
        testoob.main()
    except ImportError:
        import unittest
        unittest.main()
