#!/usr/bin/env python2.4

import unittest
import sim
import sim2


sim.CpuTimeSlice.total_nodes = 100
total_nodes = 100

class test_CpuTimeSlice(unittest.TestCase):
    def test_initialization(self):
        timeslice = sim.CpuTimeSlice()
        self.assertEqual(timeslice.total_nodes, timeslice.free_nodes)

    def test_addJob_free_nodes_simple(self):
        timeslice = sim.CpuTimeSlice()
        timeslice.addJob( sim.Job(0, 1, job_nodes=1) )
        self.assertEqual(timeslice.total_nodes-1, timeslice.free_nodes)

class test_Job(unittest.TestCase):
    def test_init_zero_nodes(self):
        self.assertRaises(AssertionError, sim.Job, 0, 0, job_nodes=0)


class test_FifoScheduler(unittest.TestCase):

    def test_simple_not_interesecting_jobs_scenario(self):

        
        j_id = "j1"; j_duration = 100; j_actual_duration = 10; j_nodes = 10; j_arrival_time = 0;  
        job1 = Job(j_id, j_duration, j_nodes, j_arrival_time, j_actual_duration)

        j_id = "j2"; j_duration = 100; j_actual_duration = 10; j_nodes = 10; j_arrival_time = 1000;  
        job2 = Job(j_id, j_duration, j_nodes, j_arrival_time, j_actual_duration)

        self.scheduler = FifoScheduler(total_nodes)
        self.scheduler.handleArrivalOfJobEvent(job1, 0)
        self.scheduler.handleArrivalOfJobEvent(job1, 10)
        self.scheduler.handleArrivalOfJobEvent(job2, 1000)
        self.scheduler.handleArrivalOfJobEvent(job2, 1010)
        self.assertEqual(job1.id, 
        
        
        
        
        
     

if __name__ == "__main__":
    unittest.main()






