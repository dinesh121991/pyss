#!/usr/bin/env python2.4

import unittest
import sim
import sim2


sim.CpuTimeSlice.total_nodes = 100

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


class test_




if __name__ == "__main__":
    unittest.main()






