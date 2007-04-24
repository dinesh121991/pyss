#!/usr/bin/env python2.4

import unittest 
from sim2 import * 


class test_Simulator(unittest.TestCase):

    def test_basic_fcfs(self):
        for i in range(24): 
            simulator = Simulator(scheduler ="Fcfs", input_file = "./Input_test_files/basic_input." + str(i))
            for job in simulator.jobs:
                # The job id in these test input files signifies its correct finishing time
                # we use this idea to enable complex testing scenarios without expanding the code 
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)


    def test_basic_conservative(self):
        for i in range(24): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_test_files/basic_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
    

    def test_basic_easyBackfill(self):
        for i in range(24): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_test_files/basic_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
    

    def test_fcfs(self):
        for i in range(8): 
            simulator = Simulator(scheduler ="Fcfs", input_file = "./Input_test_files/fcfs_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)



    def test_conservative(self):
        for i in range(9): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_test_files/bf_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)

        for i in range(2): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_test_files/cons_bf_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
        
    
    
    def test_basic_easyBackfill(self):
        for i in range(9): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_test_files/bf_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)

        for i in range(1): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_test_files/easy_bf_input." + str(i))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
                



if __name__ == "__main__":
    unittest.main()






