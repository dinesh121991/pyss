#!/usr/bin/env python2.4

import unittest 
from sim2 import * 


class test_Simulator(unittest.TestCase):

    def test_basic_fcfs(self):
        for num in range(23): 
            simulator = Simulator(scheduler ="Fcfs", input_file = "./Input_files/basic_input." + str(num))
            for job in simulator.jobs:
                # The job id in these test input files signifies its correct finishing time
                # we use this idea to enable complex testing scenarios without expanding the code 
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)


    def test_basic_conservative(self):
        for num in range(23): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_files/basic_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
    

    def test_basic_easyBackfill(self):
        for num in range(23): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_files/basic_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
    

    def test_fcfs(self):
        for num in range(8): 
            simulator = Simulator(scheduler ="Fcfs", input_file = "./Input_files/fcfs_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)



    def test_conservative(self):
        for num in range(9): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_files/bf_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)

        for num in range(2): 
            simulator = Simulator(scheduler ="Conservative", input_file = "./Input_files/cons_bf_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
        
    
    
    def test_basic_easyBackfill(self):
        for num in range(9): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_files/bf_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)

        for num in range(1): 
            simulator = Simulator(scheduler ="EasyBackfill", input_file = "./Input_files/easy_bf_input." + str(num))
            for job in simulator.jobs:
                self.assertEqual(int(float(job.id)), job.start_to_run_at_time + job.actual_duration)
                



if __name__ == "__main__":
    unittest.main()






