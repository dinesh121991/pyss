import unittest
import simulator_prototype_01 as prototype

class test_cluster(unittest.TestCase):
    NUM_PROCESSORS = 700
    def setUp(self):
        self.cluster = prototype.Cluster( num_processors = self.NUM_PROCESSORS )

    def test_initial_idle_processors(self):
        self.assertEqual(self.NUM_PROCESSORS, len(self.cluster.idle_processors))

    def test_bla(self):
        self.cluster.run_job(job = prototype.Job())
        self.assertEqual
