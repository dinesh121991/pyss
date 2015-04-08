See also the ListOfPapers and [Backfilling](Backfilling.md).

# Description #
In conservative backfilling, the scheduler will run jobs in their order of arrival (FIFO), and at any given moment will allow jobs to be [backfilled](Backfilling.md) only if running them now will not delay the current latest start time of _**any**_ other job in the queue.

# Design #
The current design is based on Ahuva's CPU Time Slice design. See the code (see classes `CPUTimeSlice` and  `CPUSnapshot` in [common.py](http://pyss.googlecode.com/svn/trunk/src/schedulers/common.py), and `ConservativeScheduler` in [conservative\_scheduler.py](http://pyss.googlecode.com/svn/trunk/src/schedulers/conservative_scheduler.py), repository [revision 763](https://code.google.com/p/pyss/source/detail?r=763)).

We are currently exploring a simpler design based on the [insights](SchedulerDesignInsights.md) gained with the EasyBackfilling scheduler design.