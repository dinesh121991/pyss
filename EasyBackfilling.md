See also the ListOfPapers and [Backfilling](Backfilling.md), and especially [this paper](http://www.cs.huji.ac.il/~feit/papers/SP2backfil01TPDS.ps.gz), section 2.2, page 6.

# About #
EASY backfilling was developed by IBM and used in IBM's SP2 and LoadLeveler products.

# Description #
In EASY backfilling, the scheduler may [backfill](Backfilling.md) later jobs even if that delays the expected start time of other jobs, so long as the first job's expected start time isn't delayed.

# Pseudocode #
Taken straight from the [paper](http://www.cs.huji.ac.il/~feit/papers/SP2backfil01TPDS.ps.gz).

  1. Find the shadow time and extra nodes
    1. Sort the list of running jobs according to their expected termination time
    1. Loop over the list and collect nodes until the number of available nodes is sufficient for the first job in the queue
    1. The time at which this happens is the _shadow time_
    1. If at this time more nodes are available than needed by the first queued job, the ones left over are the extra nodes
  1. Find a backfill job
    1. Loop on the list of queued jobs in order of arrival
    1. For each one, check whether either of the following conditions hold:
      * It requires no more than the currently free nodes, and will terminate by the shadow time, or
      * It requires no more than the minimum of the currently free nodes and the extra nodes
    1. The first such job can be used for backfilling

This is executed repeatedly whenever a new job arrives or a running job terminates, if the first job in the queue cannot start. In each iteration, the algorithm identifies a job that can backfill if one exists.