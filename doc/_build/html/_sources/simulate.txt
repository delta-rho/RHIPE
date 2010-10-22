.. highlight:: r
   :linenothreshold: 5

.. index:: simulations, rhlapply, lapply

***********
Simulations
***********


Simulations are an example of task parallel routines in which a function is
called repeatedly with varying parameters. These computations are processor
intensive and consume/produce little data. The evaluation of these tasks are
independent in that there is no communication between them.  With :math:`N` tasks and
:math:`P` processors, if :math:`P=N` we could run all :math:`N` in parallel and collect the
results. However, often :math:`P << N` and thus we must either

- Create a queue of tasks and assign the top most task on the queue to the
  next free processor. This works very well in an heterogeneous environment
  e.g. with varying processor capacities or varying task characteristics - free
  resources will be automatically assigned pending tasks. The cost in creating a
  new task can be much greater than the cost of evaluating the task.

- Partition the :math:`N` tasks into  :math:`n` splits each containing :math:`\lceil N/n
  \rceil` tasks (with the last split containing the remainder).  These splits
  are placed in a queue, each processor is assigned a splits and the tasks in
  a split are evaluated sequentially.


The second approach simplifies to the first when :math:`n=N`. Creating one split per
task is inefficient since the time to create,assign launch the task contained in
a split might be much greater than the evaluation of the task.  Moreover
with :math:`N` in the millions, this will cause the Jobtracker to run out of
memory. It is recommended to divide the :math:`N` tasks into fewer splits of
sequential tasks. Because of non uniform running times among
tasks, processors can spend  time in the sequential execution of tasks in a split
:math:`\sigma` with other processors idle. Hadoop will schedule the split
:math:`\sigma` to another processor (however it will not divide the split into smaller
splits), and the output of whichever completes first will be used.

RHIPE provides two approaches to this sort of computation.  To apply the
function :math:`F` to the set :math:`\{1,2,\ldots, M\}`, the pseudo code would follow as
(here we assume :math:`F` returns a data frame)


::

  FC <- expression({
    results <- do.call("rbind",lapply(map.values,F))
    rhcollect(1,results)
  })
  
  rhmr(map=FC,ofolder='tempfolder',inout=c('lapply','sequence'),N=M
       ,mapred=list(mapred.map.tasks=1000))
  
  do.call('rbind',lapply(rhread('/tempfolder', mc=TRUE),'[[',2))


Here :math:`F` is applied to the numbers :math:`1,2,\ldots,M`.  The job is decomposed into
1000 splits (specified by ``mapred.map.tasks``) each containing approximately
:math:`\lceil M/1000 \rceil` tasks. The expression, :math:`FC` sequentially applies :math:`F` to
the elements of ``map.values`` (which will contain a subset of :math:`1,2,\ldots,M`)
and aggregate the returned data frames with a call to ``rbind``.  In the last
line, the results of the 1000 tasks (which is a list of data frames) are read
from the HDFS, the data frame are extracted from the list and combined using a
call to ``rbind``. Much of this is boiler plate RHIPE code and the only
varying portions are: the function :math:`F`, the number of iterations :math:`M`, the number
of groups (e.g. ``mapred.map.tasks``) and the aggregation scheme (e.g. I used
the call to ``rbind``).  R lists can be written to a file on the HDFS(with
``rhwrite``), which can be used as i input to a MapReduce job .  All of this
could then be wrapped in a single function:

::
	
	rhipe.lapply(function, input, groups=number.of.cores, aggregate)

where ``function`` is :math:`F`, ``input`` could be a list or maximum trials
(e.g. :math:`M`). The parameter ``groups`` is the number of groups to divide the
job into and by default is the number of cluster cores and ``aggregate`` is a
function to aggregate the intermediate results. With this function, the user can
distribute the ``lapply`` command and rely on Hadoop to handle fault-tolerancy
and the scheduling of processors in an optimal fashion. The ``rhlapply``
function is present to do this.

.. index:: rhlapply

::

	rhlapply(ll, F, ofolder,setup=NULL,readIn = TRUE, N, aggr=NULL,...)

This applies ``F`` to the elements of ``ll``. If provided a value, it will save
the results to ``ofolder`` and the results are returned as a list if ``readIn``
is TRUE. The value of ``N`` is passed to ``rhwrite`` (if ``ll`` is a list, they
will be written to a temporary file). ``setup`` can be used to load files. The
``rhllapply`` command takes the arguments of ``rhmr`` (e.g. ``mapred``) and they
passed to ``rhmr``.
 
.. index:: random number generation, mapred.task.id

A Note on Random Number Generators
----------------------------------

RHIPE does not include parallel random generator e.g. Scalable Parallel Random
Number Generators Library and the Rstreams package for R
([ecuyer]_ and [Masac]_). Parallel RNGs can create streams of random numbers that
are not correlated across cluster computers (i.e enforce 'statistical
independence') and ensure reproducibility of streams for research.  RHIPE can
guarantee independent streams since each task has a unique identifier obtained
from the environment variable *mapred.task.id*. Since the identifier is unique
for every task it can be used to seed random number generators. This cannot be
used for reproducible results. There is ongoing work to integrate parallel
random generator packages for R with RHIPE.

.. [ecuyer] rstream: Streams of Random Numbers for Stochastic Simulation,Pierre L'Ecuyer and Josef Leydold, `<http://cran.r-project.org/web/packages/rstream/index.html>`_

.. [Masac] Algorithm 806: SPRNG: A Scalable Library for Pseudorandom Number Generation, M. Mascagni and A. Srinivasan, *ACM Transactions on Mathematical Software*, pages 436-461,volume 26, 2000

