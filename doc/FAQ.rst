FAQ
===

1. Local Testing?

Easily enough. In ``rhmr`` or ``rhlapply``, set ``mapred.job.tracker`` to
'local' in the ``mapred`` option of the respective command. This will
use the local jobtracker to run your commands. 

However keep in mind,
``shared.files`` will not work, i.e those files will not be copied to the
working directory and side effect files will not be copied back.


2. Speed?

Similar to Hadoop Streaming. The bottlenecks are writing and reading to STDIN
pipes and R.



3. What can RHIPE do?

Firstly, there are several R packages for parrallel computing. ``snow``,``snowfall``
are packages for (mostly) embarrrasingly parallel computation and do not work
with massive datasets. ``mapreduce`` implements the mapreduce algorithm on a
single machine(which can be done with RHIPE by using a cluster of size 1). 

RHIPE is a wrapper around Hadoop for the R user. So that he/she need not leave
the R environment for writing, running mapreduce applications and computing wth
massive datasets.

4. The command runner, different client and tasktrackers.

The object passed to rhex has variable called ``rhipe_command`` which is the
command of the program that Hadoop sends information to. In case the client
machine's (machine from which commands are being sent ) R installation is different from the
tasktrackers' R installation the RHIPE command runner wont be found. For example
suppose my cluster is linux and my client is OS X , then the ``rhipe_command``
variable will reflect the location of the rhipe command runner on OS X and not
that of the taskttrackers(Linux) R distribution. 

There are two ways to fix this 
a) after ``z <- rhmr(...)`` change ``r[[1]]$rhipe_command`` to the
value it should be on the tasktrackers.
(in case of ``rhlapply``, it should be ``r[[1]][[1]]$rhipe_command``)

or

b) set the environment variable ``RHIPECOMMAND`` on each of tasktrackers. RHIPE
java client will read this first before reading the above variable.
