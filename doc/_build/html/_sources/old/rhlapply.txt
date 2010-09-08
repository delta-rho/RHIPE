The ``rhlapply`` Command
========================

.. highlight:: r
   :linenothreshold: 5


Introduction
^^^^^^^^^^^^

``rhapply`` applies a user defined function to the elements of a given
R list or the function can be run over the set of numbers from 1 to
n. In the former case the list is written to a sequence file,whose length is the
default setting of ``rhwrite``. 

Running a hundreds of thousadands of seperate trials
can be terribly inefficient, instead consider grouping them, i.e set
``mapred.max.tasks`` to a value much smaller than the length of the
list.

Return Value
^^^^^^^^^^^^

``rhlapply`` returns a list, the names of which is equal to the names
of the input list (if given).

Function Usage
^^^^^^^^^^^^^^

::

	rhlapply <- function( ll=NULL,
        	             fun,
                 	     ifolder="",
			     ofolder="",
			     readIn=T,
			     inout=c('lapply','sequence')
			     mapred=list()
			     setup=NULL,jobname="rhlapply",doLocal=F,...
			     )


Description follows

``ll``
	The list object, optional. Applies ``fun`` to ``ll[[i]]`` . 
	If instead ``ll`` is a numeric, applies ``fun`` to each element of
	``seq(1,ll)``. If not given, must provide a value for ``ifolder``

``fun``
	A function that takes only one argument.

``ifolder``
	If ``ll`` is null, provide a source here. Also change the value of
	``inout[1]`` to either ``text`` or ``sequence``.

``readIn``
	The results are stored in a temporary sequence file on the DFS which is
	deleted. Should the results be returned in a list? Default is TRUE. For
	large number of output key-values (e.g 1MM) set this to FALSE, using the
	default options to ``rhread`` is extremely slow.

``ofolder``
	If given the results are written to this folder and not deleted. If not,
	they are written to temporary folder, read back in (assuming ``readIn``
	is TRUE) and deleted.

``N``
	The number of task to create, i.e the mapred.map.tasks and is passes onto the ``rhwrite`` function

``mapred``
	Options passed onto ``rhmr``

``setup``
	And expression that is called before running ``func``. Called once per
	JVM.

``aggr``
	A function (default is NULL) to aggregate results. If NULL (default), every list element is written to disk.
	This can be difficult to read back into R (especially when one has nearly 1MN trials, R has to combine a list
	of 1MN elements!). ``aggr`` is a function that takes one argument a list of values, each value being the result
	apply the user function to an element of the input list. E.g. if ``fun`` returns a data frame, one could write

::

	aggr=function(x) do.call("rbind",x)

	and the result of rhlapply will be one big data frame.

``doLocal``
	Default is ``F``. Sent to ``rhread``
``...`` 
	passed onto RHMR.


RETURN
++++++
	
An object that is passed onto ``rhex``.


IMPORTANT
+++++++++

The object passed to rhex has variable called ``rhipe_command`` which is the
command of the program that Hadoop sends information to. In case the client
machine's (machine from which commands are being sent ) R installation is different from the
tasktrackers' R installation the RHIPE command runner wont be found. For example
suppose my cluster is linux and my client is OS X , then the ``rhipe_command``
variable will reflect the location of the rhipe command runner on OS X and not
that of the taskttrackers(Linux) R distribution. 

There are two ways to fix this 
a) after ``z <- rhlapply(...)`` change ``r[[1]][[1]]$rhipe_command`` to the
value it should be on the tasktrackers.

or

b) set the environment variable ``RHIPECOMMAND`` on each of tasktrackers. RHIPE
java client will read this first before reading the above variable.






