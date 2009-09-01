The ``rhlapply`` Command
========================

.. highlight:: r
   :linenothreshold: 5


Introduction
^^^^^^^^^^^^

``rhapply`` applies a user defined function to the elements of a given
R list or the function can be run over the set of numbers from 1 to
n. In the former case the list is written to a sequence file, the
default is to write *one sequence file* per item, which can be grossly
inefficient. This can be controlled by setting ``mapred.max.tasks`` in
the ``hadoop.options`` paramater of ``rhlapply``. 

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

	rhlapply <- function( list.object,
        	             func,
                 	     configure=expression(),
                    	     output.folder='',
			     shared.files=c(),
			     hadoop.mapreduce=list(),
			     verbose=T,
			     takeAll=T)




Description of the options follows.

``list.object``
	Either an R list or a scalar number, N. In the former case, the
	user function gets the value. In the latter, the function gets
	the iteration number (from 1 to N)
``func``
	A user supplied R function, which can return any R object.
``configure``
	An expression which can be used to setup the environment. Run
	once for every *task*. So if the user has 100K tasks, it is run
	100K times.
``output.folder``
	Save the results in an output folder, not required, but this
	output folder can be used ``rhmr`` or to ``rhlapply``

``shared.files``
	A character vector of files located on the HDFS. The files are
	copied to a working folder on the cluster machines, thus
	speeding up reads. To load the filename, use the trailing part
	of the filename, e.g
::

	rhlapply(...,shared.files=c('/tmp/file1.Rdata','/user/me/obj.Rdata'))
	
	
Then e.g. in R,

::
	
	configure <- expression({
		load('obj.Rdata')
		load('file1.Rdata')
		})

	
Note, the files are copied to R's working directory.

``hadoop.mapreduce``
	A list, keys are hadoop options. Use this to set all of the
	Hadoop options.
``verbose``
	If True, the progress of the job appears in the R terminal,
	else a web url is provided for the user to inspect. Also in
	the latter, control returns immediately to the user
``takeAll``
	Get back all the results from the temporary output file.


