Miscellaneous Commands
======================

.. highlight:: r
   :linenothreshold: 5


Introduction
------------

This is a list of supporting functions for reading, writing sequence files and
manipulating files on the Hadoop Distributed File System (HDFS).

Running Mapreduce
-----------------

rhex
^^^^
Once an object is created using ``rhmr`` and ``rhlapply``, it must be sent to
the Hadoop system. The function ``rhex`` does this

::

	rhex <- function(o, async=FALSE, mapred)

Where ``o`` is the object that ``rhmr`` or ``rhlapply`` returns. ``mapred`` is a
list of the same shape as in ``rhmr`` and ``rhlapply``. Values in this over-ride
those passed in ``rhmr``(and ``rhlapply``). If ``async`` is FALSE, the function
returns when the job has finished running. The value returned is a list of
Hadoop Counters (e.g bytes sent, bytes written, time taken etc).

If ``async`` is TRUE, the function returns immediately. In this case, the value
returned can be printed (i.e just type the returned value at the REPL) or passed
to ``rhstatus`` to monitor the job. 


rhjoin
^^^^^^

::

	rhjoin <- function(o,ignore.stderr=TRUE)

where ``o`` is returned from ``rhex`` with async=TRUE. The function returns when
the job is complete and the return value is the same as ``rhex`` when ``async``
is FALSE (i.e counters and the result(failure/success) of the job). If
``ignore.stderr`` is FALSE, the progress is displayed on the screen(exactly like
``rhex``).

rhstatus
^^^^^^^^

::

	rhstatus <- function(o)

where ``o`` is returned from ``rhex`` with async=TRUE (or a
Hadoop job id (e.g "job_20091031_0001"). This will return list of counters and
the progress status of the job(number of maps complete, % map complete etc).

print
^^^^^

This a generic function for printing objects returned from ``rhex`` when
async=TRUE. The default returns start time, job name and job id, and job state,
map/reduce progress. For more verbosity, type ``print(o,verbose=2)`` which
returns a list of counters too (like ``rhstatus``).


Serialization
-------------

rhsz
^^^^
::

	rhsz <- function(object)

Serializes a given R object. Currently the only objects that can be serialized
are vectors of Raws,Numerics, Integers, Strings(including NA), Logical(including NA)
and lists of these and lists of lists of these. Attributes are copied to(e.g
names attributes). It appears objects like matrices, factors also get serialized
and unserialized sucessfully.

rhuz
^^^^
::

	rhuz <- function(object)

Unserializes a raw object returned from ``rhsz``

HDFS Related
------------
rhload
^^^^^^
::
	
	rhload <- function(file,...)


Loads an R data set stored on the DFS.


rhsave
^^^^^^
::
	
	rhsave <- function(..., file)

Saves the objects in ``...`` to ``file`` on the HDFS. All other options are
passed onto the R function ``save``


rhsave.image
^^^^^^^^^^^^
::
	
	rhsave.image <- function(..., file)

Same as R's ``save.image``, except that the file goes to the HDFS.

rhcp
^^^^^
::
	
	rhcp <- function(ifile,ofile)

Copies a ``ifile`` to ``ofile`` on the HDFS, i.e. both files must be present on the HDFS.

rhmv
^^^^^
::
	
	rhmv <- function(ifile,ofile)

Moves ``ifile`` to ``ofile`` on the HDFS (and deletes ``ifile``).
 

rhput
^^^^^
::
	
	rhput <- function(src,dest,deleteDest=TRUE)

Copies the file in ``src`` to the ``dest`` on the HDFS, deleting destination if
``deleteDest`` is TRUE.


rhget
^^^^^

::
	
	rhget <- function(src,dest)

Copies ``src``(on the HDFS) to ``dest`` on the local. If ``src`` is a directory and ``dest`` exists,
``src`` is copied inside ``dest``(i.e a folder inside ``dest``).If not(i.e
``dest`` does not exist), ``src``'s contents is copied to a new folder called
``dest``.  If ``src`` is a file, and ``dest`` is a directory ``src`` is copied
inside ``dest`` . If ``dest`` does not exist, it is copied to that file

Wildcards allowed


OVERWRITES!

rhls
^^^^
::
	
	rhls <- function(dir,recur=FALSE)

Lists the path at ``dir``. Wildcards allowed. Use ``recur`` (FALSE/TRUE) to not recurse or to recurse.


rhdel
^^^^^
::
	
	rhdel <- function(dir)

Deletes file(s) at/in ``dir``. Wildcards allowed.



rhwrite
^^^^^^^
::
	
	rhwrite <- function(lo,f,n=NULL,...)

Writes the list ``lo``  to the file ``f``. ``n`` is the number of sequence files
to split the list into.  The default value of ``n`` is 
``mapred.map.tasks`` * ``mapred.tasktracker.map.tasks.maximum`` .



rhread
^^^^^^

::
	
	rhread <- function(files,max=-1,type="sequence",verbose=T,mc=FALSE)


Reads files(s) from ``files`` (which could be a directory). Wildcards allowed.

If ``verbose`` is True, information is displayed (useful when reading many
files)

If ``max`` is positive, ``max`` key-value pairs will be read.

Set ``type`` to "map" if the directory ``files`` contains map folders.

Provided you have the ``multicore`` package, set ``mc`` to TRUE and the deserialization will occur in parallel.
You have to load ``multicore`` beforehand.

rhmerge
^^^^^^^

::

	rhmerge(inr,ou)


``inr`` can have wildcards. Usually used to merge all files in a directory into one file ``ou`` on the local file system.


.. rhreadText
.. ^^^^^^^^^^

.. ::
	
.. 	rhreadText <- function(filename)

.. Currently when outputting to text because of a bug in the code I've been forced
.. to write serialized bytes in text form. To parse such a file, copy it to the
.. local filesystem and use this function on the filename.

.. You might as well use binary output format.


rhreadBin
^^^^^^^^^

::

	rhreadBin <- function(filename, max=as.integer(-1), bf=as.integer(0))


Reads data outputed in 'binary' form. ``max`` is the maximum number to read, -1
is all. ``bf`` is the read buffer, 0 implies the os specified default ``BUFSIZ``


Map Files
---------

rhS2M
^^^^^

::	

	rhS2M <- function (files, ofile, dolocal = T, ignore.stderr = F, verbose = F) 


Converts the sequence files specified by ``files`` and places them in
destination ``ofile``. If ``dolocal`` is True the conversion is done on the
local machine, otherwise over the cluster (which is much faster for anything
greater than hundreds of megabytes). If ``ignore.stderr`` is True, the mapreduce
output is displayed on the R console. e.g

::

	rhS2m("/tmp/so/p*","/tmp/so.map",dolocal=F)


rhM2M
^^^^^

::	

	rhM2M <- function (files, ofile, dolocal = T, ignore.stderr = F, verbose = F) 


Same as S2M, except it converts a group of Map files to Map files.Why? 
Consider a mapreduce job that outputs modified keys in the reduce part, i.e the
reduce receives key K0 but emits f(K0), where f(K0) <> K0, the result of this
the keys in the reduce output part files wont be sorted even though the K0 are
sorted.

So, if the reducer emits K0, the output part files constitute a valid collection
of sorted map files. If the reducer emits f(K0), this does not hold any
more. Running ``rhM2M`` on this output produces another output in which the keys
are now sorted (i.e we just run an identity mapreduce emitting f(K0), though now
the input to the reducers are f(K0)).

To specify the input files, it is not enough to specify the directory
containing the part files, because the part files are directories which contain
a sequence file and a non sequence file. Specifying the list of directories to a
mapreduce job will cause it to fail when it reads the non-map file.

Use ``rhmap.sqs`` .



rhgetkey
^^^^^^^^

::
	
	rhgetkey <- function (keys, paths, sequence=NULL,skip=0,ignore.stderr = T, verbose = F) 

Given a list of keys and vector of  map directories (e.g /tmp/ou/mapoutput/p*"),
returns a list of key,values. If sequence is a string, the output key,values will be written to the sequence files on the DFS(the values will not be read into R).
Set skip to larger(integr) values to prevent reading in all keys of the table - slower to find your key, but can search a much large database.
