Miscellaneous Commands
======================

.. highlight:: r
   :linenothreshold: 5


Introduction
------------

This is a list of supporting functions for reading, writing sequence files and
manipulating files on the Hadoop Distributed File System (HDFS).

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
	
	rhls <- function(dir)

Lists the path at ``dir``. Wildcards allowed.


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
	
	rhread <- function(files,verbose=T,doLocal=T)


Reads files(s) from ``files`` (which could be a directory). Wildcards allowed.

If ``verbose`` is True, information is displayed (useful when reading many
files)
``rhread`` read sequence files by running a mapreduce	job to convert the
sequence file to a binary file. 
This is then merged and read into R. If ``doLocal`` is True this mapreduce
conversion job is a local mapreduce job (which can be slow for lots of part
files) else a fully distributed job.

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

