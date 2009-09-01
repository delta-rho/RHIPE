Miscellaneous Commands
======================

.. highlight:: r
   :linenothreshold: 5


Introduction
------------

This is a list of supporting functions for reading, writing sequence files and
manipulating files on the Hadoop Distributed File System (HDFS).


HDFS Related
------------
rhget
^^^^^
::

	rhget<- function(from,to,...)

Copies ``from`` from the DFS to ``to`` on the local filesystem. This uses the
system command and extra parameters are sent to the ``system`` command.


rhput
^^^^^
::

	rhput<- function(from,to,...)


The reverse of ``rhget``.


rhrm
^^^^

::

	rhrm <- function(w,gp=NA,...)

Deletes the file ``w`` from the HDFS. Regular expressions can be given in
``gp``, if so ``w`` should be a folder. For example:: 

	rhrm("/tmp",gp="partfile$")

deletes all files in ``/tmp/`` that end in *partfile*


rhls
^^^^

::

	rhls <- function(w="/")

Lists the files in the directory ``w``.

rhlsd
^^^^^
::

	rhlsd <- function(fold,....)


Returns a *data frame* of files in folder ``fold``. Wildcards are allowed in
``fold``.

Hadoop Job Related
------------------
These are related to the killing of jobs.

rhkill
^^^^^^
::

	rhkill <- function(w)

Kills the job with job id equal to ``ww``. The job title can be either of
*job2009051815100003* or *2009051815100003* (i.e same as the former with the job
prefix removed).

Saving R Datasets
-----------------

rhsave.image, rhsave, rhload
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

::

	rhsave.image <- function(file,...)
	rhsave <- function(file,....)
	rhload(file, envir=NULL,...)


Similar to ``save.image``, ``save`` and ``load`` except that it saves(or reads)
``file`` to(or from) the HDFS.

Creating Sequence FIles
-----------------------

The following are useful for creating and reading sequence files created while
using RHIPE.

rhnewSqfile
^^^^^^^^^^^

::
	
	rhnewSqfile <- function(l,name,byrow=T,f=NA,pct=0.1)

Takes an object and writes its rows(columns if ``byrow`` is FALSE and ``l`` is a matrix/data frame) or its elements if ``l`` is a list or a scalar vector to a file on the DFS given by name. Uses default compression codec.

Displays progress output at ``pct`` intervals. If not NA, ``rhnewSqfile`` calls ``f`` on every element, so this can be used to pass a character vector corresponding to the objects to be written.

The keys correspond to row names(or column names) or names of the object. 

rhsqallKV
^^^^^^^^^

::
	
	rhsqallKV <- function(ford,n,verbose=F,ignore=T,local=F,...)

Reads *all* key(or a maximum of ``n``),values from the sequence file
``ford``. Also, if ``ford`` is a folder of sequence files, one 
can use wildcards as an appropiate value (e.g ``tmp/p*``)

If ``verbose`` is TRUE, the current file being read is displayed. 

Returns a list of values with names corresponding to the keys.

Low Level Functions
-------------------

The above two sequence files functions are written using the following functions

rhsqreader
^^^^^^^^^^

::

	rhsqreader <- function(ford,local=F,...)

``ford`` is the path to a sequence file or folder containing sequence
files.

 Wildcards are allowed(if ``local`` is FALSE), so to process all part-xxx
files in a folder use ``rhsqreader("/x/p*")``. 

To obtain wildcard functionality, use a pattern. For example,
``rhsqreader("/x",local=T,pattern="^p")``. is equivalent to the previous call,
assuming ``/x`` exists on the local filesystem.This function will not go into
subdirectories.

 This function creates an object to read key/value pairs from the
set of sequence files. After calling this, call ``rhsqnextKVR`` to retrieve
key-value pairs. 

rhsqnextKVR
^^^^^^^^^^^
::

	rhsqnextKVR <- function(sqro)

Returns a list with two entries key and value. If none available, returns
NULL. ``sqro`` is the object returned by ``rhsqreader``.

rhsqnextpath
^^^^^^^^^^^^
::

	rhsqnextpath <- function(sqro)


When ``rhsqreader`` is used to read key-values from a folder of sequence files,
this function switches to the next file in the folder.

.. note:: This is used internally by ``rhsqreader`` and ``rhsqnextKVR``, the user does not need to use this.

rhsqclose
^^^^^^^^^
::

	rhsqclose <- function(sqro)

Closes an sqreader object. ``sqro`` is a sqreader object. Used internally by
other functions. 

Example
-------

Reading
^^^^^^^
Two equivalent ways to read all key value pairs in folder of sequence files.

**Long Way**

::

	rdr <- rhsqreader("/test/one/p*")
	while(TRUE){
	    value <-rhsqnextKVR(rdr)
	    if(is.null(value)) {
	        rdr <- rhsqnextpath(rdr)
	      if(is.null(rdr)) break;
	    }
	    print(value$key)
	   print(value$value)
	}


**Short Way**

::
	
	info<-rhsqallKV("/test/one/p*",ignore=F)

Writing
^^^^^^^

::
	
	x <- matrix(runif(1,2,3,4,5,6),ncol=2)
	rownames(x) <- c('a','b','c','d','e','f')
	rhnewSqfile(x,byrow=T,name="testfile")


