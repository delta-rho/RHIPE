.. highlight:: r
   :linenothreshold: 5

*****************
 RHIPE Functions
*****************

RHIPE has functions that access the HDFS from R, that are used inside MapReduce
jobs and functions for managing MapReduce jobs.

HDFS Related
============
.. index:: rhdel

rhdel - File Deletion
---------------------
::
	
	rhdel(folders)

This function deletes the folders contained in the character vector ``folders``
which are located on the HDFS. The deletion is recursive, so all subfolders will
be deleted too. Nothing is returned.

.. index:: rhls

rhls - Listing Files
--------------------
::

	rhls(path, recurse=FALSE)

Returns a data frame of filesystem information for the files located at ``path``. If
``recurse`` is TRUE, ``rhls`` will recursively travel the directory tree
rooted at ``path``. The returned object is a data frame consisting of the
columns: *permission, owner, group, size (which is numeric), modification time*,
and the *file name*. ``path`` may optionally **end** in '*' which is the
wildcard and will match any character(s).

.. index:: rhget

rhget - Copying from the HDFS
-----------------------------
::

	rhget(src,dest)


Copies the files (or folder) at ``src``, located on the HDFS to the
destination ``dest`` located on the local filesystem. If a file or folder of
the same name as ``dest`` exists on the local filesystem, it will be deleted.

.. index:: rhput

rhput - Copying to the HDF
--------------------------
::
	
	rhput(src,dest)

Copies the local file called ``src`` (not a folder) to the destination ``dest``
on the HDFS.

.. index:: rhcp

rhcp - Copying on the HDFS
--------------------------
::
	
	rhcp(src,dest)

Copies the file (or folder) ``src`` on the HDFS to the destination ``dest``
also on the HDFS.

.. index:: rhwrite

rhwrite - Writing R data to the HDFS
------------------------------------
::

	rhwrite(list,dest,N=NULL)

Takes a list of objects, found in ``list`` and writes them to the folder pointed
to by ``dest`` which will be located on the HDFS. The file ``dest`` will be in a
format interpretable by RHIPE, i.e it can be used as input to a MapReduce job.
The values of the list of are written as key-value pairs in a SequenceFileFormat
format. ``N`` specifies the number of files to write the values to. For example,
if ``N`` is 1, the entire list ``list`` will be written to one file in the
folder ``dest``. Computations across small files do not parallelize well on
Hadoop. If the file is small, it will be treated as one split and the user does
not gain any (hoped for) parallelization. Distinct files are treated as distinct
splits. It is better to split objects across a number of files. If the list
consists of a million objects, it is prudent to split them across a few
files. Thus if :math:`N` is 10 and ``list`` contains 1,000,000
values, each of the 10 files (located in the directory ``dest``) will contain
100,000 values.

Since the list only contains values, the keys are the indices of the
value in the list, stored as strings. Thus when used as a source for a MapReduce
job, the variable ``map.keys`` will contain numbers in the range :math:`[1,
length(list)]`. The variable ``map.values`` will contain elements of
``list``. 


.. index:: rhread, sequencefile, mapfile

rhread - Reading data from HDFS into R
--------------------------------------
::

	rhread(files,type="sequence",max=-1,mc=FALSE,buffsize=2*1024*1024)

Reads the key,value pairs from the files pointed to by ``files``. The source
``files`` can end in a wildcard (*) e.g. */path/input/p** will read all the
key,value pairs contained in files starting with *p* in the folder
*/path/input/*.  The parameter ``type`` specifies the format of ``files``. This
can be one of ``text``, ``map`` or ``sequence`` which imply a Text file, MapFile or a
SequenceFile respectively. For text files, RHIPE returns a matrix of lines, each row a line from the text files.
Specifying ``max`` for text files, limits the number of bytes read and is currently alpha quality.
 Thus data written by ``rhwrite`` can be read
using ``rhread``. The parameter ``max`` specifies the maximum number of entries
to read, by default all the key,value pairs will be read. Setting ``mc`` to TRUE
will use the the ``multicore`` [multicore]_ package to convert the data to R
objects in parallel. The user must have first loaded ``multicore`` via call to
library. This often does accelerate the process of reading data into R.

.. [multicore]  `<http://http://cran.r-project.org/web/packages/multicore/index.html>`_

.. index:: rhgetkey, mapfile, orderby, rhmr, 

.. index:: sequencefile;convert seqeuncefile to mapfile

rhgetkeys - Reading Values from Map Files
-----------------------------------------
::

	rhgetkey(keys, path)

Returns the values from the map files contained in ``path`` corresponding to the
keys in ``keys``. ``path`` will contain folders which is MapFiles are
stored. Thus the ``path`` must have been created as the output of a RHIPE job
with ``inout[2]`` (the output format) set to *map*. Also, the saved keys must be in sorted order. This is always the case if

1. *mapred.reduce.tasks* is not zero.
2. The variable *reduce.key* is not modified.
3. ``orderby`` is not the default (*bytes*) in the call to ``rhmr``

A simple way to convert any RHIPE SequenceFile data set  to MapFile is to run an identity MapReduce

::

  map <- expression({
    lapply(seq_along(map.values),function(i)
      rhcollect(map.keys[[i]],map.values[[i]]))
  })
  rhmr(map=map,ifolder,ofolder,inout=c("sequence","map"))

The ``keys`` argument is a list of the keys. Keys are R objects and are characterized by their attributes too. So

::
   
   > identical(c(x=1),c(1))
   FALSE
  
If the stored key is ``c(x=1)`` then this call to ``rhgetkey`` will not work

::

   rhgetkey(list(c(1)),path)

but this will

::

   rhgetkey(list(c(x=1)),path)


MapReduce Administration
========================

.. index:: rhex, rhmr, rhstatus, rhjoin, rhkill

rhex - Submitting a MapReduce R Object to Hadoop
------------------------------------------------
::
	
	rhex(mrobj, async=FALSE,mapred)

Submits a MapReduce job (created using ``rhmr``) to the Hadoop MapReduce
framework. The argument ``mapred`` serves the same purpose as the ``mapred``
argument to ``rhmr``. This will override the settings in the object returned
from ``rhmr``.  The function returns when the job ends (success/failure or
because the user terminated (see ``rhkill``)). When ``async`` is TRUE, the
function returns immediately, leaving the job running in the background on Hadoop. 

The function returns an object of class *jobtoken*. The generic function
``print.jobtoken``, displays the start time, duration (in seconds) and percent
progress. This object can be used in calls to ``rhstatus``,``rhjoin`` and ``rhkill``.


.. index:: rhstatus, rhcounter

rhstatus - Monitoring a MapReduce Job
-------------------------------------
::

	rhstatus(jobid)

This returns the status of an running MapReduce job. The parameter ``jobid`` can
either be a string with the format *job_datetime_id*
(e.g. *job_201007281701_0274*) or the value returned from ``rhex`` with the
``async`` option set to TRUE.  

A list of 4 elements: 

- the state of the job (one of *START, RUNNING, FAIL,COMPLETE*), 

- the duration in seconds, 

- a data frame with columns for the Map and Reduce phase. This data frame summarizes the number of tasks, the percent complete, and the number of tasks that are pending, running, complete or have failed.

- In addition the list has an element that consists of both user defined and Hadoop MapReduce built in counters (counters can be user defined with a call to ``rhcounter``).

.. index:: rhjoin, rhex

rhjoin - Waiting on Completion of a MapReduce Job
-------------------------------------------------
::
	
	rhjoin(jobid, ignore=TRUE)

Calling this functions pauses the R console till the MapReduce job indicated by
``jobid`` is over (successfully or not). The parameter ``jobid`` can either be
string with the format *job_datetime_id* or the value returned from ``rhex``
with the ``async`` option set to TRUE. This function returns the same object as
``rhex`` i.e a list of the results of the job (TRUE or FALSE indicating success
or failure) and a counters returned by the job. If ``ignore`` is FALSE, the
progress will be displayed on the R console (much like ``rhex``)

.. index:: rhkill

rhkill - Stopping a MapReduce Job
---------------------------------
::
	
	rhkill(jobid)

This kills the MapReduce job with job identifier given by ``jobid``. The
parameter ``jobid`` can either be string with the format *job_datetime_id* or
the value returned from  ``rhex`` with the ``async`` option set to
TRUE.
