The ``rhmr`` Command
====================

.. highlight:: r
   :linenothreshold: 5

Introduction
------------

The ``rhmr`` command runs a general mapreduce program using user supplied map
and reduce commands.

Return Value
------------
In general a set of files on the Hadoop Distributed File System. It can be of
Text Format or a Sequence file format. In case of the latter, the key and values
can be any R data structure.

Function
--------

::

	rhmr <- function(map,reduce=NULL,
                 combiner=F, 
                 setup=NULL,
                 cleanup=NULL,
                 ofolder='',
                 ifolder='',
                 inout=c("text","text"),
                 mapred=NULL,
                 shared=c(),
                 jarfiles=c(),
                 copyFiles=F,
		 partitioner=NULL,
                 opts=rhoptions(),jobname="")


``map``
	A map expression, not a function. The map expression can expect a list of keys in ``map.keys`` and list of values in ``map.values``. 
``reduce``
	Can be null if only a map job. If not,reduce should be an expression with three attributes

	``pre``
		Called for a new key, but no values have been read. The key is present in ``reduce.key``.
	``reduce`` 
		Called for reducing the incoming values. The values are in a list called ``reduce.values``
	``post``
		Called when all the values have been sent. 
``combiner``
	Uses a combiner if TRUE. If so, then ``reduce.values`` present in the ``reduce$reduce`` expression will be a *subset* of values.The reducer algorithm should be able process input emitted from map *or* reduce.
``setup``
	An expression that can be called to setup the environment. Called once for every task.
	It can be a list of two attributes ``map`` and ``reduce`` which are expressions to be run in the map and reduce stage. If a single expression then that is run for both map and reduce

``cleanup``
	Same as for ``setup``, run when all work for a task is complete.

``ifolder``
	A folder or file to be processed. Can be a vector of strings.

``ofolder``
	The folder to store output in. Side effects will be copied here.

``inout```
	A vector of input type and output type.
	 ``text`` 
	 	  indicates Text Format. Use ``mapred.field.separator`` to seperate the elements of a vector.

	``sequence`` 
		   is a sequence format. Outputs in this form /can/ be used as an input.
	``binary`` 
		   is a simple binary format consisting of key-length, key data, value-length, value data where the lengths are integers in network order. Though *much* faster than sequence in terms of reading in data, it *cannot* be used an input to a map reduce operation.
	``map``
		*Only as OutputFormat* ! That is, map can only be the second element of ``inout``. If so, the output part files will be directories, each containing a data and an index file. If the reducer writes the same key as the one received then using the function ``rhgetkey``, specifying the get and the output folder part files , one can use the output as a hash table (do keep the keys small then). However, if the keys are changed before being written (using rhcollect), the order is lost and even though one can still use the individual part file as a Map file reader, the part file containing the key needs to be known (as opposed to just specifying the directory of part files). To remedy this just run a identity map job converting map input to map output (see ``rhM2M`` and ``rhS2M``). 
		Map Output formats can be used an input format. Use the function ``rhmap.sq`` on a directory of map part files e.g ``rhmap.sq("/tmp/out/p*)``, this will return a vector of paths pointing to the *data* files in each of the part folders (the folders also contain index files, which can't be used as sequence file input to Hadoop, so these have to be filtered).

	``shared``
		A vector of files on the HDFS that will be copied to the working directory of the R program. These files can then be loaded as easily as ``load(filename)`` (removed leading path)

``jarfiles``
	Copy jar files if required. Experimental, probably doesn't work.

``copyFiles``
	For side effects to be copied back to the DFS, set this to TRUE, otherwise they wont be copied.

``mapred``
	Set Hadoop options here and RHIPE options. 

``partitioner``
	A list with two names: lims and type. type can be one of string, numeric, integer and lims must be positive. The vector lims is used as a partitioner, that is if ``c("a","b","c")`` is the key and ``lims=c(1,2)`` the first two elements will be used for partitioning. If a partitioner is used, all the emitted keys must of be the same type. If ``lims`` is of length 1, it will partitioned on that element.

``jobname``
	the jobname, if not given, then current date and time is the job title.

RHIPE Options
-------------

**rhipe_stream_buffer**
	The size of the STDIN buffer used to write data to the R process(in bytes)
	*default:* 10*1024 bytes
**mapred.textoutputformat.separator**
	The text that seperates the key from value when ``inout[2]`` equals text.
	*default:* Tab
**mapred.field.separator** 
	The text that seperates fields when ``inout[2]`` equals text.
	*default:* Space
**rhipe_reduce_buff_size**
	The maximum length of ``reduce.values``
	*default:* 10,000
**rhipe_map_buff_size**
	The maximum length of ``map.values`` (and ``map.keys``)
	*default:* 10,000
	    


Status, Counters and Writing Output
-----------------------------------

Status
^^^^^^
To update the status use ``rhstatus`` which takes a single string e.g ``rhstatus("Nice")``
This will also indicate progress.

Counter
^^^^^^^
To update the counter C in the group G with a number N, user ``rhcounter(G,C,N)``
where C and G are strings and N is a number. However, C and G can be atomic vectors and they will be converted to strings.
Previously a "," in C or G would upset Hadoop, but not with version 0.52 onwards.
The values will be returned to the R session.
Output
^^^^^^
To output data use ``rhcollect(KEY,VALUE)`` where KEY and VALUE are R objects that can be serialized by ``rhsz`` (see the misc page). If one needs to send across complex R objects e.g the KEY is a function, do something like ``rhcollect(serialize(KEY,NULL),VALUE)``


Side Effect files
-----------------
Files written to ``tmp/`` (no leading slash !) e.g ``pdf("tmp/x.pdf")`` will be copied to the output folder.


Mapreduce Options
-----------------
Many mapreduce configuration variables are stored in the environment. To get the value use ``Sys.getenv()``, e.g in the map stage, to find out the name of the current input file, use ``Sys.getenv('mapred.input.file')`` .

IMPORTANT
---------

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

or

b) set the environment variable ``RHIPECOMMAND`` on each of tasktrackers. RHIPE
java client will read this first before reading the above variable.
