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

	rhmr <- function(map,reduce,input.folder,configure=list(map=expression(),reduce=expression()),
                close=list(map=expression(),reduce=expression())
                 output.folder='',combiner=F,step=F,
                 shared.files=c(),inputformat="TextInputFormat",
                 outputformat="TextOutputFormat",
                 hadoop.mapreduce=list(),verbose=T,libjars=c())



input.folder
    A folder on the DFS containing the files to process. Can be a vector. 

output.folder
    A folder on the DFS where output will go to. 

inputformat
    Either of ``TextInputFormat`` or ``SequenceFileInputFormat``. Use the former
    for text files and the latter for sequence files created from within R or as
    outputs from RHIPE(e.g ``rhlapply`` or ``rhmr``). 
.. note:: one can't use any sequence file, they must have been created via a RHIPE function. Custom Input formats are also possible. Download the source and look at ``code/java/RXTextInputFormat.java``

outputformat
    Either of ``TextOutputFormat`` or ``SequenceFileOutputFormat``. In case of the former, the return value from the mapper or reducer is converted to character and written to disk. The following code is used to convert to character. 

::
	
	paste(key,sep='',collapse=field_separator)


Custom output formats are also possible. Download the source and look at ``code/java/RXTextOutputFormat.java``

If custom formats implement their own writables, it must subclass RXWritable or use one of the writables presents in RHIPE

shared.files
    same as in ``rhlapply``, see that for documentation. 
verbose
    If T, the job progress is displayed. If false, then the job URL is displayed. 

At any time in the configure, close, map or reduce function/expression, the variable ``mapred.task.is.map`` will be equal to TRUE if it is map task,FALSE otherwise (both strings) Also, ``mapred.iswhat`` is "mapper", "reducer", "combiner" in their respective environments.

configure
    A list with either one element (an expression) or two attributes "map" and "reduce" both of which must be expressions. These expressions are called in their respective environments, i.e the map expression is called during the map configure and similarly for the reduce expression. The reduce expression is called for the combiner configure method.

    If only one list element, the expression is used for both the map and reduce 
close
    Same as configure . 
map
    a function that takes two values key and value. Should return a list of
    lists. Each list entry must contain two elements, the first one is the key
    and second the value (they do not have to be named), e.g.

::

	ret <- list()
	ret[[1]] <-  list(key=c(1,2), value=c('x','b'))
	return(ret)


If any of key/value are missing the output is not collected, e.g. return NULL
to skip this record. If the input format is a ``TextInputFormat``, the input
value is the entire line and the input key is probably useless to the user( it is a number indicating bytes into the file). If the input format is ``SequenceFileInputFormat``, the key and value are taken from the sequence file.

reduce
    Not needed if ``mapred.reduce.tasks`` is 0. Takes a key and a list of
    values( all values emitted from the maps that share the same map output key
    ). If ``step`` is True, then not a list. Must return a list of lists each
    element of which must have two elements key and value. This collects all the
    values and sends them to function. If NULL is returned nothing is collected
    by the Hadoop collector 

step
    If step is TRUE, then the reduce function is called every batch of values
    (the size of the batch is user configurable) corresponding to a key 

        * The variable ``red.status`` is equal to 1 on the first call.
        * ``red.status`` is equal to 0 for every subsequent calls including the last value
        * The reducer function is called one last time with ``red.status`` equal to -1. The value is NULL.

          Anything returned at any of these stages is written to disk. The
          ``close`` function is called once every value for a given key has been
          processed, but returning anything has no effect. To assign to the
          global environment use the ``<<-`` operator. To bail out at any time
          (i.e after the third value you do not want to process anymore) and
          move onto the next key, return a list with the attribute ``stop`` and
          set it to 1.

combiner
    TRUE or FALSE, to use the reducer as a combiner. Using a combiner makes computation more efficient. If combiner is true, the reduce function will be called as a combiner (0 or more times, it may never be called during the combine stage even if combiner is T) .

    The value of ``mapred.task.is.map`` is "true" or "false" FALSE (both strings) if the combiner is being executed as part of the map stage or reduce stage respectively.

    Whether knowledge of this is useful or not is something I'm not sure
    of. However, if combiner is TRUE , keep in mind,your reduce function must be
    able to handle inputs sent from the map or inputs sent from the reduce
    function(itself). 

libjars
    If specifying a custom input/output format, the user might need to specify
    jar files here. 

hadoop.mapreduce
    set RHIPE and Hadoop options via this. 

``hadoop.mapreduce`` Options
----------------------------

All the options that can be set for Hadoop can be set via
``hadoop.mapreduce``. Here is a list of some options RHIPE uses

rhipejob.rport
	The port on which Rserve runs. 
	
	*default:* 8888
rhipejob.outfmt.is.text
	1 if the output format is textual. 
	
	*default:* 1
rhipejob.textoutput.fieldsep
	The output field seperator. 
	
	*default:* ''
rhipejob.textinput.comment
	If the input is textual and a line begins with this, it is skipped. 
	
	*default:* '#'
rhipejob.combinerspill
	This number of items are collected by the combiner before being sent to Rserve. 
	
	*default:* 100,000
rhipejob.tom
	Number of map key,values collated before being sent to the mapper (on Rserve). 
	
	*default:* 200,000
rhipejob.frommap
	Number of map output key,values to bring from Rserve in one go. 
	
	*default:* 200,000
rhipejob.tor.batch
	Number of reduce values to be sent to Reducer (batching). 
	
	*default:* 200,000
rhipejob.copy.to.dfs
	Copy side effect files from local to DFS? 
	
	*default:* 1
rhipejob.inputformat.keyclass
	Provide the full Java URL to the keyclass e.g ``org.saptarshiguha.rhipe.hadoop.RXWritableText``, when using a Custom InputFormat implement RXWritable and implement the methods. 
	
	*default:* The default is chosen depending on TextInputFormat or SequenceFileInputFormat.
rhipejob.inputformat.valueclass
	Provide the full Java URL to the valueclass e.g ``org.saptarshiguha.rhipe.hadoop.RXWritableText`` when using a Custom InputFormat implement RXWritable and implement the methods. 
	
	*default:* 	The default is chosen depending on TextInputFormat or SequenceFileInputFormat.
rhipejob.input.format.class
	Specify yours here. 
	
	*default:* 	As above, the default is either ``org.saptarshiguha.rhipe.hadoop.RXTextInputFormat`` or ``org.apache.hadoop.mapred.SequenceFileInputFormat``
rhipejob.outputformat.keyclass
	Provide the full Java URL to the value e.g ``org.saptarshiguha.rhipe.hadoop.RXWritableText`` , also the valueclass must implement RXWritable. 
	
	*default:* The default is chosen depending on TextInputFormat or SequenceFileInputFormat
rhipejob.outputformat.valueclass
	Provide the full Java URL to the value e.g ``org.saptarshiguha.rhipe.hadoop.RXWritableText`` , also the valueclass must implement RXWritable. 
	
	*default:* The default is chosen depending on TextInputFormat or SequenceFileInputFormat
rhipejob.output.format.class
	Specify yours here, provide libjars if required. 
	
	*default:* As above, the default is either ``org.saptarshiguha.rhipe.hadoop.RXTextOutputFormat`` or ``org.apache.hadoop.mapred.SequenceFileInputFormat``

Custom Formats or Writables
---------------------------

You can specify your own Input/Output Formats and Writables. See the source in
``code/java/`` for a ``RXTextInputFormat`` which implements
``TextInputFormat``. The reason this was re implemented is because RHIPE
requires that all Writables (for the Key and Value) must implement RXWritable or
use one of the Writables in RHIPE e.g RXWritableLong, RXWritableText,
RXWritableDouble,RXWritableRAW

Side Effect files
-----------------
Files written to ``tmp/`` (no leading slash !) e.g ``pdf("tmp/x.pdf")`` will be copied to the output folder.


