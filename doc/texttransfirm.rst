.. highlight:: r
   :linenothreshold: 5

.. _rhipeserialize:

.. index:: textouput;transformations on text files
.. index:: textouput;subsets of text files, textoutput;quoting text, textoutput;separators for vectors
.. index:: rhipe_string_quote,mapred.field.separator,mapred.textoutputformat.usekey

**********************
Transforming Text Data
**********************

This chapter builds on the :ref:`Airline Dataset`. One foreseeable use of RHIPE is
to transform text data. For example,

1. Subset Southwest Airline and Delat Airways information to create a new set of text files, one with only Southwest and the other with Delta.

2. Transform the original text data to one with fewer columns and some transformed e.g. Airport Codes to full names.

We'll cover both examples.

.. index:: partitioner

Subset
------

The text data looks like

::

  1987,10,23,5,1841,1750,2105,2005,PS,1905,NA,144,135,NA,60,51,LAX,SEA,954,NA,NA,0,NA,0,...
  1987,10,24,6,1752,1750,2010,2005,PS,1905,NA,138,135,NA,5,2,LAX,SEA,954,NA,NA,0,NA,0,...
  ...
  ...

The carrier name is column 9. Southwest carrier code is *WN*, Delta is *DL*. Only those rows with column 9
equal to *WN* or *DL* will be saved.

::

  map <- expression({
    ## Each element of map.values is a line of text
    ## this needs to be tokenized and then combined
    tkn <- strsplit(unlist(map.values),",")
    text <- do.call("rbind",tkn)
    text <- text[text[,9] %in% c("WN","DL"),,drop=FALSE]
    if(nrow(text)>0) apply(text,1, function(r) rhcollect(r[9], r))
  })

``rhcollect`` requires both a key and value but we have no need for the key. So
NULL is given as the key argument and *mapred.textoutputformat.usekey* is set to
FALSE so that the key is not written to disk. RHIPE quotes strings, which we do
not want (nothing is quoted), so *rhipe_string_quote* is set to
'' and *mapred.field.separator* is  "," since the original data is comma separated.
A partitioner is used to send all the Southwest flights to one file and Delta to another.

::


  z <- rhmr(map=map,ifolder="/airline/data/2005.csv",ofolder="/airline/southdelta",
            ,inout=c("text","text"),orderby="char",
            part=list(lims=1,type="string"),
            mapred=list(
             mapred.reduce.tasks=2,
              rhipe_string_quote='',
              mapred.field.separator=",",
              mapred.textoutputformat.usekey=FALSE))
  rhex(z)

The output, in one file is

::

  2005,1,5,3,1850,1850,2208,2025,WN,791,N404,258,155,207,103,0,BDL,...
  2005,1,5,3,810,810,1010,940,WN,824,N784,180,150,155,30,0,BDL,...
  2005,1,5,3,1430,1325,1559,1435,WN,317,N306SW,89,70,61,84,65,BDL,...
  2005,1,5,3,705,705,830,815,WN,472,N772,85,70,57,15,0,BDL,...

and the other
::

  2005,12,22,4,1652,1655,1815,1837,DL,901,N109DL,...
  2005,12,22,4,1825,1825,1858,1848,DL,902,N932DL,...
  2005,12,22,4,1507,1511,1641,1649,DL,903,N306DL,...
  
Transformations
---------------

Convert each airport codes to their name equivalent. Airport codes can be found at the `JSM website <http://stat-computing.org/dataexpo/2009/the-data.html>`_ . When working with massive data, repeatedly used operations need to be as fast as possible.
Thus we will save the airport code to airport name as a hash table using the ``new.env`` function.
Airport codes (origin and destination) are in columns 17 and 18. The ``setup`` expression loads this 
data set and creates a function that does the mapping.


::

  airport <- read.table("~/tmp/airports.csv",sep=",",header=TRUE,stringsAsFactors=FALSE)
  aton <- new.env()
  for(i in 1:nrow(airport)){
    aton[[ airport[i,"iata"] ]] <- list(ap=airport[i,"airport"],latlong=airport[i,c("lat","long")])
  }
  rhsave(aton,file="/tmp/airports.Rdata")
  
  setup <- expression({
    load("airports.Rdata")
    co <- function(N){
      sapply(text[,N],function(r){
        o <- aton[[ r[1] ]]$ap
        if(is.null(o)) NA else sprintf('"%s"',o)
      })
    }
  })
  
The map will use the ``aton`` dictionary to get the complete names which are quoted (in line 13 above). 
Removing the ``sprintf`` makes it much faster.

::

  map <- expression({
    tkn <- strsplit(unlist(map.values),",")
    text <- do.call("rbind",tkn)
    text[,17] <- co(17)
    text[,18] <- co(18)
    apply(text,1,function(r){
      rhcollect(NULL,r)
    })
  })
  
  z <- rhmr(map=map,ifolder="/airline/data/2005.csv",ofolder="/airline/transform",
            ,inout=c("text","text"),
            shared=c("/airport/airports.Rdata"),
            setup=setup,
            mapred=list(
              mapred.reduce.tasks=0,
              rhipe_string_quote='',
              mapred.field.separator=",",
              mapred.textoutputformat.usekey=FALSE))
  rhex(z)
  

and this gives us

::

  1987,10,28,3,NA,1945,NA,2100,...,"San Francisco International","John Wayne /Orange Co,...
  1987,10,29,4,2025,1945,2141,2100,...,"San Francisco International","John Wayne /Orange Co,...
  1987,10,30,5,1947,1945,2109,2100,...,"San Francisco International","John Wayne /Orange Co,...
  1987,10,1,4,2133,2100,2303,2218,...,"San Diego International-Lindbergh","San Francisco International,...
