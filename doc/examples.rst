Examples
========

.. highlight:: r
   :linenothreshold: 5

``rhlapply``
------------

Simple Example
^^^^^^^^^^^^^^
Take a sample of 100 iid observations Xi from N(0,1). Compute the mean of the eight closest neighbours to X1. This is repeated 1,000,000 times. 
::

  nbrmean <- function(r){
    d <- matrix(rnorm(200),ncol=2)
    orig <- d[1,]
    ds <- sort(apply(d,1,function(r) sqrt(sum((r-orig)^2)))[-1])[1:8]
    mean(ds)
  }
  trials <- 1000000

**One Machine**

``trials`` is 1,000,000
::

  system.time({r <- sapply(1:trials, nbrmean)})
   user   system  elapsed
   1603.414    0.127 1603.789


**Distributed, output to file**
::

   mapred <- list(mapred.map.tasks=1000)		
   r <- rhlapply(1000000, fun=nbrmean,ofolder="/test/one",mapred=mapred)
   rhex(r)

Which took 7 minutes on a 4 core machine running 6 JVMs at once.

 

Using Shared Files and Side Effects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
::

  h=rhlapply(length(simlist)
    ,func=function(r){
      ## do something from data loaded from session.Rdata
      pdf("tmp/a.pdf")
      plot(animage)
      dev.off()},
    setup=expression({
      load("session.Rdata")
    }),
    hadoop=list(mapred.map.tasks=1000),
    shared=("/tmp/session.Rdata")) ##session.Rdata created by rhsave(..., file="/tmp/session.Rdata")


Here ``session.Rdata`` is copied from HDFS to local temporary directories (making for faster reads). This
is a useful idiom for loading code that the ``rhlapply`` function might depend on. For example, assuming the image is not *huge*

::

  rhsave.image("/tmp/myimage.Rdata")
  rhlapply(N,function(r) {
    object <- dataset[[r]]
    G(object)
  },setup=expression({load("myimage.Rdata")}))


In the above example, I wish to apply the ``G`` to every element in ``dataset``.


``rhmr``
--------

Word Count
^^^^^^^^^^
Generate the words, 1 word every line 

::

	rhlapply(10000,function(r) paste(sample(letters[1:10],5),collapse=""),output.folder='/tmp/words')


Word count using the sequence file 

::




Run it 

::

 z <- rhmr(map=m,reduce=r,inout=c("sequence","sequence"),
	ifolder="/tmp/words",ofolder='/tmp/wordcount')
  rhex(z)

Subset a file
^^^^^^^^^^^^^
We can use this RHIPE to subset files. Setting ``mapred.reduce.tasks`` to 5 writes the subsetted data across 5 files (even though we haven't provided a reduce task)

::

    m <- expression({
      for(x in map.values){
	y <- strsplit(x," +")[[1]]
	for(w in y) rhcollect(w,T)
      }})
    z <- rhmr(map=m,inout=c("text","binary"),
	ifolder="X",ofolder='Y',mapred=list(mapred.reduce.tasks=5))
    rhex(z)


.. Covariance Matrix
.. ^^^^^^^^^^^^^^^^^
.. First create a file of 50 million rows with 100 columns. 

.. ::
	
.. 	f <- function(k){
.. 	  return(rnorm(100,0,1))
.. 	}
.. 	rhlapply(50e6,f,output.folder="/tmp/bigd",takeAll=F)



.. Now calculate the column sums, sum of squares and dot products which is sufficient to calculate correlations, for the first 100 columns. 

.. ::

..   m <- function(k,v){
..     v <- v[1:100]
..     coln <- 1:(length(v)-1)
..     tl <- length(v)
..     ret <- sapply(coln,function(i){
..       w <- v[i:tl]
..       sums <- w[1]
..       ssq <- w[1]^2
..       dotprod <- w[1]*w[2:length(w)]
..       list(key=as.integer(i),value=list(sums=sums,ssq=ssq,dotprod=dotprod))
..     },simplify=F)
..     return(ret)
..   }

..   r <- function(k,v){
..     summs <- sum(do.call("rbind", lapply(v,function(r) r$sums)))
..     ssq <- sum(do.call("rbind", lapply(v,function(r) r$ssq)))
..     dotprod <- apply(do.call("rbind", lapply(v,function(r) r$dotprod)),2,sum)
..     ret = list(list(key=k,value=list(sums=summs,ssq=ssq,dotprod=dotprod)))
..     return(ret)
..   }

..   rhmr(map=m,reduce=r,combiner=T,input.folder="/tmp/bigd",output.folder="/tmp/bigo",
..        inputformat="SequenceFileInputFormat",outputformat="SequenceFileOutputFormat")


.. The keys in the sequence file are the column numbers, each entry will have a contain value with the names sums,/ssq/ and dotprod which is enough to calculate correlations.


.. ::
	
.. 	suff <- rhsqallKV("/tmp/bigo",ignore=F)



.. Naive K-Means Clustering
.. ^^^^^^^^^^^^^^^^^^^^^^^^

.. Is there a need to cluster a billion row data set. Take a large sample, estimate the variances(of means) of the concerned columns and then take another sample controlling for the variance and cluster on the sample.

.. However, if you must,

.. Find the number of rows, we assume text input format. 

.. ::

..   m <- expression({
..       for(x in seq_along(map.values))
..          rhcollect(T,1)
..       })
..   r <- expression(
..         pre={
.. 	  count <- 0
.. 	 },
.. 	reduce={
.. 	  z <- unlist(reduce.values)
.. 	  count <- count+sum(unlist(reduce.values))
.. 	    },
.. 	post={
.. 	  rhcollect("NumRows",as.integer(count))
..        })
..   rhmr(map=m,reducer=r, ifolder=X,ofolder=Y,inout=c("text","sequence"))
..   numrows <- rhread("Y/p*")$NumRows


.. Sample k values and makes these the centers c0

.. ::
..   pct <- num_columns / numrows * 3
..   m <- expression({
..        for(x in seq_along(map.values)){
..          val <- map.values[[x]]
.. 	 if(runif(1) < pct)
.. 	     rhcollect(map.key[[x]],val)
.. 	  }
..        })
..   z<-rhmr(mapper=m,ifolder=X,ofolder="/tmp/centers",inout=c("text","sequence"),
..         mapred=list(mapred.reduce.tasks=10))
..   rhex(z)
..   centers <- rhread("/tmp/centers/p*")
..   ## subset to get num_column centers

.. Find the distance of every point to the centers and emit the the center to which it is closest.


.. ::
  
..   m <- function(r){
..     r <- as.numeric(r)
..     ret <- sapply(1:nrow(centers),function(k){
..       m=centers[k,]
..       c(k,(r[2]-m[2])^2+(r[1]-m[1])^2)
..     },simplify=T)
..     u <- which.min(ret[2,])
..     ret <- list( list( key=ret[1,u], value = c(r,1)))
..     return(ret)
..   }
..   r <- function(key,value){
..     value <- do.call("rbind",value)
..     l <- list( list(key=key, value=apply(value,2,sum)))
..     return(l)
..   }
..   rhmr(map=m,reduce=r,input.folder="X",output.folder="Y",combiner=T)


.. Read in the centers and see update centers (unless there is no change)

.. If finished iterating, assign rows to centers.

.. ::
  
..   assgn <- function(key,value){
..     r <- as.numeric(value)
..     ret <- sapply(1:nrow(centers),function(k){
..       m <- centers[k,]
..       c(k,(r[2]-m[2])^2+(r[1]-m[1])^2)
..     },simplify=T)
..     u <- which.min(ret[2,])
..     fret <- c(r,ret[1,u])
..     if (runif(1)<0.2) return(list((list(key=key,value=fret)))
..                            }
..     rhmr(map=assgn,red=function(){},input.folder="X",output.folder="Y",
..          preload=list(env='centers'),
..          hadoop=list(mapred.reduce.tasks=0))


