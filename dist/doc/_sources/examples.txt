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


**One Machine**

``trials`` is 1,000,000
::

  system.time({r <- sapply(1:trials, nbrmean)})
   user   system  elapsed
   1603.414    0.127 1603.789


**Distributed, output to file**
::

  system.time({
    r <- rhlapply(
                  trials, func=nbrmean,
                  output.folder="/test/one") 
  })
  user  system  elapsed 
   63.117  2.330 179.747 


A **9x speed bump**. Note, the outputs are compressed, so Hadoop needs to decompress them. If native decompression libraries are not found, Hadoop uses java to decompress.  

Using Shared Files and Side Effects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
::

  h=rhlapply(length(simlist)
    ,func=function(r){
      ## do something from data loaded from session.Rdata
      pdf("tmp/a.pdf")
      plot(animage)
      dev.off()},
    configure=expression({
      load("session.Rdata")
    }),
    hadoop=list(mapred.map.tasks=1000),
    shared.files=("/tmp/session.Rdata"))


Here ``session.Rdata`` is copied from HDFS to local temporary directories (making for faster reads). This
is a useful idiom for loading code that the ``rhlapply`` function might depend on. For example, assuming the image is not *huge*

::

  rhsave.image("/tmp/myimage.Rdata")
  rhlapply(N,function(r) {
    object <- dataset[[r]]
    G(object)
  },configure=expression({load("myimage.Rdata")}))


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

	m <- function(key,word){
	   list(list(key=word,value=1))
	   }
	 r <- function(key,value){
	   value <- do.call("sum",value)
	   return(list(list(key=key,value=value)))
	  }


Run it 

::

  rhmr(map=m,reduce=r,input.folder="/tmp/words",output.folder='/tmp/words.cnt'
     ,inputformat='SequenceFileInputFormat',outputformat='SequenceFileOutputFormat')

Subset a file
^^^^^^^^^^^^^
We can use this RHIPE to subset files.

::

  m <- function(key,val){
    if(condition.is.true(val))
      return(list(list(key='',value=val)))
  }	      
  rhmr(mapper=m,reduce=function(){},
       input.folder=inf,output.folder=opf,
       hadoop=list(mapred.reduce.tasks=0))


Covariance Matrix
^^^^^^^^^^^^^^^^^
First create a file of 50 million rows with 100 columns. 

::
	
	f <- function(k){
	  return(rnorm(100,0,1))
	}
	rhlapply(50e6,f,output.folder="/tmp/bigd",takeAll=F)



Now calculate the column sums, sum of squares and dot products which is sufficient to calculate correlations, for the first 100 columns. 

::

  m <- function(k,v){
    v <- v[1:100]
    coln <- 1:(length(v)-1)
    tl <- length(v)
    ret <- sapply(coln,function(i){
      w <- v[i:tl]
      sums <- w[1]
      ssq <- w[1]^2
      dotprod <- w[1]*w[2:length(w)]
      list(key=as.integer(i),value=list(sums=sums,ssq=ssq,dotprod=dotprod))
    },simplify=F)
    return(ret)
  }

  r <- function(k,v){
    summs <- sum(do.call("rbind", lapply(v,function(r) r$sums)))
    ssq <- sum(do.call("rbind", lapply(v,function(r) r$ssq)))
    dotprod <- apply(do.call("rbind", lapply(v,function(r) r$dotprod)),2,sum)
    ret = list(list(key=k,value=list(sums=summs,ssq=ssq,dotprod=dotprod)))
    return(ret)
  }

  rhmr(map=m,reduce=r,combiner=T,input.folder="/tmp/bigd",output.folder="/tmp/bigo",
       inputformat="SequenceFileInputFormat",outputformat="SequenceFileOutputFormat")


The keys in the sequence file are the column numbers, each entry will have a contain value with the names sums,/ssq/ and dotprod which is enough to calculate correlations.


::
	
	suff <- rhsqallKV("/tmp/bigo",ignore=F)



Naive K-Means Clustering
^^^^^^^^^^^^^^^^^^^^^^^^

Is there a need to cluster a billion row data set. Take a large sample, estimate the variances(of means) of the concerned columns and then take another sample controlling for the variance and cluster on the sample.

However, if you must,

Find the number of rows, we assume text input format. 

::

  m <- function(key,value){
    ## we use as.integer to save on the bytes sent.
    return(list(list(key=as.integer(1),value=1)))
  }
  r <- function(key,value){
    value <- sum(unlist(value))
    return(list(list(key='count', value=value)))
  }
  rhmr(map=m,reducer=r, combiner=T, input.folder=X,output.folder=Y)


Read in the text file (broken up in part* files inside Y on the HDFS), there will be 1 row with key equal to count and the value is the number of rows.

Sample k values and makes these the centers c0

::
  
  ##Only approximate sample, so increase it to get enough.
  pct <- ncols / nrows_of_dataset*2
 m <- function(key,value){
   y <- runif(1)
   if(y < pct)
     return(list(list(key=NULL,value=value)))
 }
 rhmr(map=m,reducer=function(){},preload=list(env=c('pct')),
      input.folder=X,output.folder=Y,
      hadoop=list(mapred.reduce.tasks=0,rhipejob.kvsep=''))
 system("$HADOOP/bin/hadoop dfs -cat /Y/p* > /tmp/centers.txt")
 read.table("/tmp/centers.txt")
 ## Create a matrix centers which has many columns as there are in there dataset
 ## and k rows

Find the distance of every point to the centers and emit the the center to which it is closest.


::
  
  m <- function(r){
    r <- as.numeric(r)
    ret <- sapply(1:nrow(centers),function(k){
      m=centers[k,]
      c(k,(r[2]-m[2])^2+(r[1]-m[1])^2)
    },simplify=T)
    u <- which.min(ret[2,])
    ret <- list( list( key=ret[1,u], value = c(r,1)))
    return(ret)
  }
  r <- function(key,value){
    value <- do.call("rbind",value)
    l <- list( list(key=key, value=apply(value,2,sum)))
    return(l)
  }
 rhmr(map=m,reduce=r,input.folder="X",output.folder="Y",combiner=T)


Read in the centers and see update centers (unless there is no change)

If finished iterating, assign rows to centers.

::
  
  assgn <- function(key,value){
    r <- as.numeric(value)
    ret <- sapply(1:nrow(centers),function(k){
      m <- centers[k,]
      c(k,(r[2]-m[2])^2+(r[1]-m[1])^2)
    },simplify=T)
    u <- which.min(ret[2,])
    fret <- c(r,ret[1,u])
    if (runif(1)<0.2) return(list((list(key=key,value=fret)))
                           }
    rhmr(map=assgn,red=function(){},input.folder="X",output.folder="Y",
         preload=list(env='centers'),
         hadoop=list(mapred.reduce.tasks=0))



Using a step == TRUE
^^^^^^^^^^^^^^^^^^^^

Like ``tapply``, this calculates the sum of the second column in a text file

One can use a combiner, but my file is small and I did not bother. 

::

  mapper <- function(key,r){
    x <- strsplit(r," +")[[1]]
    ret <- list()
    ret[[1]] <- list(key = x[1], value = as.numeric(x[2]))
    return(ret)
  }
  reducer <- function(key,r){
    r <- do.call("rbind",r)
    v=apply(r,2,sum)
    ret <- list()
    ret[[1]] <- list(key=key,value=v)
    ret
  }
  rhmr(input.folder="/tmp/wc",output.folder="/tmp/rand.out",map=mapper,reduce=reduce)


Doing the same with ``step`` equal to TRUE

::

  reduce <- function(key,value){
  ##Note the global assignment, the assignment is now permanent
  ##which is required since this function will be called repeatedly
    if(red.status==1) sums <<- 1 
    else if (red.status==0){
      sums <<- sums+value
    }else{
      ##red.status==-1
      list(list(key=key,value=sums))
    }
  }
  rhmr(input.folder="/tmp/rand2",output.folder="/tmp/rand.out",map=mapper,reduce=reduce,step=T)


    

