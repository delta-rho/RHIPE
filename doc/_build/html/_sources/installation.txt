.. highlight:: r
   :linenothreshold: 5

************
Installation
************
RHIPE is an R package, that can be downloaded at `this website <http://www.stat.purdue.edu/~sguha/rhipe>`_. To install
the user needs to

* Set an environment variable ``$HADOOP`` that points to the Hadoop  installation directory. It is expected that ``$HADOOP\bin`` contains the  Hadoop shell executable ``hadoop``. 
* A version of Google's Protocol Buffers (`here <http://code.google.com/p/protobuf/>`_) greater than 2.3.0

Once the package has been downloaded the user can install it via 
::


	R CMD INSTALL Rhipe_version.tar.gz


where ``version`` is the latest version of RHIPE. The source is under version
control at `GitHub <http://github.com/saptarshiguha/RHIPE/>`_ .

This needs to be installed on *all* the computers: the one you run your R environment and all the task computers. Use RHIPE is much easier if your filesystem layout (i.e location of R, Hadoop, libraries etc) is identical across all computers.

Tests
=====
In R

::

	library(Rhipe)
	rhinit(TRUE, TRUE) ## the TRUEs are optional, use them for debugging
	
should work successfully.

::

	rhwrite(list(1,2,3),"/tmp/x")

should successfully write the list to the HDFS

::

	rhread("/tmp/x")

should return a list of length 3 each element a list of 2 objects.

and a quick run of this should also work

::

  map <- expression({
    lapply(seq_along(map.values),function(r){
      x <- runif(map.values[[r]])
      rhcollect(map.keys[[r]],c(n=map.values[[r]],mean=mean(x),sd=sd(x)))
    })
  })
  ## Create a job object
  z <- rhmr(map, ofolder="/tmp/test", inout=c('lapply','sequence'),
            N=10,mapred=list(mapred.reduce.tasks=0),jobname='test')
  ## Submit the job
  rhex(z)
  ## Read the results
  res <- rhread('/tmp/test')
  colres  <- do.call('rbind', lapply(res,"[[",2))
  colres 
         n      mean        sd
   [1,]  1 0.4983786        NA
   [2,]  2 0.7683017 0.2937688
   [3,]  3 0.5936899 0.3425441
   [4,]  4 0.3699087 0.2666379
   [5,]  5 0.5179839 0.4060244
   [6,]  6 0.6278925 0.2952608
   [7,]  7 0.4920088 0.2785893
   [8,]  8 0.4592598 0.2674592
   [9,]  9 0.5734197 0.1928496
  [10,] 10 0.4942676 0.2989538


	

