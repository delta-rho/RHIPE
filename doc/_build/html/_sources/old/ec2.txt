Using RHIPE on EC2
==================

.. highlight:: sh
   :linenothreshold: 5


Introduction
------------

RHIPE also works on EC2 using Cloudera's scripts. Let me demonstrate

Download
^^^^^^^^

The Cloudera scripts can be found at http://archive.cloudera.com/docs/_getting_started.html

Follow the instructions to test your working EC2 installation. 

Using RHIPE on EC2
^^^^^^^^^^^^^^^^^^

1. You need to create an entry in your `~/.hadoop-ec2/ec2-clusters.cfg`, e.g.

::

 [test2]
 ami=ami-6159bf08 # Fedora 32 bit instance
 instance_type=c1.medium
 key_name=saptarshiguha ## Your key name
 availability_zone=us-east-1c
 private_key=PATH_TO_PRIVATE_KEY
 ssh_options= -i %(private_key)s -o StrictHostKeyChecking=no
 user_data_file=the file you download in step 2

In particular, RHIPE only works with 32/64 bit Fedora instance types, so choose those AMIs.

2. Download this file( http://github.com/saptarshiguha/RHIPE/blob/master/code/hadoop-ec2-init-remote.sh ) and replace the file of the same name (it is in the Cloudera distribution). This file contains one extra shell function to install code RHIPE requires: R, Google's protobuf and RHIPE

3. Now start your cluster 

::

 python hadoop-ec2 launch-cluster --env REPO=testing --env HADOOP_VERSION=0.20 test2 3

The number (3) must be greater than 1.

4. *Wait*, till you it completely finishes booting up (the cloudera scripts tell you the url of the jobtracker). Login to the cluster 

::

 python hadoop-ec2 login test2

5. Start `R`, and try the following

::

 library(Rhipe)
 z <- rhlapply(10,runif)
 ## Runs on a local machine(i.e the master)
 rhex(z,changes=list(mapred.job.tracker='local'))


::
 
 library(Rhipe)
 ## Runs on the cluster
 z <- rhlapply(10,runif)
 rhex(z)



6. Consider the more involved problem of bootstrapping. See this question posed on the 
R-HPC mailing list (http://permalink.gmane.org/gmane.comp.lang.r.hpc/221).
Using Rhipe( chunksize (see the posting) is 1000 per task which results in 100 tasks)

::

 y <- iris[which(iris[,5] != "setosa"), c(1,5)]
 rhsave(y,file="/tmp/tmp.Rdata")

 ## The function 'f' depends on 'x' so we must save it
 ## using rhsave and then load it in the setup

 setup <- expression({
    load("tmp.Rdata")
   })

 f<- function(i){
    ind <- sample(100, 100, replace=TRUE)
    result1 <- glm(y[ind,2]~y[ind,1], family=binomial(logit))
    return(structure(coefficients(result1), names=NULL))
 }

 z <- rhlapply(100000L,f,shared="/tmp/tmp.Rdata",setup=setup,
              mapred=list(mapred.map.tasks=100000L/1000
                ,mapred.reduce.tasks=5))

 g <- rhex(z)
 g1 <- do.call("rbind",lapply(g,function(r) r[[2]]))
 g2 <- cbind(unlist(lapply(g,function(r) r[[1]])),g1)

I used 3 c1.xlarge nodes(each $0.68/hr). This took 2 minutes and  5 seconds to run and another minute to read the data back in.

On 10 similar nodes, this took 1 minute and 2 seconds. There is a point where it won't become any faster.

On 20 nodes(with `mapred.map.tasks=160`), it takes 52 seconds (probably not worth the extra cost ... ) 


