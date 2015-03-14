## Run the linear regression performance test function (linear.regression.performance.test)

############################
## load libraries
############################

library(Rhipe)
rhinit()
rhoptions(readback = FALSE)

<<<<<<< HEAD

=======
>>>>>>> 423f43cf0a9301d414fffdcf31d8e2d4ad9264b5
############################
## setup
############################

## top level directory for experiment on HDFS
<<<<<<< HEAD
hdfs.dir = file.path(rhoptions()$HADOOP.TMP.FOLDER, "linear.perf.test5")
=======
hdfs.dir = file.path(rhoptions()$HADOOP.TMP.FOLDER, "linear.perf.test2")
>>>>>>> 423f43cf0a9301d414fffdcf31d8e2d4ad9264b5
rhmkdir(hdfs.dir)

# number of files for the input data
nfile <- 256
# break time in seconds between jobs
sleep <- 60
<<<<<<< HEAD
sleep <- 5
=======
>>>>>>> 423f43cf0a9301d414fffdcf31d8e2d4ad9264b5
# number of replicate runs
run.vec <- 1
# number of reduce tasks
RED.TASKS <- 44
# number of map tasks
MAP.TASKS <- 44
# delete data when finished?
delete.data <- FALSE
 
############################
## factors
############################

# log2 number of observations
<<<<<<< HEAD
#n <- 30
n <- 20
# number of predictor variables
p.vec <- 2^(4:6) - 1
p.vec <- 2^4 - 1
# log2 number of observations per subset
m.vec <- 8:16
m.vec <- 8
# log2 number of subset groups
g.vec <- c(0, 7, 14)
g.vec <- 0
# log2 HDFS block size: 128, 256, 64 MB
BLK.vec <- c(27,28,26)
BLK.vec <- 27
# HDFS replication factor
REP.vec <- 1:3 
REP.vec <- 1
# MapReduce factors: Task JVM reuse, does not re-generate data
REUSE.vec <- c(1, -1)       
  
   
timing <- linear.regression.performance.test(nfile=nfile, run.vec=run.vec, 
   RED.TASKS=RED.TASKS, MAP.TASKS=MAP.TASKS, 
   n=n, p.vec=p.vec, m.vec=m.vec, g.vec=g.vec, BLK.vec=BLK.vec, REP.vec=REP.vec, 
   REUSE.vec=REUSE.vec, hdfs.dir=hdfs.dir, sleep=sleep, 
   delete.data=delete.data)

write.table(timing, file=paste("~/rhipe_perf_test_", basename(hdfs.dir), ".csv", sep=""),
   col.names=TRUE, row.names=FALSE, sep=",")
   
=======
n <- 30
#n <- 10
# number of predictor variables
p.vec <- 2^(4:6) - 1
#p.vec <- 2^4 - 1
# log2 number of observations per subset
m.vec <- 8:16
#m.vec <- 8
# log2 number of subset groups
g.vec <- c(0, 7, 14)
#g.vec <- 0
# log2 HDFS block size: 128, 256, 64 MB
BLK.vec <- c(27,28,26)
#BLK.vec <- 27
# HDFS replication factor
REP.vec <- 3:1 
#REP.vec <- 3
# MapReduce factors: Task JVM reuse, does not re-generate data
REUSE.vec <- c(1, -1)    

timing <- linear.regression.performance.test(nfile=nfile, run.vec=run.vec, RED.TASKS=RED.TASKS,
   MAP.TASKS=MAP.TASKS, 
   n=n, p.vec=p.vec, m.vec=m.vec, g.vec=g.vec, BLK.vec=BLK.vec, REP.vec=REP.vec, 
   REUSE.vec=REUSE.vec, hdfs.dir=hdfs.dir, sleep=sleep, 
   delete.data=delete.data)
   
   
   
   
linear.reg.generate.data(REP.vec=REP.vec, BLK.vec=BLK.vec, m.vec=m.vec, p.vec=p.vec, n=n, 
   n.file=n.file, run=1, MAP.TASKS=44, hdfs.dir=hdfs.dir, sleep=sleep)
>>>>>>> 423f43cf0a9301d414fffdcf31d8e2d4ad9264b5
