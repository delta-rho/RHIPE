#' A performance test that runs a set of linear regression algorithms.
#'
#' The function \bold{linear.regression.performance.test} constructs a set of linear
#' regression problems to solve using Rhipe, and reports timing statistics for
#' three steps:
#' \enumerate{
#'    \item Create datasets (linear.reg.generate.data)
#'    \item Fit linear regression, including read/write time (linear.reg.compute.rw)
#'    \item Read/write time only for step 2 (linear.reg.rw)
#' }
#'
#' These functions are based on the manuscript:  \cr
#' "A Multi-Factor Designed Experiment for Performance of the D&R
#' R-Hadoop Computational Environment for Large Complex Data."
#' Jeff Li, Douglas G. Crabill, Ryan Hafen, and William S. Cleveland.
#' Department of Statistics, Purdue University.
#'
#' Notes on order in which the factors are varied when generating data
#' or computing the linear regressions, from slowest to fastest
#' \enumerate{
#'   \item run.vec: replicated runs
#'   \item REP.vec, BLK.vec: data are re-generated
#'   \item REUSE.vec, g.vec: factors does not need re-generating data
#'   \item m.vec, p.vec: they always run together as a unit
#' }
#'
#' @param nfile Number of files for the input data
#' @param run.vec Vector of replicate run IDs
#' @param MAP.TASKS Suggested (minimum) number of map tasks (Hadoop may ignore this)
#' @param RED.TASKS Suggested (minimum) number of reduce tasks (Hadoop may ignore this)
#' @param n log2 number of observations
#' @param p.vec Number of predictor variables (numeric vector)
#' @param m.vec log2 number of observations per subset (numeric vector)
#' @param g.vec log2 number of subset groups (numeric vector)
#' @param BLK.vec log2 HDFS block size
#' @param REP.vec HDFS replication factor
#' @param REUSE.vec Task JVM reuse, does not re-generate data (numeric vector, 1, -1)
#' @param hdfs.dir HDFS directory for saving datasets and results
#' @param mapred.options a list of options to be passed to the mapred parameter of rhwatch
#' @param sleep Break time in seconds between jobs
#' @param delete.data TRUE/FALSE delete linear regression data before exiting function?
#' @param quiet TRUE/FALSE suppress status messages? Default is FALSE.
#' @return timing data.frame of timing results
#'
#' @examples
#' library(Rhipe)
#' rhinit()
#' rhoptions(readback = FALSE)
#' timing.df <- linear.regression.performance.test(nfile=256, run.vec=1:2, RED.TASKS=1,
#'     n=10, p.vec=2^4:6 - 1, m.vec=8:16, g.vec=c(0, 7, 14), BLK.vec=c(),
#'     REP.vec=c(), REUSE.vec=c(1, -1), hdfs.dir=rhoptions()$HADOOP.TMP.FOLDER,
#'     sleep=60)
#'
#' @seealso rhwatch, rhex
#' @export
linear.regression.performance.test <- function(nfile=256, run.vec=1, MAP.TASKS=44, RED.TASKS=44, n=10,
   p.vec=2^4 - 1, m.vec=8, g.vec=0, BLK.vec=27, REP.vec=3, REUSE.vec=c(1,-1),
   hdfs.dir=rhoptions()$HADOOP.TMP.FOLDER, mapred.options=list(), sleep=60, delete.data=FALSE,
   quiet=FALSE) {

   require(Rhipe)
   readback.option.val <- rhoptions()$readback
   rhoptions(readback = FALSE)

   timing <- list()

   for (run in run.vec) {

      ## generate datasets
      timing.curr <- linear.reg.generate.data(nfile=nfile, REP.vec=REP.vec, BLK.vec=BLK.vec,
         m.vec=m.vec, p.vec=p.vec, n=n, run=run, hdfs.dir=hdfs.dir, sleep=sleep, quiet=quiet)

      Sys.sleep(time=sleep*2)

      ## computation + read/write time
      timing.curr <- c(timing.curr, linear.reg.compute.rw(REP.vec=REP.vec, BLK.vec=BLK.vec,
         REUSE.vec=REUSE.vec, g.vec=g.vec, m.vec=m.vec, p.vec=p.vec, MAP.TASKS=MAP.TASKS,
         RED.TASKS=RED.TASKS, run=run, hdfs.dir=hdfs.dir, mapred.options=mapred.options,
         sleep=sleep, quiet=quiet))

      Sys.sleep(time=sleep*2)

      ## read/write time
      timing.curr <- c(timing.curr, linear.reg.rw(REP.vec=REP.vec, BLK.vec=BLK.vec,
         REUSE.vec=REUSE.vec, g.vec=g.vec,
         m.vec=m.vec, p.vec=p.vec, MAP.TASKS=MAP.TASKS, RED.TASKS=RED.TASKS, run=run,
         hdfs.dir=hdfs.dir, sleep=sleep, quiet=quiet))

      timing <- c(timing, timing.curr)

      ## save the results
      # timing = ldply(timing, as.data.frame) #plyr library
      timing.curr <- do.call(rbind, lapply(timing.curr, FUN=data.frame))

      timing.file <- file.path(hdfs.dir, paste("timing.linear.reg.run",run,".RData", sep=""))
      rhsave(timing.curr, file=timing.file)

      #print(timing)
      cat("Timing results for run", run, "saved in",  timing.file, "\n")
   }

   if (delete.data) {
      cat("Deleting linear regression input and output data\n")
      rhdel(file.path(hdfs.dir, "dm"))
      rhdel(file.path(hdfs.dir, "gf"))
      rhdel(file.path(hdfs.dir, "nf"))
      # sleep for a relatively long time for HDFS to actually get rid of the data
      Sys.sleep(time=sleep) # originally sleep*5
   }

   rhoptions(readback =  readback.option.val)
   cat("Finished tests\n")
   timing <- do.call(rbind, lapply(timing, FUN=data.frame))
   timing
}

#' The function \bold{linear.reg.generate.data} generates datasets for the
#' linear regression performance tests.
#' @rdname linear.regression.performance.test
#' @export
linear.reg.generate.data <- function(REP.vec=3, BLK.vec=27, m.vec=8, p.vec=2^4 - 1, n=10,
   nfile=256, run=1, MAP.TASKS=44, hdfs.dir=rhoptions()$HADOOP.TMP.FOLDER,
   mapred.options=list(), sleep=60, quiet=FALSE) {

   require(Rhipe)
   if (!quiet) { cat("Generating linear regression data\n") }

   timing = list()
   compute = "Data"

   for (REP in REP.vec) {
   for (BLK in BLK.vec) {
   for (m in m.vec) {
   for (p in p.vec) {
       if (!quiet) {
          cat("Current params: REP=", REP, ", BLK=", BLK, ", m=", m, ", p=", p, "\n", sep="")
       }
       dm = list()
       dm$map = rhmap({
         options(error=dump.frames(to.file=TRUE))
         for (r in map.values){
           value = matrix(c(rnorm(m*p), sample(c(0,1), m, replace=TRUE)), ncol=p+1)
           rhcollect(r, value) # key is subset id
         }
       })
       dm$input = c(2^(n-m), nfile) ## just a vector, uses lapplyio to read
       dm$output = paste(hdfs.dir,"/dm/",'n',n,'p',p,"m",m,"run",run,"REP",REP,"BLK",BLK, sep="")
       dm$jobname = dm$output
       dm$mapred = mapred.options

       dm$mapred = list(
            mapred.task.timeout=0
            , mapred.map.tasks=MAP.TASKS #CDH3,4
            , mapreduce.job.maps=MAP.TASKS #CDH5
            , mapred.reduce.tasks=0 #CDH3,4
            , mapreduce.job.reduces=0 #CDH5
            , dfs.replication=REP
            , dfs.block.size=2^BLK
            , dfs.blocksize=2^BLK #CDH5
            , mapred.job.reuse.jvm.num.tasks=-1
            , mapreduce.job.jvm.numtasks=-1
        )

       dm$parameters = list(m=2^m, p=p)
       dm$noeval = TRUE
       dm.mr = do.call('rhwatch', dm)

       t = as.numeric(system.time({rhex(dm.mr, async=FALSE)})[3])
       timing[[length(timing)+1]] = list(compute=compute, n=n, p=p, m=m,
          run=run, REP=REP, BLK=BLK, REUSE=-1, g=NA, t=t)
       Sys.sleep(time=sleep)
   }}}}
   timing
}


#' The function \bold{linear.reg.comput.rw} collects times for calculation and
#' read/write time for the linear regression performance tests.
#' @rdname linear.regression.performance.test
#' @export
linear.reg.compute.rw <- function(REP.vec=3, BLK.vec=27, REUSE.vec=c(1, -1),
   g.vec=0, m.vec=8, p.vec=2^4 - 1, MAP.TASKS=44, RED.TASKS=44, run=1,
   hdfs.dir=rhoptions()$HADOOP.TMP.FOLDER, sleep=60, quiet=FALSE, mapred.options=list()) {

   require(Rhipe)
   if (!quiet) { cat("Computing linear regression\n") }

   timing = list()
   compute = "M + R/W"

   for (REP in REP.vec) {
   for (BLK in BLK.vec) {
   for (REUSE in REUSE.vec) {
   for (g in g.vec) {
   for (m in m.vec) {
   for (p in p.vec) {
         if (!quiet) {
            cat("Current params: REP=", REP, ", BLK=", BLK, ", REUSE=", REUSE, ", g=", g,
               ", m=", m, ", p=", p, "\n", sep="")
         }
         gf = list()
         gf$map = rhmap({
            options(error=dump.frames(to.file=TRUE))
            for (r in seq_along(map.values)) {
               v = map.values[[r]]
               value = c(1,glm.fit(v[,1:p],v[,p+1],family=binomial())$coef)
               rhcollect(map.keys[[r]] %% g, value)
            }
         })
         gf$reduce = expression(
            pre = { v = rep(0,p+1) },
            reduce = { v = v + apply(matrix(unlist(reduce.values), ncol=p+1, byrow=TRUE), 2, sum) },
            post = { rhcollect(reduce.key, v) }
         )

         gf$mapred = mapred.options
         gf$mapred[["mapred.map.tasks"]] <- MAP.TASKS #CDH3,4
         gf$mapred[["mapreduce.job.maps"]] <- MAP.TASKS #CDH5
         gf$mapred[["mapred.reduce.tasks"]] <- RED.TASKS #CDH3,4
         gf$mapred[["mapreduce.job.reduces"]] <- RED.TASKS #CDH5
         gf$mapred[["rhipe_map_buff_size"]] <- 2^15
         gf$mapred[["dfs.replication"]] <- REP
         gf$mapred[["dfs.blocksize"]] <- 2^BLK #CDH5
         gf$mapred[["dfs.block.size"]] <- 2^BLK
         gf$mapred[["mapred.job.reuse.jvm.num.tasks"]] <- REUSE
         gf$mapred[["mapreduce.job.jvm.numtasks"]] <- REUSE #CDH5

         gf$parameters = list(p=p, g=2^g)
         gf$input = paste(hdfs.dir,"/dm/",'n',n,'p',p,"m",m,"run",run,"REP",REP,"BLK",BLK, sep="")
         gf$output = paste(hdfs.dir,"/gf/",'n',n,'p',p,"m",m,"run",run,"REP",REP,"BLK",BLK,"REUSE",REUSE,"g",g, sep="")
         gf$jobname = gf$output
         gf$noeval = TRUE
         gf.mr = do.call('rhwatch', gf)
         t = as.numeric(system.time({rhex(gf.mr, async=FALSE)})[3])
         timing[[length(timing)+1]] = list(compute=compute, n=n, p=p, m=m, run=run, REP=REP, BLK=BLK, REUSE=REUSE, g=g, t=t)
         Sys.sleep(time=sleep)
   }}}}}}
   timing
}

#' The function \bold{linear.reg.rw} collects times for read/write only for
#' the linear regression performance tests.
#' @rdname linear.regression.performance.test
#' @export
linear.reg.rw <- function(REP.vec=3, BLK.vec=27, REUSE.vec=c(1, -1),
   g.vec=0, m.vec=8, p.vec=2^4 - 1, MAP.TASKS=44, RED.TASKS=44, run=1,
   hdfs.dir=rhoptions()$HADOOP.TMP.FOLDER, sleep=60, quiet=FALSE, mapred.options=list()) {

   require(Rhipe)
   if (!quiet) { cat("Calculating read/write time\n") }

   timing = list()
   compute = "R/W"

   for (REP in REP.vec) {
   for (BLK in BLK.vec) {
   for (REUSE in REUSE.vec) {
   for (g in g.vec) {
   for (m in m.vec) {
   for (p in p.vec) {
       if (!quiet) {
         cat("Current params: REP=", REP, ", BLK=", BLK, ", REUSE=", REUSE, ", g=", g,
            ", m=", m, ", p=", p, "\n", sep="")
       }
       nf = list()
       nf$map = expression({
         options(error=dump.frames(to.file=TRUE))
         for (r in seq_along(map.values)) {
           value = numeric(p+1)
           rhcollect(map.keys[[r]] %% g, value)
         }
       })
       nf$reduce = expression(
         post = { rhcollect(reduce.key, numeric(p+1)) }
       )

       nf$mapred = mapred.options
       nf$mapred[["mapred.map.tasks"]] <- MAP.TASKS #CDH3,4
       nf$mapred[["mapreduce.job.maps"]] <- MAP.TASKS #CDH5
       nf$mapred[["mapred.reduce.tasks"]] <- RED.TASKS #CDH3,4
       nf$mapred[["mapreduce.job.reduces"]] <- RED.TASKS #CDH5
       nf$mapred[["rhipe_map_buff_size"]] <- 2^15
       nf$mapred[["dfs.replication"]] <- REP
       nf$mapred[["dfs.blocksize"]] <- 2^BLK #CDH5
       nf$mapred[["dfs.block.size"]] <- 2^BLK
       nf$mapred[["mapred.job.reuse.jvm.num.tasks"]] <- REUSE
       nf$mapred[["mapreduce.job.jvm.numtasks"]] <- REUSE #CDH5

       nf$parameters = list(p=p, g=2^g)
       nf$input = paste(hdfs.dir,"/dm/",'n',n,'p',p,"m",m,"run",run,"REP",REP,"BLK",BLK, sep="")
       nf$output = paste(hdfs.dir,"/nf/",'n',n,'p',p,"m",m,"run",run,"REP",REP,"BLK",BLK,"REUSE",REUSE,"g",g, sep="")
       nf$jobname = nf$output
       nf$noeval = TRUE
       nf.mr = do.call('rhwatch', nf)
       t = as.numeric(system.time({rhex(nf.mr, async=FALSE)})[3])
       timing[[length(timing)+1]] = list(compute=compute, n=n, p=p, m=m, run=run, REP=REP, BLK=BLK, REUSE=REUSE, g=g, t=t)
       Sys.sleep(time=sleep)
   }}}}}}
   timing
}
