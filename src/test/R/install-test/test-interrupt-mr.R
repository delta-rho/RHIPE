## This file contains tests that perform a simple map-reduce job 
## and interrupts it to make sure it is killed successfully.

context("Kill a simple mr job run asynchronously")

test_that("test rhinit", {
   rhinit()
})

test.dir <- file.path(rhoptions()$HADOOP.TMP.FOLDER, "rhipeTest")

test_that("clean rhoptions()$HADOOP.TMP.FOLDER/rhipeTest and set working directory", {
   if(rhexists(test.dir))
      rhdel(test.dir)

   rhmkdir(test.dir)
   hdfs.setwd(test.dir)
})

test_that("simple mr job setup", {
   # dummy set of data
   permute <- sample(1:150, 150)
   splits <- split(permute, rep(1:3, 50))
   irisSplit <- lapply(seq_along(splits), function(x) {
     list(x, iris[splits[[x]],])
   })

   if(rhexists("irisData"))
      rhdel("irisData")
   rhwrite(irisSplit, file="irisData")
})

test_that("start simple mr job asynchronously, then kill it", {
    ## Function to loop for x seconds 
    pause <- function(x) {
        s <- Sys.time()
        while (Sys.time() - s < x) {}
    }

    # map code for computing range
    rangeMap <- rhmap({
       by(r, r$Species, function(x) {
          rhcollect(
             as.character(x$Species[1]),
             range(x$Sepal.Length)
          )
       })
    })
    expect_true("rhmr-map" %in% class(rangeMap))
   
   # reduce code for computing max
   rangeReduce <- expression(
      pre = {
         rng <- c(Inf, -Inf)
      }, 
      reduce = {
         a <- reduce.key
         rx <- unlist(reduce.values)
         rng <- c(min(rng[1], rx, na.rm = TRUE), max(rng[2], rx, na.rm = TRUE))
      },
      post = {
         rhcollect(reduce.key, rng)
      }
   )
 
   # if irisMax already exists, delete it before starting job
   if (rhexists("irisMax")) { rhdel("irisMax") }
 
   # set up the job
   job <- rhwatch(
      map = rangeMap, 
      reduce = rangeReduce,
      input = "irisData",
      output = "irisMax",
      noeval=TRUE
   )
   
   # run the job
   jobtoken <- rhex(job, async=TRUE)
   
   # job status
   jobinfo1 <- rhJobInfo(jobtoken)
   expect_equal(length(jobinfo1), 9)
   
   # pause 2 sec
   pause(2)
   
   # kill the job
   rhkill(jobtoken)
   
   # get updated job info and check status for KILLED
   jobinfo2 <- rhJobInfo(jobtoken)
   expect_true(jobinfo2$State == "KILLED")
   # expect_true(jobinfo2$ReduceTasks[[1]]$Status == "KILLED")
   
   # check for output from the killed job 
   # (if irisMax exists, it should only contain logs)
   expect_true(!is.element(file.path(test.dir, "irisMax"), rhls()$file) ||
       all(regexpr("_logs$", rhls(file.path(test.dir, "irisMax"))$file)>=0),
       label="there is no output from killed job")

})

# several parameters of rhwatch to test (readback, mapred, combiner, different input/output formats)

