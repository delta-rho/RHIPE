## This file contains tests that perform a map-reduce job with
## an error to test Rhipe's error handling functionality.

context("MR job that throws an error in the map step\n")

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

test_that("mr job setup", {
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

custom.err.handler <- function() {
    dump.frames(to.file=TRUE)
    stop(paste(geterrmessage(), " (working directory=", getwd(), ")", sep=""))
}

test_that("run mr job with error in the map step", {
    # map code for computing range 
    # The error is in the by() statement, which uses Speces instead of Species
    rangeMap <- rhmap({
       options(error=function() {
                dump.frames(to.file=TRUE)
                stop(paste(geterrmessage(), " (working directory=", getwd(), ")", sep=""))
            })
       by(r, r$Speces, function(x) {
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
   
   # execute the job
   expect_error(res <- rhwatch(
      map = rangeMap, 
      reduce = rangeReduce,
      input = "irisData",
      output = "irisMax"
   ))
   
   # check to see there are any error dump files
   err.files <- rhls(paste(rhoptions()$HADOOP.TMP.FOLDER, "map-reduce-error", sep="/"), recurse=TRUE)
   expect_true(nrow(err.files) > 0)
   
   # check to see if an error file has been modified in the last minute
   err.file.dates <- strptime(err.files$modtime, format="%Y-%m-%d %H:%M")
   expect_true(any(difftime(Sys.time(), err.file.dates, units="minutes") < 1))
   
   # get the most recent error file (NOTE: this could get problematic
   # in a system where multiple people are running hadoop jobs from R at
   # the same time. In that case we need to add more code to distiguish 
   # the file created by this job.)
   
   last.file.ind <- rev(order(err.files$modtime))[1]
   err.file.name <- err.files$file[last.file.ind]
   rhget(err.file.name, "last.dump.Rda")
   load("last.dump.Rda")
   expect_true(exists("last.dump"))
   
   # here a user would run debugger() to step into the stack trace
   # of the error to find the problem
   
})
