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
   expect_warning(
       res <- rhwatch(
      map = rangeMap, 
      reduce = rangeReduce,
      input = "irisData",
      output = "irisMax"
   ))
   
   expect_equal("FAILED", res[[1]]$state)

   # check to see there are any error dump files
   # err.files <- rhls(paste(rhoptions()$HADOOP.TMP.FOLDER, "map-reduce-error", sep="/"), recurse=TRUE)
   err.files <- rhls(rhoptions()$HADOOP.TMP.FOLDER)
   err.files <- err.files[grepl("/map-reduce-error", err.files$file),]

   expect_true(nrow(err.files) > 0)

   # extract id from tracking to match the file
   f.tracking <- gsub(".*_([0-9]+_[0-9]+).*", "\\1", err.files$file)
   res.tracking <- gsub(".*_([0-9]+_[0-9]+).*", "\\1", res[[1]]$tracking)

   # check to see if there is a corresponding error file
   err.file.ind <- which(f.tracking %in% res.tracking)
   expect_true(length(err.file.ind) == 1)
   
   # get the error file
   err.file.name <- err.files$file[err.file.ind]
   rhget(err.file.name, "last.dump.Rda")
   ## this is original code:
   # load("last.dump.Rda")
   # expect_true(exists("last.dump"))
   # file.remove("last.dump.Rda")
   ## but last.dump.Rda is a directory, for example:
   ## last.dump.Rda/map-reduce-errorjob_1416592114994_0015/task_1416592114994_0015_m_000000/last.dump.rda
   ## so for now, deal with it like this:
   ## this needs to be updated!!
   # load(list.files("last.dump.Rda", recursive = TRUE, full.names = TRUE)[1])
   # expect_true(exists("last.dump"))
   # unlink("last.dump.Rda", recursive = TRUE)
   
   # here a user would run debugger() to step into the stack trace
   # of the error to find the problem   
})
