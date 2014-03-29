## This file contains tests that perform a simple map-reduce job 
## asynchronously and then joins it.

context("Interrupting a simple mr job")

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

## Function to loop for x seconds 
pause <- function(x) {
    s <- Sys.time()
    while (Sys.time() - s < x) {}
}

test_that("start simple mr job asynchronously, then join it", {
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

   # join the job (wait for it to complete)
   res <- rhjoin(jobtoken)
   
   # test rhofolder() function
   expect_true(file.path(hdfs.getwd(), "irisMax"), rhofolder(job), "rhofolder returns correct value")
   
   # get output and test for correctness
   irisMax <- rhread("irisMax")
   
   irisMax <- do.call(rbind, lapply(irisMax, function(x) {
      data.frame(species=x[[1]], min=x[[2]][1], max=x[[2]][2], stringsAsFactors=FALSE)
   }))
   irisMax <- irisMax[order(irisMax$species),]
   
   expectedMax <- as.numeric(by(iris, iris$Species, function(x) max(x$Sepal.Length)))
   
   expect_equivalent(irisMax$max, expectedMax, "result of mr job is not correct")
   
})

# several parameters of rhwatch to test (readback, mapred, combiner, different input/output formats)

