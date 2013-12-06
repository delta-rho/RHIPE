context("Simple mr job")

test_that("test rhinit", {
   rhinit()
})

test_that("clean /tmp/rhipeTest and set working directory", {
   if(rhexists("/tmp/rhipeTest"))
      rhdel("/tmp/rhipeTest")

   rhmkdir("/tmp/rhipeTest")
   hdfs.setwd("/tmp/rhipeTest")
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

test_that("run simple mr job", {
   # map code for computing range
   rangeMap <- rhmap({
      by(r, r$Species, function(x) {
         rhcollect(
            as.character(x$Species[1]),
            max(x$Sepal.Length)
         )
      })
   })
   
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
   res <- try(rhwatch(
      map = rangeMap, 
      reduce = rangeReduce,
      input = "irisData",
      output = "irisMax"
   ))
   
   expect_true(!inherits(res, "try-error"),
      label = "mr job errored out")
   
   res <- do.call(rbind, lapply(res, function(x) {
      data.frame(species=x[[1]], min=x[[2]][1], max=x[[2]][2])
   }))
   res <- res[order(res$species),]
   
   resTest <- structure(list(species = structure(1:3, .Label = c("setosa", 
   "virginica", "versicolor"), class = "factor"), min = c(5.3, 7.7, 
   6.7), max = c(5.8, 7.9, 7)), .Names = c("species", "min", "max"
   ), row.names = c(NA, 3L), class = "data.frame")
   
   expect_equivalent(res, resTest, "result of mr job is correct")
})

# several parameters of rhwatch to test (readback, mapred, combiner, different input/output formats)

