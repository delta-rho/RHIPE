context("Simple mr job")

test_that("test rhinit", {
   rhinit()
})

test_that("clean /tmp/rhipeTest and set working directory", {
   if(rhexists("/tmp/rhipeTest"))
      rhdel("/tmp/rhipeTest")

   # TODO: add test to see if rhmkdir honors working directory
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
   # rhoptions(runner = "/share/apps/R/3.0.2/bin/R CMD /share/apps/R/3.0.2/lib64/R/library/Rhipe/bin/RhipeMapReduce --slave --silent --vanilla")
   
   # map code for computing range
   rangeMap <- rhmap({
      by(r, r$Species, function(x) {
         rhcollect(
            as.character(x$Species[1]),
            range(x$Sepal.Length)
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
      label = "mr job ran successfully")
   
   res <- do.call(rbind, lapply(res, function(x) {
      data.frame(species=x[[1]], min=x[[2]][1], max=x[[2]][2], stringsAsFactors=FALSE)
   }))
   res <- res[order(res$species),]
   
   resTest <- as.numeric(by(iris, iris$Species, function(x) max(x$Sepal.Length)))
   
   expect_equivalent(res$max, resTest, "result of mr job is correct")
})

# several parameters of rhwatch to test (readback, mapred, combiner, different input/output formats)

