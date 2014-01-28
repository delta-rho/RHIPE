library(Rhipe)
rhinit()

if(rhexists("/tmp/rhipeTest"))
    rhdel("/tmp/rhipeTest")

rhmkdir("/tmp/rhipeTest")
hdfs.setwd("/tmp/rhipeTest")



   # dummy set of data
permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
    list(x, iris[splits[[x]],])
})

if(rhexists("irisData"))
    rhdel("irisData")
rhwrite(irisSplit, file="irisData")

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

# several parameters of rhwatch to test (readback, mapred, combiner, different input/output formats)

