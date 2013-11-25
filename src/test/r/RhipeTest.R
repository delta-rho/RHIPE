library(Rhipe)
rhinit()

# set your "working directory" on HDFS to be /user/perk387
#hdfs.setwd("/user/perk387")
hdfs.setwd("/tmp")
#rhoptions(runner = "sh /share/apps/rhipe/rhRunner.sh")

# create a dummy set of data
permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
  list(x, iris[splits[[x]],])
})

# if it is already on HDFS, delete it
#rhdel("irisData")
# write the data to HDFS
rhwrite(irisSplit, file="irisData")

#setup <- expression({
#   .libPaths(“/Users/perk387/R/x86_64-unknown-linux-gnu-library/3.0")
#})

# map code for computing max
maxMap <- rhmap({
  by(r, r$Species, function(x) {
     rhcollect(
        as.character(x$Species[1]),
        max(x$Sepal.Length)
     )
  })
})

# reduce code for computing max
maxReduce <- expression(
  pre = {
     rng <- c(Inf, -Inf)
  }, 
  reduce = {
     a <- reduce.key
     rx <- unlist(reduce.values)
     rng <- c(min(rng[1], rx, na.rm = TRUE), max(rng[2], rx, na.rm =
TRUE))
  }, 
  post = {
     rhcollect(reduce.key, rng)
  }
)

# execute the job
res <- rhwatch(
  map=maxMap, 
  reduce=maxReduce,
  input="irisData",
  output="irisMax"
)


