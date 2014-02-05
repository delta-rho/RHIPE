options(error=dump.frames)

library(Rhipe)
rhinit()



hdfs.setwd("/tmp")

# create a dummy set of data
permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
            list(x, iris[splits[[x]],])
        })

# if it is already on HDFS, delete it
if(rhexists("iris"))
    del("iris")
    
rhwrite(irisSplit, file="iris")

custom.err.handler <- function() {
    dump.frames(to.file=TRUE)
    stop(paste(geterrmessage(), " (working directory=", getwd(), ")", sep=""))
}

# map code for computing max
maxMap <- rhmap({
  options(error=custom.err.handler)
  by(r, r$Speces, function(x) {
     rhcollect(
        as.character(x$Species[1]),
        max(x$Sepal.Length)
     )
  options(error=errHandler)
  })
})

# reduce code for computing max
maxReduce <- expression(
  pre = {
     rng <- c(Inf, -Inf)
  },
  reduce = {
     stop("This is a test error")
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
        input="iris",
        output="irisMax"
)
