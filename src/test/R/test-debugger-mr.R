options(error=dump.frames)

library(Rhipe)
rhinit()

# set your "working directory" on HDFS to be /user/perk387
hdfs.setwd("/tmp")

# create a dummy set of data
permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
            list(x, iris[splits[[x]],])
        })

# write the data to HDFS
rhwrite(irisSplit, file="iris")

## TODO wrap all map and reduce functions with this
custom.err.handler <- function() {
    dump.frames(to.file=TRUE)
    stop(paste(geterrmessage(), " (working directory=", getwd(), ")", sep=""))
}

setup=expression(map={
        options(error=custom.err.handler)
    },
    reduce={
        options(error=custom.err.handler)    
    })

# map code for computing max
maxMap <- rhmap({
#  errHandler <- getOption(error)
#  options(error=custom.err.handler)
  by(r, r$Speces, function(x) {
     rhcollect(
        as.character(x$Species[1]),
        max(x$Sepal.Length)
     )
#  options(error=errHandler)
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
        setup=setup,
        input="iris",
        output="irisMax"
)
