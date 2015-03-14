## Get environment vars on each node

library(Rhipe)
rhinit()
rhoptions(readback = TRUE)

test.dir <- file.path(rhoptions()$HADOOP.TMP.FOLDER)
hdfs.setwd(test.dir)

# dummy set of data
permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
  list(x, iris[splits[[x]],])
})

if(rhexists("irisData"))
   rhdel("irisData")
rhwrite(irisSplit, file="irisData")

# INSERT ENVIRONMENT VARIABLE NAME HERE!
env.var.name <- "mapred.task.attempt.id"
envMap <- rhmap({
   rhcollect(
         env.var.name,
         Sys.getenv(env.var.name)
      )
})

# OR GET ALL ENV VARS
envMap <- rhmap({
      env.vars = Sys.getenv()
      for (i in 1:length(env.vars)) {
         rhcollect(names(env.vars)[i], env.vars[i])
      }
})

# reduce code for computing max
envReduce <- expression(
   pre = { }, 
   reduce = {
      a <- reduce.key
      rx <- unlist(reduce.values)
      rng <- unique(rx)
   },
   post = {
      rhcollect(reduce.key, rng)
   }
)

# execute the job
res <- rhwatch(
   map = envMap, 
   reduce = envReduce,
   input = "irisData",
   output = "env"
)

#res

variables <- do.call(rbind, lapply(res, function(x) {
   data.frame(var=x[[1]], value=x[[2]], stringsAsFactors=FALSE)
}))
rownames(variables) <- variables$var
