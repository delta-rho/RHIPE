## This file contains tests that perform a simple map-reduce job 
## using input from text format rather than sequence file format
context("Simple mr job with text input")

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

tmp.output.name <- "iris_jfkdaligoafjiaeo.csv"
test_that("write mr input data to hdfs", {
   # input data
   write.table(iris, tmp.output.name, sep=",", col.names=FALSE, row.names=FALSE)
   if(rhexists("iris"))
      rhdel("iris")
   rhput("iris_jfkdaligoafjiaeo.csv", file.path(test.dir, "iris.csv"))
   file.remove(tmp.output.name)
})

test_that("run simple mr job with text input", {
    # map code for computing range
    rangeMap <- rhmap(
        expr={
           options(error=function() {
                    dump.frames(to.file=TRUE)
                    stop(paste(geterrmessage(), " (working directory=", getwd(), ")", sep=""))
                    })
           # r is a line from the file
           vals <- strsplit(r, ",")[[1]]
           rhcollect(
              as.character(gsub("\"", "", vals[5])), # Species (remove quotes)
              as.numeric(vals[1]) # Sepal.Length
           )
        }
    )
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
   res <- rhwatch(
      map = rangeMap, 
      reduce = rangeReduce,
      input = rhfmt("text", file.path(test.dir, "iris.csv")),
      output = "irisMaxTmp"
   )
   
   # test the returned value
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

