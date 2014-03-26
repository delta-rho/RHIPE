library(Rhipe)

rhinit()

test.dir <- file.path(rhoptions()$HADOOP.TMP.FOLDER, "rhipeTest")

########################################################################
### test HDFS operations
########################################################################

## list files
rhls("/")
# should return a data frame of files in / and associated info

## try to list a file in a directory that doesn't exist
rhls("/directoryThatDoesntExist")
# should return a simple message that the file does not exist without java.io.FileNotFoundException...

## see if a file exists
rhexists("/directoryThatDoesntExist")
# FALSE
rhexists("/tmp")
# TRUE (if you have /tmp set up on your HDFS - I'm assuming you do)

## create a directory (first delete it if it's there)
rhdel(test.dir)
# should be a simple message that it doesn't exist (unless it does of course)

# make the directory
rhmkdir(test.dir)

# what if I do it again?
rhmkdir(test.dir)

# what about nested directories?
rhmkdir("/tmp/tmp/tmp")
# works

# what about creating directories inside of one that we don't have permission
rhmkdir("/tmp/rhipeTest/permissionTest")
rhchmod("/tmp/rhipeTest/permissionTest", "000")
rhls("/tmp/rhipeTest/permissionTest")
# okay - I'm having a hard time simulating directories I don't have access to
# system("hadoop fs -chown bob:bob /tmp/rhipeTest/permissionTest")

## change permissions
rhchmod(test.dir, "711")
# did it work?
ff <- rhls("/tmp")
ff$permission[ff$file == test.dir] == "drwx--x--x"

# give it a garbage permission:
rhchmod(test.dir, "asdf")
# java error

# change permissions of file that doesn't exist
rhchmod("/directoryThatDoesntExist", "777")

# rhexists
rhexists(test.dir)
rhexists("/directoryThatDoesntExist")

## test writing some data using "classic"
kv <- list(list(1, 1), list(2, 2), list(3, 3))
rhwrite(kv, file = "/tmp/rhipeTest/kv_classic", numfiles = 3)
# this creates 3 files:
rhls("/tmp/rhipeTest/kv_classic")
# does data read back appropriately?
kvr <- rhread("/tmp/rhipeTest/kv_classic")
identical(kv, kvr)

# here is a danger: wait for a minute and write to the same location
# but this time put all k/v in just one file:
rhwrite(kv, file = "/tmp/rhipeTest/kv_classic")
rhls("/tmp/rhipeTest/kv_classic")
# the part_1 file gets overwritten with the full 3 k/v pairs
# but part_2 and part_3 still exist
# there should be a warning or an "append=TRUE" or something to rhwrite

## what about writing some invalid data (not key-value pairs)?
kv2 <- as.list(1:3)
rhwrite(kv2, file = "/tmp/rhipeTest/kv_classic_bad")
# good error message

## we can also write data frames with kvpairs=FALSE
# it splits the data frame into 'chunk' rows and makes that chunk the value, with the key being NULL
rhwrite(iris, file = "/tmp/rhipeTest/df_iris", numfiles = 3, kvpairs=FALSE, chunk = 10)
# the result should be 3 files on HDFS, each with k/v pairs of chunks of 
# the data frame, 10 rows per k/v pair
# it would be nice if it would just do this automatically when input is a data frame, no need to specify kvpairs=FALSE
nrow(rhls("/tmp/rhipeTest/df_iris")) == 3
# first k/v pair should match first 10 rows of iris data
identical(rhread("/tmp/rhipeTest/df_iris/part_1")[[1]][[2]], head(iris, 10))

## rhsave (saves R objects to a .Rdata file and moves them to HDFS)
rn <- rnorm(10)
rhsave(rn, file = "/tmp/rhipeTest/rn.Rdata")

# what about this?
rhsave(rn, file = "/directoryThatDoesntExist/rn.Rdata")
# it actually creates the directory
rhdel("/directoryThatDoesntExist")
# depending on your setup, this may have a permissions issue

## rhload (copies .Rdata on HDFS to local, and then reads it in)
rn2 <- rn
rhload("/tmp/rhipeTest/rn.Rdata")
identical(rn, rn2)

test_that("test hdfs.setwd", {
   hdfs.setwd(test.dir)
   ff <- rhls()$file
   expect_true("/tmp/rhipeTest/rn.Rdata" %in% ff)
   # TODO: test all other places rhabsolute.hdfs.path is used
})

test_that("test rhchmod", {
   rhchmod("kv_df", 777)
   ff <- rhls()
   expect_equal(ff$permission[ff$file == "/tmp/rhipeTest/kv_df"], "drwxrwxrwx")
})

# hdfs.setwd, rhmv, rhput, rhget, etc.
rhmkdir("/tmp/rhipeTest/setwdTest")

hdfs.setwd("/directoryThatDoesntExist")
# good error message

## make sure all hdfs operations honor the working directory
hdfs.setwd("/tmp/rhipeTest/setwdTest")

rhwrite(iris, file = "df_iris", numfiles = 3, kvpairs=FALSE, chunk = 10)
rhexists("/tmp/rhipeTest/setwdTest/df_iris")
rhls("df_iris")
tmp <- rhread("df_iris")
rhsave(rn, file = "rn.Rdata")
rhexists("/tmp/rhipeTest/setwdTest/rn.Rdata")
rhload(rn, file = "rn.Rdata")
rhmkdir("tmp")
rhexists("/tmp/rhipeTest/setwdTest/tmp")
rhchmod("tmp", "777")


########################################################################
### simple MR job
########################################################################

rhmkdir("/tmp/rhipeTest/simpleMR")
hdfs.setwd("/tmp/rhipeTest/simpleMR")

permute <- sample(1:150, 150)
splits <- split(permute, rep(1:3, 50))
irisSplit <- lapply(seq_along(splits), function(x) {
  list(x, iris[splits[[x]],])
})

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
# rhmap() creates map expression that gets applied to each key/value pair
# where the current key is available as k and the current value is r

# another way to specify the map:
rangeMap2 <- expression({
   for(i in seq_along(map.keys)) {
      k <- map.keys[[i]]
      r <- map.values[[i]]
      
      by(r, r$Species, function(x) {
         rhcollect(
            as.character(x$Species[1]),
            range(x$Sepal.Length)
         )
      })
   }
})
# in this case, a chunk of key/value pairs are available as map.keys and map.values
# you might choose this if you want to bundle keys or values together before collecting
# for efficiency reasons

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

# read in results and compare to what we think we should get
res <- do.call(rbind, lapply(res, function(x) {
   data.frame(species=x[[1]], min=x[[2]][1], max=x[[2]][2], stringsAsFactors=FALSE)
}))
res <- res[order(res$species),]

resTest <- as.numeric(by(iris, iris$Species, function(x) max(x$Sepal.Length)))

identical(res$max, resTest)

############################################################################
### test MR job with R code error
############################################################################

badMap <- rhmap({
   rhcollect(k, stuff)
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
   map = badMap, 
   reduce = rangeReduce,
   input = "irisData",
   output = "irisMax"
))
# this should give an error: object 'stuff not found'

############################################################################
### MR job with "map" output format
############################################################################

res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduce,
   input = "irisData",
   output = rhfmt("irisMaxMap", type="map"),
   readback = FALSE
))

# this creates a hadoop mapfile irisMaxMap
# mapfile is simply a bunch of indexed sequence files in subdirectories part-r-xxxxx
rhls("irisMaxMap/part-r-00000")

# the 'data' file is the sequence file
# the 'index' file is the index

############################################################################
### reading a mapfile
############################################################################

res <- rhread("irisMaxMap")

# this barfs because it expects a sequence file by default
# but the error message is not indicative of this at all

# this will work:
res <- rhread("irisMaxMap", type="map")

# it seems RHIPE should really be able to automatically determine
# if something is a mapfile

############################################################################
### MR job with output map but readback is TRUE
############################################################################

# this problem is even worse if you run a MR job with mapfile output
# but with readback=TRUE
res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduce,
   input = "irisData",
   output = rhfmt("irisMaxMap", type="map"),
   readback = TRUE
))

# you see the same error as when we tried to read in the result above
# here, since readback=TRUE, it tries to read the result in
# but the result is a mapfile and it doesn't know that
# this is not good - especially because the error message gives you no 
# indication that this is what happened

############################################################################
### MR job with mapfile as input, but that fact is not specified
############################################################################

# let's use irisMaxMap as an input for an "identity" MR job
# but I don't specify that the input is a mapfile

res <- try(rhwatch(
   map = rhmap({rhcollect(k, r)}), 
   input = "irisMaxMap"
))

# the map is simply passing the keys and values to the reduce
# there is no reduce, which means an identity reduce is run

# the appropriate way to specify input here would be
# input = rhfmt("irisMaxMap", type="map")

# since it doesn't know this is a mapfile, it tries to input both the data and index
# files, but the index files are not valid input
# this is the error you get:
# java.lang.ClassCastException: org.apache.hadoop.io.LongWritable cannot be cast to org.godhuli.rhipe.RHBytesWritable, etc.
# this is not helpful to the user at all

# all of this could be fixed if we automatically detect map and sequence files

############################################################################
### test with a bad runner (common source of error)
############################################################################

goodRunner <- rhoptions()$runner # first save the good runner
rhoptions(runner="R CMD /Users/hafe647/Rlibs/Rhipe/bin/blah")

## test with combiner=TRUE
res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduce,
   input = "irisData",
   output = "irisMax"
))
# should get something like this:
# ---------------------------------
# There were R errors, showing 30:
# 
# 1(1): subprocess failed with code: 126Autokill is true and terminating
# ...

# this error is not very informative, and this is a common problem

# should we check that the runner is legit?
# this will run the runner and see what happens
a <- system(rhoptions()$runner, intern=TRUE)

# another common problem is RhipeMapReduce shared library dependencies not 
# being available - we can check that with this:
# ldd /Users/hafe647/Rlibs/Rhipe/bin/RhipeMapReduce
# this is difficult to replicate

# another common problem is permissions (mapred user does not have permissions)

# set the runner back to what it was before so we can go on...
rhoptions(runner=goodRunner)


############################################################################
### test MR job with readback = TRUE
############################################################################

res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduce,
   readback = TRUE,
   input = "irisData",
   output = "irisMax"
))

# read in results and compare to what we think we should get
res <- do.call(rbind, lapply(res, function(x) {
   data.frame(species=x[[1]], min=x[[2]][1], max=x[[2]][2], stringsAsFactors=FALSE)
}))
res <- res[order(res$species),]
identical(res$max, resTest)


############################################################################
### test MR job with a combiner
############################################################################

# combiner runs reduce on the map side to help reduce the size of the data
# prior to sending to reduce
# in this case, reduce needs to be associative

res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduce,
   readback = TRUE,
   input = "irisData",
   combiner = TRUE,
   output = "irisMax"
))

## now try a non-associative reduce:

rangeReduceBad <- expression(
   pre = {
      rng <- c(Inf, -Inf)
   }, 
   reduce = {
      a <- reduce.key
      rx <- unlist(reduce.values)
      rng <- c(min(rng[1], rx, na.rm = TRUE), max(rng[2], rx, na.rm = TRUE))
   },
   post = {
      rhcollect(reduce.key, data.frame(min=rng[1], max=rng[2]))
   }
)

res <- try(rhwatch(
   map = rangeMap, 
   reduce = rangeReduceBad,
   readback = TRUE,
   input = "irisData",
   combiner = TRUE,
   output = "irisMax"
))

# this runs fine but shouldn't:
# k/v pairs returned by reduce applied at combiner stage are not compatible
# as inputs to the real reduce - this must mean combiner is not run
# maybe because the data is too small?

############################################################################
### test MR job with custom mapred options
############################################################################



############################################################################
### test MR job with text input
############################################################################



