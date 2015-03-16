## TEST: local mode

# use /tmp instead of tempdir() because it can give weird results
# should be fine since any system should be linux

ltest.dir <- "/tmp/_rhipe_test"
ltest.dir <- paste("file://", ltest.dir, "/", sep = "")

## TEST: rhmkdir
if(rhexists(ltest.dir))
  rhdel(ltest.dir)

test_that("test rhmkdir", {
   rhmkdirRes <- try(rhmkdir(ltest.dir))
   expect_true(!inherits(rhmkdirRes, "try-error"),
      label = "rhmkdir ran without error")

   expect_true(rhmkdirRes,
      label = "hdfs directory successfully created")

   expect_true(rhexists(ltest.dir),
      label = "directory listing contains directory created by rhmkdir")
})

## TEST: rhls

test_that("first rhls test / test rhls on empty directory", {
   rhlsRes <- try(rhls(ltest.dir))
   expect_true(!inherits(rhlsRes, "try-error"),
      label = "rhls ran without error")

   expect_equivalent(
      names(rhlsRes),
      c("permission", "owner", "group", "size", "modtime", "file"),
      label = "column names for data frame returned by rhls")

   expect_equal(nrow(rhlsRes), 0)
})

test_that("try to list a directory that doesn't exist", {
    expect_error(rhls("/directoryThatDoesntExist"),
    regex="java.io.FileNotFoundException: Cannot access /directoryThatDoesntExist")
})

## TEST: hdfs.setwd

test_that("set HDFS working dir to rhoptions()$HADOOP.TMP.FOLDER/rhipeTest", {
    hdfs.setwd(ltest.dir)
    expect_equal(hdfs.getwd(), ltest.dir)
})

test_that("set HDFS working dir to dir that doesn't exist", {
    expect_error(hdfs.setwd("/directoryThatDoesntExist"),
        regex="Invalid HDFS path")
})

## TEST: rhexists

test_that("see if file exists", {
    expect_false(rhexists("/directoryThatDoesntExist"))
})

## TEST: rhread/rhwrite classic

kv <- list(list(1, 1), list(2, 2), list(3, 3))

test_that("test rhwrite classic", {
   rhwriteRes <- try(
      rhwrite(kv, file = file.path(ltest.dir, "kv_classic"), numfiles = 10))
   expect_true(!inherits(rhwriteRes, "try-error"),
      label = "rhwrite classic ran without error")

   expect_equal(nrow(rhls(file.path(ltest.dir, "kv_classic"))), 4)
})

test_that("test rhread classic", {
   expect_equivalent(rhread(file.path(ltest.dir, "kv_classic")), kv)
})

test_that("attempt to write some invalid data (not key-value pairs)", {
    kv2 <- as.list(1:3)
    expect_error(rhwrite(kv2, file = file.path(ltest.dir, "kv_classic_bad")),
        regex="You requested 'classic' write")
})

## TEST: rhread/rhwrite data.frame
test_that("test rhwrite data frame", {
   rhwriteRes <- try(
      rhwrite(iris, file = file.path(ltest.dir, "kv_df"), chunk = 10, numfiles=3, kvpairs=FALSE))
   expect_true(!inherits(rhwriteRes, "try-error"),
      label = "rhwrite data frame ran without error")

   expect_equal(nrow(rhls(file.path(ltest.dir, "kv_df"))), 3)
})

test_that("test rhread data frame", {
   expect_equivalent(rhread(file.path(ltest.dir, "kv_df"))[[1]][[2]], head(iris, 10))
})

## TEST: rhsave

rn <- rnorm(10)

test_that("test rhsave", {
   rhsave(rn, file = file.path(ltest.dir, "rn.Rdata"))
   expect_true(rhexists(file.path(ltest.dir, "rn.Rdata")))
})

## TEST: rhload

test_that("test rhload", {
   rn2 <- rn
   rhload(file.path(ltest.dir, "rn.Rdata"))
   expect_equivalent(rn, rn2)
})

test_that("attempt to rhload nonexistent file", {
   expect_error(rhload(file.path(ltest.dir, "rjfjfk.Rdata")),
       regex="does not exist")
})

## TEST: rhcp

test_that("test rhcp (delete=FALSE)", {
    if (rhexists(file.path(ltest.dir, "rn2.Rdata"))) { rhdel(file.path(ltest.dir, "rn2.Rdata")) }
    expect_true(rhcp(file.path(ltest.dir, "rn.Rdata"), file.path(ltest.dir, "rn2.Rdata")))
    expect_true(paste(ltest.dir, "rn2.Rdata", sep = "") %in% rhls()$file)
})

test_that("test rhcp (delete=TRUE)", {
    expect_true(rhcp(file.path(ltest.dir, "rn2.Rdata"), file.path(ltest.dir, "rn3.Rdata"), delete=TRUE))
    expect_false(paste(ltest.dir, "rn2.Rdata", sep = "") %in% rhls()$file)
    expect_true(paste(ltest.dir, "rn3.Rdata", sep = "") %in% rhls()$file)
})

## TEST: rhmv

test_that("test rhmv", {
    expect_true(rhcp(file.path(ltest.dir, "rn3.Rdata"), file.path(ltest.dir, "rn2.Rdata"), delete=TRUE))
    expect_false(paste(ltest.dir, "rn3.Rdata", sep = "") %in% rhls()$file)
    expect_true(paste(ltest.dir, "rn2.Rdata", sep = "") %in% rhls()$file)
})

## TEST: rhchmod

test_that("test rhchmod", {
   rhchmod("kv_df", 777)
   ff <- rhls()
   expect_equal(ff$permission[ff$file == paste(ltest.dir, "kv_df", sep = "")], "drwxrwxrwx")
})

test_that("test garbage file permission in rhchmod", {
    expect_error(rhchmod(ltest.dir, "asdf"),
        regex="java.lang.IllegalArgumentException: asdf")
})

test_that("change permissions of a file that doesn't exist", {
    expect_error(rhchmod("/directoryThatDoesntExist", "777"),
      regex="java.io.FileNotFoundException")
      # regex="java.io.FileNotFoundException")
})







