## This file contains a set of tests of the HDFS (file and directory) 
## related functions in Rhipe. 

context("HDFS operations check")

## TEST: rhinit

test_that("test rhinit", {
   rhinit()
})

## TEST: hdfs.getwd

test_that("test hdfs.getwd", {
   hdfs.wd <- hdfs.getwd()
})

## TEST: rhmkdir
test.dir <- file.path(rhoptions()$HADOOP.TMP.FOLDER, "rhipeTest")

test_that("test rhmkdir", {
   rhmkdirRes <- try(rhmkdir(test.dirtest.dir))
   expect_true(!inherits(rhmkdirRes, "try-error"),
      label = "rhmkdir ran without error")
   
   expect_true(rhmkdirRes, 
      label = "hdfs directory successfully created")
   
   expect_true(rhexists(test.dir), 
      label = "directory listing contains directory created by rhmkdir")
})

## TEST: rhls

test_that("first rhls test / test rhls on empty directory", {
   rhlsRes <- try(rhls(test.dir))
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

test_that("set HDFS working dir to /tmp/rhipeTest", {
    hdfs.setwd(test.dir)
    expect_equal(hdfs.getwd(), test.dir)
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
      rhwrite(kv, file = "/tmp/rhipeTest/kv_classic", numfiles = 10))
   expect_true(!inherits(rhwriteRes, "try-error"),
      label = "rhwrite classic ran without error")
   
   # TODO: test chunk parameter
   
   expect_equal(nrow(rhls("/tmp/rhipeTest/kv_classic")), 4)
})

test_that("test rhread classic", {
   expect_equivalent(rhread("/tmp/rhipeTest/kv_classic"), kv)
})

test_that("attempt to write some invalid data (not key-value pairs)", {
    kv2 <- as.list(1:3)
    expect_error(rhwrite(kv2, file = "/tmp/rhipeTest/kv_classic_bad"), 
        regex="You requested 'classic' write")
})

## TEST: rhread/rhwrite data.frame
test_that("test rhwrite data frame", {
   rhwriteRes <- try(
      rhwrite(iris, file = "/tmp/rhipeTest/kv_df", chunk = 10, numfiles=3, kvpairs=FALSE))
   expect_true(!inherits(rhwriteRes, "try-error"),
      label = "rhwrite data frame ran without error")
   
   expect_equal(nrow(rhls("/tmp/rhipeTest/kv_df")), 3)
})

test_that("test rhread data frame", {
   expect_equivalent(rhread("/tmp/rhipeTest/kv_df")[[1]][[2]], head(iris, 10))
})

## TEST: rhsave

rn <- rnorm(10)

test_that("test rhsave", {
   rhsave(rn, file = "/tmp/rhipeTest/rn.Rdata")
   expect_true(rhexists("/tmp/rhipeTest/rn.Rdata"))
})

## TEST: rhload

test_that("test rhload", {
   rn2 <- rn
   rhload("/tmp/rhipeTest/rn.Rdata")
   expect_equivalent(rn, rn2)
})

test_that("attempt to rhload nonexistent file", {
   expect_error(rhload("/tmp/rhipeTest/rjfjfk.Rdata"), 
       regex="does not exist")
})

## TEST: rhcp

test_that("test rhcp (delete=FALSE)", {
    if (rhexists("/tmp/rhipeTest/rn2.Rdata")) {rhdel("/tmp/rhipeTest/rn2.Rdata")}
    expect_true(rhcp("/tmp/rhipeTest/rn.Rdata", "/tmp/rhipeTest/rn2.Rdata"))
    expect_true("/tmp/rhipeTest/rn2.Rdata" %in% rhls()$file)
})

test_that("test rhcp (delete=TRUE)", {
    expect_true(rhcp("/tmp/rhipeTest/rn2.Rdata", "/tmp/rhipeTest/rn3.Rdata", delete=TRUE))
    expect_false("/tmp/rhipeTest/rn2.Rdata" %in% rhls()$file)
    expect_true("/tmp/rhipeTest/rn3.Rdata" %in% rhls()$file)
})

## TEST: rhmv

test_that("test rhmv", {
    expect_true(rhcp("/tmp/rhipeTest/rn3.Rdata", "/tmp/rhipeTest/rn2.Rdata", delete=TRUE))
    expect_false("/tmp/rhipeTest/rn3.Rdata" %in% rhls()$file)
    expect_true("/tmp/rhipeTest/rn2.Rdata" %in% rhls()$file)
})

## TEST: rhchmod

test_that("test rhchmod", {
   rhchmod("kv_df", 777)
   ff <- rhls()
   expect_equal(ff$permission[ff$file == "/tmp/rhipeTest/kv_df"], "drwxrwxrwx")
})

test_that("test garbage file permission in rhchmod", {
    expect_error(rhchmod(test.dir, "asdf"), 
        regex="java.lang.IllegalArgumentException: asdf")
})

test_that("change permissions of a file that doesn't exist", {
    expect_error(rhchmod("/directoryThatDoesntExist", "777"), 
        regex="No such file or directory")
})

## TEST: hdfs.absolute.path

test_that("test rhabsolute.hdfs.path on relative path", {
    expect_equal(rhabsolute.hdfs.path("rn.Rdata"), paste(hdfs.getwd(), "rn.Rdata", sep="/"))
})

test_that("test rhabsolute.hdfs.path on absolute path", {
    expect_equal(rhabsolute.hdfs.path("/tmp/rhipeTest/rn.Rdata"), "/tmp/rhipeTest/rn.Rdata")
})

## TEST: rhsave.image

test_that("test rhsave.image", {
    rhsave.image(file="hdfs.test.save.Rdata")
    expect_true(paste(hdfs.getwd(), "hdfs.test.save.Rdata", sep="/") %in% rhls()$file)
})

## TEST: rhput

test_that("test rhput", {
    cat("test file\n2\n3\n4", file="test.txt")
    rhput(file.path(getwd(), "test.txt"), "/tmp/rhipeTest/test.txt")
    expect_true("/tmp/rhipeTest/test.txt" %in% rhls()$file)
})

test_that("test rhput (delete=TRUE)", {
    cat("test file #2\n2\n3\n4\n", file="test.txt")
    rhput(file.path(getwd(), "test.txt"), "/tmp/rhipeTest/test.txt", delete=TRUE)
    expect_true("/tmp/rhipeTest/test.txt" %in% rhls()$file)
})
invisible(file.remove("test.txt")) ##remove non hdfs file

## TEST: hdfsReadLines

test_that("test hdfsReadLines", {
    file.lines <- hdfsReadLines("/tmp/rhipeTest/test.txt")
    expect_true(length(file.lines) == 4)
    expect_true(file.lines[1] == "test file #2")
})

test_that("test hdfsReadLines on file that doesn't exist", {
    expect_error(hdfsReadLines("/tmp/rhipeTest/test2.txt"), 
        regex="java.io.FileNotFoundException")
})

## TEST: rhget

test_that("test rhget", {
    rhget("/tmp/rhipeTest/test.txt", file.path(getwd(), "test_from_hdfs.txt"))
    expect_true("test_from_hdfs.txt" %in% list.files())
    file.lines <- readLines("test_from_hdfs.txt")
    expect_true(length(file.lines) == 4)
    expect_true(file.lines[1] == "test file #2")
})

test_that("test rhget on non-existent file", {
    expect_error(rhget("/tmp/rhipeTest/thisdoesnotexist", file.path(getwd(), "tmp.txt")),
        regex="does not exist")
})

## TEST: rhdel

test_that("rhdel hdfs.test.save.Rdata", {
    rhdel("hdfs.test.save.Rdata")
    expect_false(paste(hdfs.getwd(), "hdfs.test.save.Rdata", sep="/") %in% rhls()$file)
})

test_that("remove /tmp/rhipeTest", {
   if(rhexists(test.dir))
      rhdel(test.dir)
})

## TEST: rhoptions()$HADOOP.TMP.FOLDER

test_that("test if rhoptions()$HADOOP.TMP.FOLDER exists and is writable", {
    expect_true(rhexists(rhoptions()$HADOOP.TMP.FOLDER), 
        label="rhoptions()$HADOOP.TMP.FOLDER does not exist in HDFS space")
    expect_true(rhcp("/tmp/rhipeTest/rn.Rdata", 
        paste(rhoptions()$HADOOP.TMP.FOLDER, "rn.Rdata", sep="/")),
        label="rhoptions()$HADOOP.TMP.FOLDER in HDFS space is not writable")
    expect_true(paste(rhoptions()$HADOOP.TMP.FOLDER, "rn.Rdata", sep="/") 
        %in% rhls(rhoptions()$HADOOP.TMP.FOLDER)$file)
    rhdel(paste(rhoptions()$HADOOP.TMP.FOLDER, "rn.Rdata", sep="/"))
})
## NOTE: if rhoptions()$HADOOP.TMP.FOLDER does not exist in HDFS space or is not 
## writable, the user should change the directory (after calling rhinit() and 
## before any map-reduce jobs) using 
## rhoptions(HADOOP.TMP.FOLDER="someDirectoryThatExists")

## TEST: rhclean

test_that("test rhclean", {
    expect_true(rhcp("/tmp/rhipeTest/rn.Rdata", paste(rhoptions()$HADOOP.TMP.FOLDER, "rhipe-temp-jdflajg.Rdata", sep="/")))
    rhclean()
    expect_false(paste(rhoptions()$HADOOP.TMP.FOLDER, "rhipe-temp-jdflajg.Rdata", sep="/") %in% rhls(rhoptions()$HADOOP.TMP.FOLDER)$file)
})
