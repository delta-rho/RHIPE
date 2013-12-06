context("HDFS operations check")

test_that("test rhinit", {
   rhinit()
})

test_that("remove /tmp/rhipeTest", {
   if(rhexists("/tmp/rhipeTest"))
      rhdel("/tmp/rhipeTest")
})

test_that("test rhmkdir", {
   rhmkdirRes <- try(rhmkdir("/tmp/rhipeTest"))
   expect_true(!inherits(rhmkdirRes, "try-error"),
      label = "rhmkdir ran without error")
   
   expect_true(rhmkdirRes, 
      label = "hdfs directory successfully created")
   
   expect_true(rhexists("/tmp/rhipeTest"), 
      label = "directory listing contains directory created by rhmkdir")
})

test_that("first rhls test / test rhls on empty directory", {
   rhlsRes <- try(rhls("/tmp/rhipeTest"))
   expect_true(!inherits(rhlsRes, "try-error"),
      label = "rhls ran without error")
   
   expect_equivalent(
      names(rhlsRes), 
      c("permission", "owner", "group", "size", "modtime", "file"),
      label = "column names for data frame returned by rhls")
   
   expect_equal(nrow(rhlsRes), 0)
})

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

rn <- rnorm(10)

test_that("test rhsave", {
   rhsave(rn, file = "/tmp/rhipeTest/rn.Rdata")
   expect_true(rhexists("/tmp/rhipeTest/rn.Rdata"))
})

test_that("test rhload", {
   rn2 <- rn
   rhload("/tmp/rhipeTest/rn.Rdata")
   expect_equivalent(rn, rn2)
})

test_that("test hdfs.setwd", {
   hdfs.setwd("/tmp/rhipeTest")
   ff <- rhls()$file
   expect_true("/tmp/rhipeTest/rn.Rdata" %in% ff)
   # TODO: test all other places rhabsolute.hdfs.path is used
})

test_that("test rhchmod", {
   rhchmod("kv_df", 777)
   ff <- rhls()
   expect_equal(ff$permission[ff$file == "/tmp/rhipeTest/kv_df"], "drwxrwxrwx")
})

# rhmv

