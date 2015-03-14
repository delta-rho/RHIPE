context("Test serialization and de-serialization")

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

test_that("serialization and de-serialization in R memory", {
   # dummy set of data
   permute <- sample(1:150, 150)
   splits <- split(permute, rep(1:3, 50))
   irisSplit <- lapply(seq_along(splits), function(x) {
     list(x, iris[splits[[x]],])
   })
   sample.string <- "this is a test of the serialization and de-serialization functions in Rhipe"
   
   all.data <- list(iris, irisSplit, sample.string)
   
   serialized <- rhsz(all.data)
   
   unserialized <- rhuz(serialized)
   
   expect_equivalent(all.data, unserialized)
})
   
test_that("deserialize data serialized in java", {
   #de-serialize something serialized in Java
   java.opts <- rhuz(rhoptions()$server$rhmropts())
   expect_true(data.class(java.opts) == "list")
   expect_true(all(lapply(java.opts, FUN=data.class) == "character"))  
})

test_that("use rhwrite/rhread which use serialization", {
    # dummy set of data
    permute <- sample(1:150, 150)
    splits <- split(permute, rep(1:3, 50))
    irisSplit <- lapply(seq_along(splits), function(x) {
      list(x, iris[splits[[x]],])
    })
   rhwrite(irisSplit, "serializationTest")
   new.data <- rhread("serializationTest")
   
   expect_equivalent(irisSplit, new.data)
})
