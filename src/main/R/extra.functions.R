
mkdtemp <- function(indir) {
   .Call("createTempDir", sprintf("%s/rhipedir-XXXXXX", indir), PACKAGE = "Rhipe")
}


initPRNG <- function(seed = NULL) {
   seed <- eval(seed)
   mi <- function() {
      getUID <- function(id = Sys.getenv("mapred.task.id")) {
         if (id == "") { id <- Sys.getenv("mapreduce.task.id") }
         a <- strsplit(id, "_")[[1]]
         a <- as.numeric(a[c(2, 3, 5)])
      }
      library(parallel)
      RNGkind("L'Ecuyer-CMRG")
      if (!is.null(iseed)) 
         set.seed(iseed)
      current.task.number <- getUID()[3]
      seed <- .Random.seed
      if (current.task.number > 1) {
         for (i in 2:current.task.number) seed <- nextRNGStream(seed)
      }
      RNGkind("default")
      assign(".Random.seed", seed, envir = .GlobalEnv)
   }
   e1 <- new.env(parent = .BaseNamespaceEnv)
   assign("iseed", seed, envir = e1)
   environment(mi) <- e1
   mi
}

parseJobIDFromTracking = function(results){
     results$jobid
}


#' Reads all or some lines from a text file located on the HDFS
#' @param inp The location of the text file, interolated based on hdfs.getwd
#' @param l the number of lines to read
#' @keywords HDFS TextFile
#' @export
hdfsReadLines <- function(inp, l = -1L) {
   rhoptions()$server$readTextFile(rhabsolute.hdfs.path(inp), as.integer(l))
}


getypes <- function(files, type, skip) {
   type <- match.arg(type, c("sequence", "map", "text", "gzip", "index"))
   files <- switch(type, text = {
      unclass(rhls(files)["file"])$file
   }, gzip = {
      uu <- unclass(rhls(files)["file"])$file
      uu[grep("gz$", uu)]
   }, sequence = {
      unclass(rhls(files)["file"])$file
   }, map = {
      uu <- unclass(rhls(files, recurse = TRUE)["file"])$file
      uu[grep("data$", uu)]
   }, index = {
      uu <- unclass(rhls(files, recurse = TRUE)["file"])$file
      uu[grep("data$", uu)]
   })
   for (s in skip) {
      remr <- c(grep(s, files))
      if (length(remr) > 0) 
         files <- files[-remr]
   }
   return(files)
}



## When a function is passed,use this to convert into expression appropiately
rhmap2 <- function(J) {
   j <- expression({
      result <- mapply(rhipe_inner_runner, map.keys, map.values, SIMPLIFY = FALSE)
   })
   environment(j) <- .BaseNamespaceEnv
   class(j) <- c(class(j), "rhmr-map2")
   j
} 
