#' Set HDFS Working Directory
#'
#' Sets a working directory for all Rhipe commands that use the HDFS.
#' By default hdfs.working.dir is set to '/'.  All input directories to
#' all Rhipe commands may omit a leading '/' and indicate they are off of
#' the current hdfs.working.dir. 
#' 
#' This is anagolous to how file operations
#' in R work off the current working directory.
# 
#'  
#' Using working directories with relative paths for input and output
#' helps users make more portable scripts.
#' 
#' @param path Path to set working directory.  May be relative the current 
#' HDFS working directory or absolute.
#' @param check.valid.hdfs Optional.  By default hdfs.setwd checks to see
#' if the HDFS directory exist by trying to list files in it.  If it doesn't appear to
#' exist it doesn't set the working directory.  If this check seems cumbersome to you,
#' you may disable it by setting this to FALSE.
#' @author Jeremiah Rounds
#' @seealso \code{\link{hdfs.getwd}}
#' @examples
#' 
#' \dontrun{
#' #DOES NOT RUN.  JUST ILLUSTRATING CONCEPTS.
#' hdfs.setwd('/tmp')
#' #this would output to /tmp/test/something.out
#' rhwatch(map = map, reduce = reduce, input = N, output = "test/something.out") #assume map expression and reduce expression have been defined
#' rhread('test/something.out',type='sequence')  #reads a sequence file in /tmp/test/something.out
#' this is NOT relative thus it reads from /test/something.out
#' rhread('/test/something.out',type='sequence') 
#' }
#' @export

hdfs.setwd <- function(path, check.valid.hdfs = TRUE) {
   path <- rhabsolute.hdfs.path(path)
   if (check.valid.hdfs) {
      # safety first.  See if the directory exist reusing a list.  Is there a better
      # command that doesn't have the overhead of returning files?
      x <- NULL
      try({
        newPath <- .jnew("org/apache/hadoop/fs/Path", path)
        fs <- newPath$getFileSystem( rhoptions()$clz$config)
        x <- fs$exists(newPath)
      }, silent = TRUE)
      if (!x) 
         stop("Invalid HDFS path.")
   }
   rhoptions(hdfs.working.dir = path)
} 
