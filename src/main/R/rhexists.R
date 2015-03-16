#' Check if File Exists on HDFS
#' 
#' Check if file exists on HDFS
#' 
#' @param path is the location of the file to be checked. It is expanded using \code{\link{rhabsolute.hdfs.path}}
#' @author Ryan Hafen
#' @seealso \code{\link{rhls}}, \code{\link{rhwrite}}, \code{\link{rhmkdir}}
#' @export
rhexists <- function(path) {
   path <- rhabsolute.hdfs.path(path)
   path <- .jnew("org/apache/hadoop/fs/Path", path)
   fs <- path$getFileSystem(rhoptions()$clz$config)
   fs$exists(path)
}
 
