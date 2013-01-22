#' Put a file unto the HDFS
#'
#' Copies the local file called \code{src} (not a folder) to the destination
#' \code{dest} on the HDFS. Uses \code{path.expand} to expand the \code{src}
#' parameter.
#' 
#' @param src Path to the local file to be copied to the HDFS.
#' @param dest Path to the file on the HDFS.  rhput creates the file
#'   at dest.
#' @param deletedest If TRUE this function attempts to delete the destination of the HDFS before trying to copy to that location on the HDFS.
#' @author Saptarshi Guha
#' @return NULL
#' @note Local filesystem copy remains after the operation is complete.
#' @seealso \code{\link{rhget}}, \code{\link{rhdel}},
#'   \code{\link{rhread}}, \code{\link{rhwrite}}, \code{\link{rhsave}}
#' @keywords put HDFS file
#' @export
rhput <- function(src, dest,deletedest=TRUE){
  dest = rhabsolute.hdfs.path(dest)
  y <- as.logical(deletedest)
  Y <- if(is.na(y) || y==FALSE) FALSE else TRUE
  rhoptions()$server$rhput(path.expand(src),dest,y)
}

