#' Put a file unto the HDFS
#'
#' Copies the local file called \code{src} (not a folder) to the destination
#' \code{dest} on the HDFS. Uses \code{path.expand} to expand the \code{src}
#' parameter.
#' 
#' @param src Path to the local file to be copied to the HDFS.
#' @param dest Absolute path to the file on the HDFS.  rhput creates the file
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
  x <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhput",path.expand(src),dest,as.logical(deletedest)))
}

# rhput <- function(src,dest,deleteDest=TRUE,ignore.stderr=T,verbose=F){
#   doCMD(rhoptions()$cmd['put'],locals=path.expand(src),dest=dest,overwrite=deleteDest,needoutput=F
#         ,ignore.stderr=ignore.stderr,verbose=verbose)
# ##   doCMD(src,dest,deleteDest,if(is.null(socket)) rhoptions()$socket else socket)
# }

