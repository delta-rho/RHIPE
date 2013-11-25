#' HDFS File Deletion
#'
#' This function deletes the folders contained in the character vector
#' \code{folders} which are located on the HDFS. The deletion is recursive, so
#' all subfolders will be deleted too. Nothing is returned.
#' 
#' @param folder The absolute path on the hdfs to the directory(s) to be
#'   deleted.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhput}}, \code{\link{rhls}},
#'   \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}, \code{\link{rhget}}
#' @keywords delete HDFS directory
#' @export
rhdel <- function(folder){
  folder = rhabsolute.hdfs.path(folder)
  rhoptions()$server$rhdel(folder)
}


# rhdel <- function(fold,ignore.stderr=T,verbose=F){
#    doCMD(rhoptions()$cmd['del'],fold=fold,needout=F,ignore.stderr=ignore.stderr,verbose=verbose)
# }

