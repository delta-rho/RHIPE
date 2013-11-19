#' Move files (or folders) on the HDFS (delete original)
#'
#' Copies the file (or folder) \code{src} on the HDFS to the destination
#' \code{dest} also on the HDFS.
#' 
#' @param ifile Absolute path to be copied on the HDFS or the output from rhwatch(.., read=FALSE).
#' @param ofile Absolute path to place the copies on the HDFS.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}
#' @keywords copy HDFS file
#' @export
rhmv <- function(ifile, ofile) {
  rhcp(ifile, ofile,delete=TRUE)
}
