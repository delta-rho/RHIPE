#' Copy files (or folders) on the HDFS
#'
#' Copies the file (or folder) \code{src} on the HDFS to the destination
#' \code{dest} also on the HDFS.
#' 
#' @param ifile Absolute path to be copied on the HDFS or the output from rhwatch(.., read=FALSE).
#' @param ofile Absolute path to place the copies on the HDFS.
#; @param delete should we delete ifile when done?
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}
#' @keywords copy HDFS file
#' @export
rhcp <- function(ifile, ofile,delete=FALSE) {
  ifile = rhabsolute.hdfs.path(rhofolder(ifile))
  ofile = rhabsolute.hdfs.path(ofile)
  ## system(command=paste(paste(Sys.getenv("HADOOP_BIN"), "hadoop",
  ##          sep=.Platform$file.sep), "fs", "-cp", ifile, ofile, sep=" "))
  fu  = .jnew("org/apache/hadoop/fs/FileUtil")
  fs <- rhoptions()$clz$filesystem
  ipath <- .jnew("org/apache/hadoop/fs/Path",ifile)
  opath <- .jnew("org/apache/hadoop/fs/Path",ofile)
  fu$copy(fs,ipath, fs,opath,delete, TRUE, rhoptions()$clz$config)
  ## if(delete)
  ##   rhdel(ifile)
  ## v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhcp",ifile, ofile))
}
