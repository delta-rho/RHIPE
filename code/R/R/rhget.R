#' Copying from the HDFS
#' Moves files from the HDFS to a local directory.
#' 
#' Copies the files (or folder) at \code{src}, located on the HDFS to the
#' destination \code{dest} located on the local filesystem. If a file or folder
#' of the same name as \code{dest} exists on the local filesystem, it will be
#' deleted. The \code{dest} can contain ~ which will be expanded. The original
#' copy of the file or folder is left on the HDFS after this operation.
#' 
#' @param src Absolute path to file or directory on HDFS to get.
#' @param dest Path to file or directory on local filesystem to create as the
#'   new copy.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhput}}, \code{\link{rhdel}},
#'   \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}
#' @keywords get HDFS directory
#' @export
rhget <- function(src, dest){
  x <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhget",src,path.expand(dest)))
}

# rhget <- function(src,dest,ignore.stderr=T,verbose=F){
#   ## Copies src to dest
#   ## If src is a directory and dest exists,
#   ## src is copied inside dest(i.e a folder inside dest)
#   ## If not, src's contents is copied to a new folder called dest
#   ##
#   ## If source is a file, and dest exists as a dire
#   ## source is copied inside dest
#   ## If dest does not exits, it is copied to that file
#   ## Wildcards allowed
#   ## OVERWRITES!
#   doCMD(rhoptions()$cmd['get'],src=src,dest=dest,needout=F,ignore.stderr=ignore.stderr,verbose=verbose)
# ##   doGet(src,dest,if(is.null(socket)) rhoptions()$socket else socket)
# }
