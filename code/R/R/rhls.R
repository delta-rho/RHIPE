#' List Files On HDFS
#'
#' List all files and directories contained in a directory on the HDFS.
#' 
#' @param folder  Path of directory on HDFS or output from rhmr or rhwatch(read=FALSE)
#' @param recurse If TRUE list all files and directories in sub-directories.
#' @author Saptarshi Guha
#' @details Returns a data.frame of filesystem information for the files located
#'   at \code{path}. If \code{recurse} is TRUE, \code{rhls} will recursively
#'   travel the directory tree rooted at \code{path}. The returned object is a
#'   data.frame consisting of the columns: \emph{permission, owner, group, size
#'   (which is numeric), modification time}, and the \emph{file name}.
#'   \code{path} may optionally end in `*' which is the wildcard and will match
#'   any character(s).
#' @return vector of file and directory names
#' @seealso \code{\link{rhput}}, \code{\link{rhdel}},
#'   \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}, \code{\link{rhget}}
#' @keywords list HDFS directory
#' @export
rhls <- function(folder=NULL,recurse=FALSE){
	## List of files,
  if( is(folder,"rhmr") || is(folder, "rhwatch"))
    folder <- rhofolder(folder)
  if(is.null(folder))
    folder <- hdfs.getwd()
  folder <- rhabsolute.hdfs.path(folder)
  v <- rhoptions()$server$rhls(folder, if(recurse) 1L else 0L)
  v <- rhuz(v)
  if(is.null(v)) return(NULL)
                                        #condition nothing in the directory?
  if(length(v) == 0){
    f = as.data.frame(matrix(NA,0,6))
  } else {
    f <- as.data.frame(do.call("rbind",sapply(v,strsplit,"\t")),stringsAsFactors=F)
  }
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f$size <- as.numeric(f$size)
  unique(f)
}

