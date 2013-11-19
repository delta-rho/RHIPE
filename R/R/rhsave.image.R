#' save.image to HDFS
#'
#' Puts the result of a \code{save.image} call unto the HDFS.  Useful if you
#' have variables in the current environment you want to work with in a
#' MapReduce as a shared object.
#' 
#' @param \ldots additional parameters for \code{save.image}
#' @param file Path to file on HDFS.  Creates the file or overwrites
#'   it.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhsave}} , \code{\link{rhload}}
#' @export
rhsave.image <- function(...,file){
	file=rhabsolute.hdfs.path(file)
  on.exit({unlink(x)})
  x <- tempfile(pattern='rhipe.save')
  save.image(file=x,...)
  rhput(src=x,dest=file)
}
