#' save.image to HDFS
#'
#' Puts the result of a \code{save.image} call unto the HDFS.  Useful if you
#' have variables in the current environment you want to work with in a
#' MapReduce as a shared object.
#' 
#' @param \ldots additional parameters for \code{save.image}
#' @param file Absolute path to file on HDFS.  Creates the file or overwrites
#'   it.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhsave}} , \code{\link{rhload}}
#' @examples
#' 
#' \dontrun{
#' 
#' ## The function is currently defined as
#' function (..., file) 
#' {
#'     on.exit({
#'         unlink(x)
#'     })
#'     x <- tempfile(pattern = "rhipe.save")
#'     save.image(file = x, ...)
#'     rhput(src = x, dest = file)
#'  }
#' 
#' }
#' @export
rhsave.image <- function(...,file){
  on.exit({unlink(x)})
  x <- tempfile(pattern='rhipe.save')
  save.image(file=x,...)
  rhput(src=x,dest=file)
}