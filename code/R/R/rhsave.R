#' Save .RData to HDFS
#'
#' Puts the result of a \code{save} call unto the HDFS.  Useful if you have
#' variables in the current environment you want to work with in a MapReduce as
#' a shared object.
#' 
#' @param \ldots additional parameters for \code{rhsave}
#' @param file Absolute path to file on HDFS.  Creates the file or overwrites
#' @param environment to search for objects to be saved
#'   it.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{rhsave.image}, \code{\link{rhload}}
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
#'     save(file = x, ...)
#'     rhput(src = x, dest = file)
#'   }
#' }
#' @export
rhsave <- function (..., file, envir=parent.frame()) {
  on.exit({
    unlink(x)
  })
  x <- tempfile(pattern = "rhipe.save")
  save(..., file = x, envir=envir)
  rhput(src = x, dest = file)
}
