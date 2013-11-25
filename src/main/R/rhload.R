#' Load an RData from the HDFS.
#'
#' Calls the function load after fetching an RData file from the HDFS.
#' 
#' @param file Path to the .RData file on the HDFS.
#' @param envir Environment in which to load the .RData file.
#' @author Saptarshi Guha
#' @return data from HDFS
#' @seealso \code{\link{rhsave}} , \code{rhsaveimage}
#' @examples
#' 
#' \dontrun{
#' 
#' ## The function is currently defined as
#' function (file, envir = parent.frame()) 
#' {
#'     on.exit({
#'         unlink(x)
#'     })
#'     x <- tempfile(pattern = "rhipe.load")
#'     rhget(file, x)
#'     load(x, envir)
#' }
#' }
#' @export
rhload <- function (file, envir=parent.frame()) 
{
    on.exit({
        unlink(x)
    })
    x <- tempfile(pattern = "rhipe.load")
    rhget(file, x)
    load(x,envir )
}
