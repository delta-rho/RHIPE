#' Returns the output folder from a previous RHIPE job
#'
#' Returns the output folder from a previous RHIPE job
#' Take a look at \code{\link{hdfs.getwd}} for more information.
#' @param job Can be a character indicating the string to a folder,
#' or the result of call to rhmr or rhwatch. For the latter,
#' this works only read is FALSE (because if read is TRUE, the output is returned)
#' @author Saptarshi Guha
#' @export
rhofolder <- function(job){
  if(is.character(job)) return (job)
  if(is(job,'rhmr'))
    return (job[[1]]$rhipe_output_folder)
  if(is(job,"rhwatch"))
    return(job[[2]][[1]]$rhipe_output_folder)
  stop("Not a valid object, should be rhmr, rhwatch or character")
}
