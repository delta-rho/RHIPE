#' Wait For A MapReduce Job
#'
#' This function waits for a MapReduce job to complete before returning.
#' 
#' @param job The parameter \code{job} can either be string with the format
#'   \emph{job_datetime_id} or the value returned from \code{rhex} with the
#'   \code{async} option set to TRUE.
#' @author Saptarshi Guha
#' @note Calling this functions pauses the R console till the MapReduce job indicated
#' by \code{job} is over (successfully or not). The parameter \code{job} can either
#' be string with the format \emph{job_datetime_id} or the value returned from
#' \code{rhex} with the \code{async} option set to TRUE. 
#' @return This function returns
#' the same object as \code{rhex} i.e a list of the results of the job (TRUE or
#' FALSE indicating success or failure) and a counters returned by the job. 
#' @seealso \code{\link{rhstatus}}, \code{\link{rhmr}}, \code{\link{rhkill}},
#'   \code{\link{rhex}}
#' @export
rhjoin <- function(job) {
    verbose <- TRUE  ## DEPRECIATED ARGUMENT NOT EVEN USED IN JAVA ANYMORE
    if (class(job) != "jobtoken" && class(job) != "character") 
       stop("Must give a jobtoken object(as obtained from rhex)")
    if (class(job) == "character") {
       id <- job 
    } else {
       job <- job[[1]]
       id <- job[["job.id"]]
    }
    result <- rhoptions()$server$rhjoin(id, verbose)
    result <- rhuz(result)
    
    result[["jobInfo"]] <- rhJobInfo(id)
    result
}

