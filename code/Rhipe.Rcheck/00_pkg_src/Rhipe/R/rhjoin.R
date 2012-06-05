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
rhjoin <- function(job){
	verbose=TRUE ## DEPRECIATED ARGUMENT NOT EVEN USED IN JAVA ANYMORE
  if(class(job)!="jobtoken" && class(job)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(job)=="character") id <- job else {
    job <- job[[1]]
    id <- job[['job.id']]
  }
  result <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhjoin", list(id,
                                                                          needoutput=as.character(TRUE),
                                                                          joinwordy = as.character(as.logical(verbose)))))[[1]]
  if(length(job)==2){
    ## from rhlapply
    return(job[[2]]())
  }
  return(    list(result=result[[1]], counters=result[[2]]))
}

# rhjoin <- function(x,verbose=TRUE,ignore.stderr=TRUE){
#   if(class(x)!="jobtoken" && class(x)!="character") stop("Must give a jobtoken object(as obtained from rhex) or the Job id")
#   if(class(x) == "jobtoken") job.id <-  x[[1]]['job.id'] else job.id = x
#   result <- Rhipe:::doCMD(rhoptions()$cmd['join'], jobid =job.id,needoutput=TRUE,
#                           joinwordy = as.character(as.logical(verbose))
#                           ,ignore.stderr=ignore.stderr)
#                          
#   if(class(x) == "jobtoken" && length(x)==2){
#     ## from rhlapply
#     return(x[[2]]())
#   }
#   return(    list(result=result[[1]], counters=result[[2]]))
# }
