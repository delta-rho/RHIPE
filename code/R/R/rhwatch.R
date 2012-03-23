#' Start and Monitor Status of a MapReduce Job
#'
#' Returns the status of an running MapReduce job upon completion/failure 
#'
#' @param job The parameter \code{job} can either be a string with the format
#'   \emph{job_datetime_id} (e.g. \emph{job_201007281701_0274}) or the value
#'   returned from \code{rhmr}.
#' @param mon.sec If \code{mon.sec} is greater than 0, a small data frame
#'   indicating the progress will be returned every \code{mon.sec} seconds.
#' @param ... Extra parameters passed to \code{rhstatus}.
#' @seealso \code{\link{rhex}}, \code{\link{rhmr}}, \code{\link{rhkill}}
#' @keywords MapReduce job status
#' @export
rhwatch <- function(job,mon.sec=5,...){
  if(class(job)=="rhmr"){
    results <- rhstatus(rhex(z,async=TRUE),mon.sec=mon.sec,....)
    if(results$state == "SUCCESS" && sum(rhls(ofolder)$size)/1024 < rhoptions()$max.read.in.size){
      num.records <- results$counters$'Map-Reduce Framework'$'Reduce output records'
      if (num.records > rhoptions()$reduce.output.records.warn)
        warning(sprintf("Number of output records is %s which is greater than rhoptions()$reduce.output.records.warn\n. Consider running a mapreduce to make this smaller, since reading so many key-value pairs is slow in R", num.records))
      if(rhoptions()$rhmr.max.records.to.read.in != NA)
        return( rhread(ofolder,max=rhmr.max.records.to.read.in) )
      else
        return( rhread(ofolder) )
    }
  }
  else
    ## Ideally even with a job.id i can still get the all the job info
    ## by looking somewhere in the output folder.
    ## job is now a job_identifier string
    rhstatus(job,mon.sec=mon.sec,....) 
}
