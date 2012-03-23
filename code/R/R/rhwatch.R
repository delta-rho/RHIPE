#' Start and Monitor Status of a MapReduce Job
#'
#' Returns the status of an running MapReduce job upon completion/failure 
#'
#' @param job The parameter \code{job} can either be a string with the format
#'   \emph{job_datetime_id} (e.g. \emph{job_201007281701_0274}) or the value
#'   returned from \code{rhmr}.
#' @param mon.sec If \code{mon.sec} is greater than 0, a small data frame
#'   indicating the progress will be returned every \code{mon.sec} seconds.
#' @param readback if FALSE, results will not be read back and insteat results from rhstatus is returned
#' @param ... Extra parameters passed to \code{rhstatus}.
#' @return If the state is SUCCEEDED and total output size (in MB) is less than \code{rhoptions()$max.read.in.size} the data is read with a warning if the number of records is more than \code{rhoptions()$reduce.output.records.warn}. If \code{rhoptions()$rhmr.max.records.to.read.in} is not NA, that many records is read. This only works for Sequence output.
#' @seealso \code{\link{rhex}}, \code{\link{rhmr}}, \code{\link{rhkill}}
#' @keywords MapReduce job status
#' @export
rhwatch <- function(job,mon.sec=5,readback=TRUE,...){
  if(class(job)=="rhmr"){
    results <- rhstatus(rhex(job,async=TRUE),mon.sec=mon.sec,....)
    ofolder <- job[[1]]$rhipe_output_folder
    if(readback==TRUE && results$state == "SUCCEEDED" && sum(rhls(ofolder)$size)/(1024^2) < rhoptions()$max.read.in.size){
      W <- 'Reduce output records'
      if(!is.null(job[[1]]$mapred.reduce.tasks) && as.numeric(job[[1]]$mapred.reduce.tasks)==0) W <- 'Map output records'
      num.records <- results$counters$'Map-Reduce Framework'[W]
      if (num.records > rhoptions()$reduce.output.records.warn)
        warning(sprintf("Number of output records is %s which is greater than rhoptions()$reduce.output.records.warn\n. Consider running a mapreduce to make this smaller, since reading so many key-value pairs is slow in R", num.records))
      if(!is.na(rhoptions()$rhmr.max.records.to.read.in))
        return( rhread(ofolder,max=rhoptions()$rhmr.max.records.to.read.in) )
      else
        return( rhread(ofolder) )
    }
    if(results$state %in% c("FAILED","KILLED"))
      {
        warning(sprintf("Job failure, deleting output: %s:", ofolder))
        rhdel(ofolder)
      }
    return(list(results,job))
  }
  else
    ## Ideally even with a job.id i can still get the all the job info
    ## by looking somewhere in the output folder.
    ## job is now a job_identifier string
    rhstatus(job,mon.sec=mon.sec,....) 
}
