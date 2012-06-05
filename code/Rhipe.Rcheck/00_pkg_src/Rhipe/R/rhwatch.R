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
#' @debug can be one of 'count' which counts the number of errors but does not stop the job, 'stop' which
#' stops the job and 'collect' which collects 20 errors per split, saves them in files (inside the output folder) prefixed with rhipe.debug and does not kill the job
#' @param ... Extra parameters passed to \code{rhstatus}.
#' @return If the state is SUCCEEDED and total output size (in MB) is less than \code{rhoptions()$max.read.in.size} the data is read with a warning if the number of records is more than \code{rhoptions()$reduce.output.records.warn}. If \code{rhoptions()$rhmr.max.records.to.read.in} is not NA, that many records is read. This only works for Sequence output.
#' @seealso \code{\link{rhex}}, \code{\link{rhmr}}, \code{\link{rhkill}}
#' @keywords MapReduce job status
#' @export
rhwatch <- function(job,mon.sec=5,readback=TRUE,debug=NULL,...){
  if(job[[1]]$mapred.job.tracker == TRUE){
    z <- Rhipe:::rhwatch.runner(job, mon.sec,readback,....)
    if(readback==FALSE){
      class(z) <- append(class(z),"rhwatch")
    }
    return(z)
  }
  if(!is.null(debug)){

    if(! "rhmr-map" %in% class(m <- unserialize(charToRaw(job[[1]]$rhipe_map))))
      stop("RHIPE: for debugging purposes, must use a map expression returned  by ewrap")
     
    ##Replace the map expression
    j=m[[1]][[3]] ##the mapply
    jj <- j[[3]][[2]] ## the function passed to mapply
    l <- list()
    l$replace <-  jj[[3]][[2]] ## body of jj
    l$before=m[[1]][[2]]
    l$after=m[[1]][[4]]
    FIX <- function(x) if(is.null(x)) NULL else x
    newm <- as.expression(bquote({
      .(BEFORE)
      result <- mapply(function(.index,k,r) {
        tryCatch(.(REPLACE),error=function(e) rhipe.trap(e,r,k))},1:length(map.values),map.keys,map.values)
      .(AFTER)
    },list(BEFORE=FIX(l$before),AFTER=FIX(l$after),REPLACE=FIX(l$replace))))
    job[[1]]$rhipe_map <- rawToChar(serialize(newm,NULL,ascii=TRUE))
    
    setup <- rhoptions()$debug$map$setup
    cleanup <- rhoptions()$debug$map$cleanup
    handler <- rhoptions()$debug$map$handler$count
    if(is.list(debug) && !is.null(debug$map)){
      if(!is.null(debug$map$setup)) setup     <- debug$map$setup
      if(!is.null(debug$map$cleanup)) cleanup <- debug$map$cleanup
      if(!is.null(debug$map$handler)) handler <- debug$map$handler
    }else if(is.character(debug)){
      handler <- rhoptions()$debug$map$handler[[debug]]
    }
    
    setupmap <- unserialize(charToRaw(job[[1]]$rhipe_setup_map))
    job[[1]]$rhipe_setup_map<- rawToChar(serialize(c(setupmap,setup),NULL,ascii=TRUE))
    
    cleanupmap <- unserialize(charToRaw(job[[1]]$rhipe_cleanup_map))
    job[[1]]$rhipe_cleanup_map<- rawToChar(serialize(c(cleanupmap,rhoptions()$debug$map$cleanup),NULL,ascii=TRUE))
    
   
    
    if(is.null(job[[1]]$rhipe.params.names)){
      job[[1]]$rhipe.params.names <- "rhipe.trap"
    }else{
      job[[1]]$rhipe.params.names <- sprintf("%s;%s",job[[1]]$rhipe.params.names,"rhipe.trap")
    }
    job[[1]]$rhipe.trap <- rawToChar(serialize(handler,NULL,ascii=TRUE))
    job[[1]]$rhipe_copy_file <- 'TRUE' ##logic for local runner is wrong here
    ## job[[1]]$keep.failed.task.files <- 'true'
  }
  if(!is.null((list(...))) && !is.null(list(...)[[".rdb"]])) return(job)
  z <- Rhipe:::rhwatch.runner(job, mon.sec,readback,....)
  if(readback==FALSE){
    class(z) <- append(class(z),"rhwatch")
  }
  z
}

rhwatch.runner <- function(job,mon.sec=5,readback=TRUE,...){
  if(class(job)=="rhmr"){
    results <- rhstatus(rhex(job,async=TRUE),mon.sec=mon.sec,...)
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
    rhstatus(job,mon.sec=mon.sec,...) 
}
