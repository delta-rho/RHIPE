#' Retrieves errors from a job when debug=='collect' in rhwatch or dump frames was used
#'
#' Retrieves errors from a job when debug=='collect' in rhwatch or the user dumped frames
#' @param job is the result of rhwatch (when read=FALSE) or a jobid (character)
#' @param prefix is the prefix string of the debug files (optional for dump frames)
#' @param num.file is the number of debug files to read (doesn't apply to dump frame options)
#' @author Saptarshi Guha
#' @export
rherrors <- function(job, prefix = "rhipe_debug", num.file = 1) {
   tryCatch({
      id <- if(is(job, 'rhwatch')) job[[1]]$jobid ## if rhwatch succeeds
             else  if(is(job, 'list') && is(job[[2]], "rhmr")) job[[1]]$jobid ## if job fails
	     else  if(is(job,"character")) job ## you just provide a job id
      x1 <- rhls(sprintf("%s/map-reduce-error%s", rhoptions()$HADOOP.TMP.FOLDER,id),rec=TRUE)
      m1 = nrow(x1)
} ,error=function(e) 0)
   if(m1>0){
      ## user used the dump file option
      return(x1[x1$size>0,])
   }
   files <- tryCatch(rhls(sprintf("%s/%s/", rhofolder(job), rhoptions()$rhipe_copyfile_folder))$file, error=function(e) NULL)
   if(is.null(files)) {
     warning( sprintf("No Errors for this location: %s",(sprintf("%s", rhofolder(job)))))
     return(NULL)
  }
   files <- files[grepl(prefix, files)][1:num.file]
   y <- new.env()
   unlist(lapply(files, function(r) {
      rhload(r, envir = y)
      y$rhipe.errors
   }), recursive = FALSE)

}