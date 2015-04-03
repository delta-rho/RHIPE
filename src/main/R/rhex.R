#' Execute a MapReduce Job On Hadoop
#'
#' Submits a MapReduce job (created using \code{rhwatch}) to the Hadoop MapReduce
#' framework. The argument \code{mapred} serves the same purpose as the
#' \code{mapred} argument to \code{rhwatch}. This will override the settings in
#' the object returned from \code{rhwatch}.  The function returns when the job
#' ends (success/failure or because the user terminated (see \code{rhkill})).
#' When \code{async} is TRUE, the function returns immediately, leaving the job
#' running in the background on Hadoop.
#' 
#' When \code{async=TRUE}, function returns an object of class \emph{jobtoken}.
#' The generic function \code{print.jobtoken}, displays the start time,
#' duration (in seconds) and percent progress. This object can be used in calls
#' to \code{rhstatus},``rhjoin`` and \code{rhkill}. Otherwise is returns a list
#' of counters and the job state.
#' 
#' @param conf conf is a list returned from \code{\link{rhmr}} describing the
#'   MapReduce job.
#' @param async When \code{async} is TRUE, the function returns immediately,
#'   leaving the job running in the background on Hadoop.
#' @param mapred See Details.
#' @param \ldots additional parameters for \code{system} call.
#' @return When \code{async=TRUE}, function returns an object of class
#'   \emph{jobtoken}.
#' @author Saptarshi Guha
#' @seealso \code{\link{rhmr}}, \code{\link{rhstatus}}, \code{\link{rhkill}}
#' @keywords MapReduce job execute
#' @export
rhex <- function(conf, async = TRUE, mapred, ...) {
   exitf <- NULL
   ## browser()
   if (class(conf) == "rhmr") {
      zonf <- conf$temp
      lines <- conf[[1]]
      parameters <- conf$parameters
   } else stop("Wrong class of list given")
   on.exit({
      if (!is.null(lines$rhipe_cp_tempdir)) {
         unlink(lines$rhipe_cp_tempdir, recursive = TRUE)
      }
   }, add = TRUE)
   if (!missing(mapred)) {
      for (i in names(mapred)) {
         lines[[i]] <- mapred[[i]]
      }
   }
   lines$rhipe_job_async <- as.character(as.logical(async))
   
   conffile <- file(zonf, open = "wb")
   writeBin(as.integer(length(lines)), conffile, size = 4, endian = "big")
   for (x in names(lines)) {
      writeBin(as.integer(nchar(x)), conffile, size = 4, endian = "big")
      writeBin(charToRaw(x), conffile, endian = "big")
      writeBin(as.integer(nchar(lines[[x]])), conffile, size = 4, endian = "big")
      writeBin(charToRaw(as.character(lines[[x]])), conffile, endian = "big")
   }
   close(conffile)
   if (!is.null(parameters)) {
      lines <- Rhipe:::saveParams(parameters, lines = lines)
   }
   ## cmd <- sprintf('%s/hadoop jar %s org.godhuli.rhipe.RHMR %s
   ## ',Sys.getenv('HADOOP_BIN'),rhoptions()$jarloc,zonf) x. <- paste('Running: ',
   ## cmd) y. <- paste(rep('-',min(nchar(x.),40)))
   ## message(y.);message(x.);message(y.)
   {
      result <- tryCatch(rhoptions()$server$rhex(zonf, rhoptions()$server$getConf()), 
         error = function(e) {
            e$printStackTrace()
            stop(e)
         })
      
      if (result == 1) 
         result <- 256
      ## cat(sprintf('result:%s\n',result))
   }
   f3 <- NULL
   if (result == 256) {
      f1 <- file(zonf, "rb")
      f2 <- readBin(f1, "integer", 1, endian = "network")
      f3 <- rhuz(readBin(f1, "raw", f2))
      close(f1)
   }
   unlink(zonf)
   if (result == 256 && !is.null(exitf) && !async) {
      return(exitf())
   }
   if (async == TRUE) {
      y <- if (!is.null(exitf)) 
         list(f3, lines, exitf) else list(f3, lines)
      names(y[[1]]) <- c("job.url", "job.name", "job.id", "job.start.time")
      class(y) <- "jobtoken"
      return(y)
   }
   if (result == 256) {
      if (!is.null(f3) && !is.null(f3$R_ERRORS)) {
         rr <- FALSE
         stop(sprintf("ERROR\n%s", paste(names(f3$R_ERRORS), collapse = "\n")))
      } else rr <- TRUE
   } else rr <- FALSE
   return(list(state = rr, counters = f3))
}



saveParams <- function(parameters, lines) {
   if (length(ls(parameters$envir)) > 0) 
      message(sprintf("Saving %s parameter%s to %s (use rhclean to delete all temp files)", 
         length(ls(parameters$envir)), if (length(ls(parameters$envir)) > 1) 
            "s" else "", parameters$file))
   vlist <- ls(parameters$envir)
   vwhere <- parameters$envir
   lines$rhipe.has.params <- TRUE
   rhsave(list = vlist, envir = vwhere, file = parameters$file)
   return(lines)
}



