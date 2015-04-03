#' Report Status Of A MapReduce Job
#'
#' Returns the status of an running MapReduce job. 
#'
#' @param job The parameter \code{job} can either be a string with the format
#'   \emph{job_datetime_id} (e.g. \emph{job_201007281701_0274}) or the value
#'   returned from \code{rhex} with the \code{async} option set to TRUE.
#' @param mon.sec If \code{mon.sec} is greater than 0, a small data frame
#'   indicating the progress will be returned every \code{mon.sec} seconds.If 0, it will return immediately. If Inf, it will wait till over.
#' @param autokill If \code{autokill} is TRUE, then any R errors caused by the
#'   map/reduce code will cause the job to be killed.
#' @param verbose If \code{verbose} is TRUE, also provided is the state of the
#'   job, the duration in seconds, a data frame with columns for the Map and
#'   Reduce phase. This data frame summarizes the number of tasks, the percent
#'   complete, and the number of tasks that are pending, running, complete or
#'   have failed. In addition the list has an element that consists of both
#'   user defined and Hadoop MapReduce built in counters (counters can be user
#'   defined with a call to \code{rhcounter}).
#' @param showErrors If TRUE display errors from R in MapReduce tasks.
#' @param handler is a function that gets the counters and job related information
#' and you can use it to kill the job, returning FALSE, stops monitoring
#' @return a list of the current state
#' @author Saptarshi Guha
#' @note This function does something different depending on if it is used in
#' MapReduce expression during a MapReduce task.  In a MapReduce task use this
#' function to REPORT the status of your job to Hadoop.
#' 
#' From the user side, this displays the status of a running MapReduce job and
#' reports information accumulated about the Hadoop job.
#' 
#' The parameter \code{job}
#' can either be a string with the format \emph{job_datetime_id} (e.g.
#' \emph{job_201007281701_0274}) or the value returned from \code{rhex} with
#' the \code{async} option set to TRUE.
#'
#' @seealso \code{\link{rhex}}, \code{\link{rhmr}}, \code{\link{rhkill}}
#' @keywords MapReduce job status
#' @export
rhstatus <- function(job, mon.sec = 5, autokill = TRUE, showErrors = TRUE, verbose = FALSE, 
   handler = rhoptions()$statusHandler) {
   if (!is(job, "jobtoken") && !is(job, "character") && !is(job, "rhwatch")) 
      stop("Must give a jobtoken object(as obtained from rhwatch(..read=FALSE))")
   if (is(job, "character")) 
      id <- job else if (is(job, "jobtoken")) {
      job <- job[[1]]
      id <- job[["job.id"]]
   } else if (is(job, "rhwatch")) {
      id = parseJobIDFromTracking(job[[1]])
   }
   if (mon.sec == Inf) {
      result <- rhoptions()$server$rhjoin(id, TRUE)
      mon.sec <- 1
   }
   if (mon.sec <= 0) {
      return(Rhipe:::.rhstatus(id, autokill, showErrors))
   } else {
      handler <- if (is.null(handler)) 
         function(y) TRUE else {
         message("RHIPE: Using custom handler")
         handler
      }
      
      # nr is initial rows to set the cursor back (used when job.status.overprint is
      # TRUE)
      nr <- 0
      orig_width <- getOption("width")
      width <- as.integer(Sys.getenv("COLUMNS"))
      if (is.na(width)) {
         width <- getOption("width")
      } else {
         options(width = width)
      }
      
      while (TRUE) {
         y <- .rhstatus(id, autokill = TRUE, showErrors)
         
         # in case user resizes terminal
         width <- as.integer(Sys.getenv("COLUMNS"))
         if (is.na(width)) 
            width <- getOption("width") + nchar(getOption("prompt"))
         
         # build the entire string first
         headerTxt <- c(sprintf("[%s] Name:%s Job: %s  State: %s Duration: %s", 
            date(), y$jobname, id, y$state, y$duration), sprintf("URL: %s", y$tracking))
         progressTxt <- capture.output(print(y$progress))
         
         if (!is.null(y$warnings)) {
            warningsTxt <- c("--Warnings Present, follows:", capture.output(print(y$warnings)))
         } else {
            warningsTxt <- NULL
         }
         
         if (verbose) {
            countersTxt <- capture.output(print(y$counters))
         } else {
            countersTxt <- NULL
         }
         res <- handler(y)
         if (!is.null(res) && !res) {
            ## warning('RHIPE: Breaking because users handler function said so')
            y$state <- sprintf("%s:UserKill", y$state)
         }
         
         if (!y$state %in% c("PREP", "RUNNING")) 
            break
         
         waitTxt <- sprintf("Waiting %s seconds", mon.sec)
         
         # if overprint, move cursor up (terminal needs to be vt100 compatible)
         if (rhoptions()$job.status.overprint) {
            if (exists("allTxt")) {
              nr <- sum(ceiling(nchar(allTxt)/width))
            }
            if (nr > 0) {
              esc <- paste("\033[", nr, "A100\033[", width, "D", sep = "")
              cat(esc)
            }
         }
         
         allTxt <- c(headerTxt, progressTxt, warningsTxt, countersTxt, waitTxt)
         
         cat(allTxt, sep = "\n")
         flush.console()
         
         Sys.sleep(max(1, as.integer(mon.sec)))
      }
      return(y)
   }
}

.rhstatus <- function(id, autokill = FALSE, showErrors = FALSE) {
   result <- rhoptions()$server$rhstatus(as.character(id), as.logical(showErrors))
   result <- rhuz(result)
   d <- data.frame(pct = result[[3]], numtasks = c(result[[4]][1], result[[5]][[1]]), 
      pending = c(result[[4]][2], result[[5]][[2]]), running = c(result[[4]][3], 
         result[[5]][[3]]), complete = c(result[[4]][4], result[[5]][[4]]), killed = c(result[[4]][5], 
         result[[5]][[5]]), failed_attempts = c(result[[4]][6], result[[5]][[6]]), 
      killed_attempts = c(result[[4]][7], result[[5]][[7]]))
   
   rownames(d) <- c("map", "reduce")
   duration <- result[[2]]
   state <- result[[1]]
   errs <- unique(result[[7]])
   haveRError <- FALSE
   msg.str <- "There were R errors, showing 30:\n"
   wrns <- NULL
   wrns <- if (!is.null(result[[6]]$R_UNTRAPPED_ERRORS)) {
      local({
         k <- length(result[[6]]$R_UNTRAPPED_ERRORS)
         ff <- result[[6]]$R_UNTRAPPED_ERRORS
         d <- data.frame(untrappedError = names(ff), count = as.integer(ff))
         d <- d[order(d$count), ]
         rownames(d) <- NULL
         ## amsg <- if(k==1) warning(sprintf('The RHIPE( %s ) job has %s untrapped
         ## error\n',as.character(id),k)) else warning(sprintf('The RHIPE( %s ) job has %s
         ## untrapped errors\n',as.character(id),k))
         d
      })
   }
   
   if (!is.null(result[[6]]$R_ERRORS)) {
      ## I treat these errors differently from other types not sure if thats need, if
      ## not, this code can be eliminated and errs can be extended by R_ERRORS
      haveRError <- TRUE
      message(sprintf("\n%s\n%s", paste(rep("-", nchar(msg.str)), collapse = ""), 
         msg.str))
      a <- result[[6]]$R_ERRORS
      v <- unique(names(sort(a)))
      ## newr <- t(sapply(v,function(x){ y <- strsplit(x,'\n')[[1]] f <-
      ## which(sapply(y,function(r) grep('(R ERROR)',r),USE.NAMES=FALSE)>=1)
      ## if(length(f)>0) c('R',paste(y[(f[1]+3):(f[2]-1)],collapse='\n')) else
      ## c('NR',x) },USE.NAMES=FALSE)) rerr <- head(newr[newr[,1]=='R',2],30)
      rerr <- head(v, 30)
      sapply(seq_along(rerr), function(i) {
         cat(sprintf("%s(%s): %s", i, as.integer(a[rerr[i]]), rerr[i]))
      })
      if (autokill) {
         message(sprintf("Autokill is true and terminating %s", id))
         rhkill(id)
      }
   }
   if (length(errs) > 0) {
      if (showErrors) {
         newr <- t(sapply(errs, function(x) {
            y <- strsplit(x, "\n")[[1]]
            f <- which(sapply(y, function(r) grep("(R ERROR)", r), USE.NAMES = FALSE) >= 
              1)
            if (length(f) > 0) 
              c("R", paste(y[(f[1] + 3):(f[2] - 1)], collapse = "\n")) else c("NR", x)
         }, USE.NAMES = FALSE))
         if (any(newr[, 1] == "R")) {
            haveRError <- TRUE
            message(sprintf("\n%s\n%s", paste(rep("-", nchar(msg.str)), collapse = ""), 
              msg.str))
            rerr <- head(newr[newr[, 1] == "R", 2], 30)
            sapply(rerr, cat)
         }
         if (any(newr[, 1] == "NR")) {
            message(sprintf("There were Hadoop specific errors (autokill will not kill job), showing at most 30:"))
            rerr <- head(newr[newr[, 1] == "NR", 2], 30)
            sapply(rerr, cat)
         }
      }
      if (autokill && haveRError) {
         message(sprintf("Autokill is true and terminating %s", id))
         rhkill(id)
      }
   }
   if (any(d[, "failed_attempts"] > 0) && !showErrors) 
      warning("There are failed attempts, call rhstatus with  showErrors=TRUE. Note, some are fatal (e.g R errors) and some are not (e.g node failure)")
   if (haveRError) 
      state <- "FAILED"
   ro <- result[[6]]
   trim.trailing <- function(x) sub("\\s+$", "", x)
   ro2 <- lapply(ro, function(r) {
      s <- as.matrix(r)
      rownames(s) <- trim.trailing(rownames(s))
      if (!is.null(rownames(s))) 
         s <- s[order(tolower(rownames(s))), , drop = FALSE]
      s
   })
   return(list(state = state, duration = duration, progress = d, warnings = wrns, 
      counters = ro2, rerrors = haveRError, errors = errs, jobname = result[[9]], 
      tracking = result[[8]], config = result[[10]],jobid  = result[[11]]  ))
}



# rhstatus <- function(x){ if(class(x)!='jobtoken' && class(x)!='character' )
# stop('Must give a jobtoken object(as obtained from rhex)')
# if(class(x)=='character') id <- x else { x <- x[[1]] id <- x[['job.id']] }
# result <- Rhipe:::doCMD(rhoptions()$cmd['status'],jobid =id,
# needoutput=TRUE,ignore.stderr=TRUE,verbose=FALSE) d <-
# data.frame('pct'=result[[3]],'numtasks'=c(result[[4]][1],result[[5]][[1]]),
# 'pending'=c(result[[4]][2],result[[5]][[2]]), 'running' =
# c(result[[4]][3],result[[5]][[3]]), 'complete' =
# c(result[[4]][4],result[[5]][[4]]) ,'failed' =
# c(result[[4]][5],result[[5]][[5]])) rownames(d) <- c('map','reduce') duration =
# result[[2]] state = result[[1]]
# return(list(state=state,duration=duration,progress=d, counters=result[[6]])); } 
