#' Very Detailed Job Information 
#'
#' Returns detailed job information across all TaskIDs
#'
#' @param job The parameter \code{job} can either be a string with the format
#'   \emph{job_datetime_id} (e.g. \emph{job_201007281701_0274}) or the value
#'   returned from \code{rhex} with the \code{async} option set to TRUE.
#' @export
rhJobInfo <- function(job) {
   fu <- rhoptions()$clz$fileutils
   if (!is(job, "jobtoken") && !is(job, "character") && !is(job, "rhwatch")) 
      stop("Must give a jobtoken object(as obtained from rhwatch(..read=FALSE))")
   if (is(job, "character")) 
      id <- job else if (is(job, "jobtoken")) {
      job <- job[[1]]
      id <- job[["job.id"]]
   } else if (is(job, "rhwatch")) {
	 id = parseJobIDFromTracking(job[[1]])
   }
   a <- fu$getDetailedInfoForJob(id)
   a <- rhuz(a)
   names(a) <- c("State", "Start", "Progess", "Counters", "Tracking", "Name", "Conf", 
      "MapTasks", "ReduceTasks")
   names(a$Progess) <- c("map", "reduce")
   a$Counters$job_time <- NULL
   a$Counters <- lapply(a$Counters, function(s) {
      data.frame(name = names(s), value = s, stringsAsFactors = FALSE)
   })
   jj <- function(L) {
      lapply(L, function(m) {
         names(m) <- c("Counters", "Status", "Progress", "TaskID", "SuccessfulAttempt")
         m$Progress <- as.list(m$Progress)
         names(m$Progress) <- c("progress", "start", "end")
         m$Progress$start <- as.POSIXct(m$Progress$start/1000, origin = "1970-01-01")
         m$Progress$end <- as.POSIXct(m$Progress$end/1000, origin = "1970-01-01")
         m$Counters$job_time <- NULL
         m$Counters <- lapply(m$Counters, function(s) {
            data.frame(name = names(s), value = s, stringsAsFactors = FALSE)
         })
         m
      })
   }
   a$MapTasks <- jj(a$MapTasks)
   a$ReduceTasks <- jj(a$ReduceTasks)
   a
} 
