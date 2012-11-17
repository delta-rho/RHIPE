\name{rhstatus}
\alias{rhstatus}
\title{Report Status Of A MapReduce Job}
\usage{
  rhstatus(job, mon.sec = 5, autokill = TRUE,
    showErrors = TRUE, verbose = FALSE, handler = NULL)
}
\arguments{
  \item{job}{The parameter \code{job} can either be a
  string with the format \emph{job_datetime_id} (e.g.
  \emph{job_201007281701_0274}) or the value returned from
  \code{rhex} with the \code{async} option set to TRUE.}

  \item{mon.sec}{If \code{mon.sec} is greater than 0, a
  small data frame indicating the progress will be returned
  every \code{mon.sec} seconds.If 0, it will return
  immediately. If Inf, it will wait till over.}

  \item{autokill}{If \code{autokill} is TUE, then any R
  errors caused by the map/reduce code will cause the job
  to be killed.}

  \item{verbose}{If \code{verbose} is TRUE, also provided
  is the state of the job, the duration in seconds, a data
  frame with columns for the Map and Reduce phase. This
  data frame summarizes the number of tasks, the percent
  complete, and the number of tasks that are pending,
  running, complete or have failed. In addition the list
  has an element that consists of both user defined and
  Hadoop MapReduce built in counters (counters can be user
  defined with a call to \code{rhcounter}).}

  \item{showErrors}{If TRUE display errors from R in
  MapReduce tasks.}

  \item{handler}{is a function that gets the counters and
  job related information and you can use it to kill the
  job, returning FALSE, stops monitoring}
}
\value{
  a list of the current state
}
\description{
  Returns the status of an running MapReduce job.
}
\note{
  This function does something different depending on if it
  is used in MapReduce expression during a MapReduce task.
  In a MapReduce task use this function to REPORT the
  status of your job to Hadoop.

  From the user side, this displays the status of a running
  MapReduce job and reports information accumulated about
  the Hadoop job.

  The parameter \code{job} can either be a string with the
  format \emph{job_datetime_id} (e.g.
  \emph{job_201007281701_0274}) or the value returned from
  \code{rhex} with the \code{async} option set to TRUE.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhex}}, \code{\link{rhmr}},
  \code{\link{rhkill}}
}
\keyword{job}
\keyword{MapReduce}
\keyword{status}

