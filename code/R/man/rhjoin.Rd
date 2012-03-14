\name{rhjoin}
\alias{rhjoin}
\title{Wait For A MapReduce Job}
\usage{
  rhjoin(job)
}
\arguments{
  \item{job}{The parameter \code{job} can either be string
  with the format \emph{job_datetime_id} or the value
  returned from \code{rhex} with the \code{async} option
  set to TRUE.}
}
\value{
  This function returns the same object as \code{rhex} i.e
  a list of the results of the job (TRUE or FALSE
  indicating success or failure) and a counters returned by
  the job.
}
\description{
  This function waits for a MapReduce job to complete
  before returning.
}
\note{
  Calling this functions pauses the R console till the
  MapReduce job indicated by \code{job} is over
  (successfully or not). The parameter \code{job} can
  either be string with the format \emph{job_datetime_id}
  or the value returned from \code{rhex} with the
  \code{async} option set to TRUE.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhstatus}}, \code{\link{rhmr}},
  \code{\link{rhkill}}, \code{\link{rhex}}
}

