\name{rhkill}
\alias{rhkill}
\title{Kill A MapReduce Job}
\usage{
  rhkill(job)
}
\arguments{
  \item{job}{The parameter \code{job} can either be string
  with the format \emph{job_datetime_id} or the value
  returned from \code{rhex} with the \code{async} option
  set to TRUE.}
}
\value{
  NULL
}
\description{
  This kills the MapReduce job with job identifier given by
  \code{job}. The parameter \code{job} can either be string
  with the format \emph{job_datetime_id} or the value
  returned from \code{rhex} with the \code{async} option
  set to TRUE.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhstatus}}, \code{\link{rhmr}},
  \code{\link{rhjoin}}, \code{\link{rhex}}
}

