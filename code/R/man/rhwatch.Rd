\name{rhwatch}
\alias{rhwatch}
\title{Start and Monitor Status of a MapReduce Job}
\usage{
  rhwatch(job, mon.sec = 5, ...)
}
\arguments{
  \item{job}{The parameter \code{job} can either be a
  string with the format \emph{job_datetime_id} (e.g.
  \emph{job_201007281701_0274}) or the value returned from
  \code{rhmr}.}

  \item{mon.sec}{If \code{mon.sec} is greater than 0, a
  small data frame indicating the progress will be returned
  every \code{mon.sec} seconds.}

  \item{...}{Extra parameters passed to \code{rhstatus}.}
}
\description{
  Returns the status of an running MapReduce job upon
  completion/failure
}
\seealso{
  \code{\link{rhex}}, \code{\link{rhmr}},
  \code{\link{rhkill}}
}
\keyword{job}
\keyword{MapReduce}
\keyword{status}

