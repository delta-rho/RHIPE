\name{rhex}
\alias{rhex}
\title{Execute a MapReduce Job On Hadoop}
\usage{
  rhex(conf, async = TRUE, mapred, ...)
}
\arguments{
  \item{conf}{conf is a list returned from
  \code{\link{rhmr}} describing the MapReduce job.}

  \item{async}{When \code{async} is TRUE, the function
  returns immediately, leaving the job running in the
  background on Hadoop.}

  \item{mapred}{See Details.}

  \item{\ldots}{additional parameters for \code{system}
  call.}
}
\value{
  When \code{async=TRUE}, function returns an object of
  class \emph{jobtoken}.
}
\description{
  Submits a MapReduce job (created using \code{rhmr}) to
  the Hadoop MapReduce framework. The argument
  \code{mapred} serves the same purpose as the
  \code{mapred} argument to \code{rhmr}. This will override
  the settings in the object returned from \code{rhmr}.
  The function returns when the job ends (success/failure
  or because the user terminated (see \code{rhkill})). When
  \code{async} is TRUE, the function returns immediately,
  leaving the job running in the background on Hadoop.
}
\details{
  When \code{async=TRUE}, function returns an object of
  class \emph{jobtoken}. The generic function
  \code{print.jobtoken}, displays the start time, duration
  (in seconds) and percent progress. This object can be
  used in calls to \code{rhstatus},``rhjoin`` and
  \code{rhkill}. Otherwise is returns a list of counters
  and the job state.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhmr}}, \code{\link{rhstatus}},
  \code{\link{rhkill}}
}
\keyword{execute}
\keyword{job}
\keyword{MapReduce}

