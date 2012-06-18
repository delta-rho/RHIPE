\name{rhwatch}
\alias{rhwatch}
\title{Start and Monitor Status of a MapReduce Job}
\usage{
  rhwatch(job, mon.sec = 5, readback = TRUE, debug = NULL,
    ...)
}
\arguments{
  \item{job}{The parameter \code{job} can either be a
  string with the format \emph{job_datetime_id} (e.g.
  \emph{job_201007281701_0274}) or the value returned from
  \code{rhmr}.}

  \item{mon.sec}{If \code{mon.sec} is greater than 0, a
  small data frame indicating the progress will be returned
  every \code{mon.sec} seconds.}

  \item{readback}{if FALSE, results will not be read back
  and insteat results from rhstatus is returned}

  \item{debug}{can be one of 'count' which counts the
  number of errors but does not stop the job, 'stop' which
  stops the job (setting debug to NULL (default) is the
  same and is much faster) and 'collect' which collects 20
  errors per split, saves them in files (inside the output
  folder) prefixed with rhipe.debug and does not kill the
  job. The maximum number of errors to collect can be set
  in the \code{param} argument to rhmr, i.e
  rh.max.errors=10 collects a maximum of 10 errors}

  \item{...}{Extra parameters passed to \code{rhstatus}.}
}
\value{
  If the state is SUCCEEDED and total output size (in MB)
  is less than \code{rhoptions()$max.read.in.size} the data
  is read with a warning if the number of records is more
  than \code{rhoptions()$reduce.output.records.warn}. If
  \code{rhoptions()$rhmr.max.records.to.read.in} is not NA,
  that many records is read. This only works for Sequence
  output.
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

