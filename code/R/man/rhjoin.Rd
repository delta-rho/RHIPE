\name{rhjoin}
\alias{rhjoin}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Wait For A MapReduce Job
	
}
\description{
	This function waits for a MapReduce job to complete before returning.
}
\usage{
rhjoin(x, verbose = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
 The parameter \code{x} can either be
string with the format \emph{job_datetime_id} or the value returned from \code{rhex}
with the \code{async} option set to TRUE. 
}

}
\details{
Calling this functions pauses the R console till the MapReduce job indicated by
\code{x} is over (successfully or not). The parameter \code{x} can either be
string with the format \emph{job_datetime_id} or the value returned from \code{rhex}
with the \code{async} option set to TRUE. This function returns the same object as
\code{rhex} i.e a list of the results of the job (TRUE or FALSE indicating success
or failure) and a counters returned by the job. If \code{ignore} is FALSE, the
progress will be displayed on the R console (much like \code{rhex}).
}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhstatus}}, \code{\link{rhmr}}, \code{\link{rhkill}}, \code{\link{rhex}}
}
