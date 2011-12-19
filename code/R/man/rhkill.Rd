\name{rhkill}
\alias{rhkill}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Kill A MapReduce Job
}
\description{
	This kills the MapReduce job with job identifier given by \code{x}.
}
\usage{
rhkill(x)
}
	
\arguments{
  \item{x}{
		
}
}
\details{
This kills the MapReduce job with job identifier given by \code{x}. The
parameter \code{x} can either be string with the format \emph{job_datetime_id} or
the value returned from  \code{rhex} with the \code{async} option set to
TRUE.
}
\author{
	Saptarshi Guha
}


\seealso{
 \code{\link{rhstatus}}, \code{\link{rhmr}}, \code{\link{rhjoin}}, \code{\link{rhex}}
}
