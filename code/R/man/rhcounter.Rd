\name{rhcounter}
\alias{rhcounter}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Update Status Counters During a MapReduce Job
}
\description{
	From within a map or reduce expression update a Hadoop status counter.  Useful for monitoring your job.
}
\usage{
rhcounter(group, name, value)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{group}{
	Character string describing group (may be anything the user wants).
}
  \item{name}{
	Character string indicating name (may be anything the user wants).
}
	\item{value}{
	Integer indicating the amount to increment the counter group/name counter by
}

}
\details{

Increments (in a safe way i.e. no race conditions) the distributed counter
\code{name} that belongs to family \code{group} by \code{value}.  Ideally \code{group} and
\code{name} should be strings, any R object can be sent and it will be converted to
its string representation.  The group/name counter need not exist before this call.  All counters start at zero.
}

\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
	\code{\link{rhstatus}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{MapReduce}
