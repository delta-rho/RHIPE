\name{rhgetkey}
\alias{rhgetkey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Get Value Associated With a Key In A Map File
}
\description{
	Returns the values associated with a key in a map file on the HDFS.
}
\usage{
rhgetkey(keys, paths, sequence = "", skip = 0L, mc = FALSE, size = 3000, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{keys}{
	Keys to return values for.
}
  \item{paths}{
	Absolute path to map file on HDFS.
}

  \item{mc}{
		Setting \code{mc} to TRUE will use the the \code{multicore} package to convert the data to R
objects in parallel. The user must have first loaded \code{multicore} via call to
library. This often does accelerate the process of reading data into R.~
}

}
\details{

Returns the values from the map files contained in \code{path} corresponding to the
keys in \code{keys}. \code{path} will contain folders which is MapFiles are
stored. Thus the \code{path} must have been created as the output of a RHIPE job
with \code{inout[2]} (the output format) set to \emph{map}. Also, the saved keys must be in sorted order. This is always the case if
\emph{mapred.reduce.tasks} is not zero. The variable \emph{reduce.key} is not modified.

A simple way to convert any RHIPE SequenceFile data set  to MapFile is to run an identity MapReduce.
}
\value{

}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhget}}, \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhdel}},   \code{\link{rhwrite}},  \code{\link{rhsave}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{keys} 
\keyword{HDFS} 
\keyword{file}
