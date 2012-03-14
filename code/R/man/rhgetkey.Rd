\name{rhgetkey}
\alias{rhgetkey}
\title{Get Value Associated With a Key In A Map File}
\usage{
  rhgetkey(keys, paths, mc = FALSE, size = 3000, skip = 0L)
}
\arguments{
  \item{keys}{Keys to return values for.}

  \item{paths}{Absolute path to map file on HDFS.}

  \item{mc}{Setting \code{mc} to TRUE will use the the
  \code{multicore} package to convert the data to R objects
  in parallel. The user must have first loaded
  \code{multicore} via call to library. This often does
  accelerate the process of reading data into R.}

  \item{skip}{Corresponds to io.map.index.skip}

  \item{size}{Number of Key,Value pairs to increase the
  buffer size by at a time.}
}
\value{
  Returns the values from the map files contained in
  \code{path} corresponding to the keys in \code{keys}.
  \code{path} will contain folders which is MapFiles are
  stored. Thus the \code{path} must have been created as
  the output of a RHIPE job with \code{inout[2]} (the
  output format) set to \emph{map}. Also, the saved keys
  must be in sorted order. This is always the case if
  \emph{mapred.reduce.tasks} is not zero. The variable
  \emph{reduce.key} is not modified.
}
\description{
  Returns the values associated with a key in a map file on
  the HDFS.
}
\note{
  A simple way to convert any RHIPE SequenceFile data set
  to MapFile is to run an identity MapReduce.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhget}}, \code{\link{rhput}},
  \code{\link{rhdel}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}
}
\keyword{file}
\keyword{HDFS}
\keyword{keys}

