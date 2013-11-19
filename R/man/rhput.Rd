\name{rhput}
\alias{rhput}
\title{Put a file unto the HDFS}
\usage{
  rhput(src, dest, deletedest = TRUE)
}
\arguments{
  \item{src}{Path to the local file to be copied to the
  HDFS.}

  \item{dest}{Path to the file on the HDFS.  rhput creates
  the file at dest.}

  \item{deletedest}{If TRUE this function attempts to
  delete the destination of the HDFS before trying to copy
  to that location on the HDFS.}
}
\value{
  NULL
}
\description{
  Copies the local file called \code{src} (not a folder) to
  the destination \code{dest} on the HDFS. Uses
  \code{path.expand} to expand the \code{src} parameter.
}
\note{
  Local filesystem copy remains after the operation is
  complete.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhget}}, \code{\link{rhdel}},
  \code{\link{rhread}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}
}
\keyword{file}
\keyword{HDFS}
\keyword{put}

