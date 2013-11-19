\name{rhmv}
\alias{rhmv}
\title{Move files (or folders) on the HDFS (delete original)}
\usage{
  rhmv(ifile, ofile)
}
\arguments{
  \item{ifile}{Absolute path to be copied on the HDFS or
  the output from rhwatch(.., read=FALSE).}

  \item{ofile}{Absolute path to place the copies on the
  HDFS.}
}
\value{
  NULL
}
\description{
  Copies the file (or folder) \code{src} on the HDFS to the
  destination \code{dest} also on the HDFS.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhget}}, \code{\link{rhput}},
  \code{\link{rhdel}}, \code{\link{rhread}},
  \code{\link{rhwrite}}, \code{\link{rhsave}}
}
\keyword{copy}
\keyword{file}
\keyword{HDFS}

