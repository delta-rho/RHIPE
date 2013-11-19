\name{rhget}
\alias{rhget}
\title{Copying from the HDFS
Moves files from the HDFS to a local directory.}
\usage{
  rhget(src, dest)
}
\arguments{
  \item{src}{Absolute path to file or directory on HDFS to
  get.}

  \item{dest}{Path to file or directory on local filesystem
  to create as the new copy.}
}
\value{
  NULL
}
\description{
  Copies the files (or folder) at \code{src}, located on
  the HDFS to the destination \code{dest} located on the
  local filesystem. If a file or folder of the same name as
  \code{dest} exists on the local filesystem, it will be
  deleted. The \code{dest} can contain ~ which will be
  expanded. The original copy of the file or folder is left
  on the HDFS after this operation.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhput}}, \code{\link{rhdel}},
  \code{\link{rhread}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}
}
\keyword{directory}
\keyword{get}
\keyword{HDFS}

