\name{rhdel}
\alias{rhdel}
\title{HDFS File Deletion}
\usage{
  rhdel(folder)
}
\arguments{
  \item{folder}{The absolute path on the hdfs to the
  directory(s) to be deleted.}
}
\value{
  NULL
}
\description{
  This function deletes the folders contained in the
  character vector \code{folders} which are located on the
  HDFS. The deletion is recursive, so all subfolders will
  be deleted too. Nothing is returned.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhput}}, \code{\link{rhls}},
  \code{\link{rhread}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}, \code{\link{rhget}}
}
\keyword{delete}
\keyword{directory}
\keyword{HDFS}

