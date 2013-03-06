\name{rhls}
\alias{rhls}
\title{List Files On HDFS}
\usage{
  rhls(folder = NULL, recurse = FALSE)
}
\arguments{
  \item{folder}{Path of directory on HDFS or output from
  rhmr or rhwatch(read=FALSE)}

  \item{recurse}{If TRUE list all files and directories in
  sub-directories.}
}
\value{
  vector of file and directory names
}
\description{
  List all files and directories contained in a directory
  on the HDFS.
}
\details{
  Returns a data.frame of filesystem information for the
  files located at \code{path}. If \code{recurse} is TRUE,
  \code{rhls} will recursively travel the directory tree
  rooted at \code{path}. The returned object is a
  data.frame consisting of the columns: \emph{permission,
  owner, group, size (which is numeric), modification
  time}, and the \emph{file name}.  \code{path} may
  optionally end in `*' which is the wildcard and will
  match any character(s).
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhput}}, \code{\link{rhdel}},
  \code{\link{rhread}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}, \code{\link{rhget}}
}
\keyword{directory}
\keyword{HDFS}
\keyword{list}

