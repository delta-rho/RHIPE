\name{rhabsolute.hdfs.path}
\alias{rhabsolute.hdfs.path}
\title{Get Absolute HDFS Path}
\usage{
  rhabsolute.hdfs.path(paths)
}
\arguments{
  \item{paths}{Path to examine and change to absolute.
  Input characters or a list or vector of characters.}
}
\value{
  Absolute HDFS path corresponding to relative path in the
  input.  If input is a vector or list returns a vector or
  list of paths.  Class of elements of return are always
  character even if NA.
}
\description{
  Changes a relative path (a path without a leading /) into
  an absolute path based on the value of hdfs.getwd().  If
  it is already an absolute path there is no change to the
  returned path.
}
\details{


  For all returns, any trailing "/" are removed from the
  path (if path nchar > 1).
}
\author{
  Jeremiah Rounds
}

