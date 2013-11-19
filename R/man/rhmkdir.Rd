\name{rhmkdir}
\alias{rhmkdir}
\title{Creates Directories on the HDFS}
\usage{
  rhmkdir(path, permissions)
}
\arguments{
  \item{path}{is the location of the directory to be
  created. It is expanded using
  \code{\link{rhabsolute.hdfs.path}}}

  \item{permissions}{either of the integer form e.g. 777 or
  string e.g. rwx. If missing the default is used.}
}
\value{
  Logical TRUE if success
}
\description{
  Equivalent of mkdir -p in Unix
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhcp}}, \code{\link{rhmv}},
}
\keyword{file}
\keyword{HDFS}
\keyword{mkdir}

