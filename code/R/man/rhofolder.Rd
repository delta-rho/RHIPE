\name{rhofolder}
\alias{rhofolder}
\title{Returns the output folder from a previous RHIPE job}
\usage{
  rhofolder(job)
}
\arguments{
  \item{job}{Can be a character indicating the string to a
  folder, or the result of call to rhmr or rhwatch. For the
  latter, this works only read is FALSE (because if read is
  TRUE, the output is returned)}
}
\description{
  Returns the output folder from a previous RHIPE job Take
  a look at \code{\link{hdfs.getwd}} for more information.
}
\author{
  Saptarshi Guha
}

