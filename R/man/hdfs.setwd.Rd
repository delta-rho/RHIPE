\name{hdfs.setwd}
\alias{hdfs.setwd}
\title{Set HDFS Working Directory}
\usage{
  hdfs.setwd(path, check.valid.hdfs = TRUE)
}
\arguments{
  \item{path}{Path to set working directory.  May be
  relative the current HDFS working directory or absolute.}

  \item{check.valid.hdfs}{Optional.  By default hdfs.setwd
  checks to see if the HDFS directory exist by trying to
  list files in it.  If it doesn't appear to exist it
  doesn't set the working directory.  If this check seems
  cumbersome to you, you may disable it by setting this to
  FALSE.}
}
\description{
  Sets a working directory for all Rhipe commands that use
  the HDFS. By default hdfs.working.dir is set to "/".  All
  input directories to all Rhipe commands may omit a
  leading "/" and indicate they are off of the current
  hdfs.working.dir.
}
\details{
  This is anagolous to how file operations in R work off
  the current working directory.

  Using working directories with relative paths for input
  and output helps users make more portable scripts.
}
\examples{
\dontrun{
#DOES NOT RUN.  JUST ILLUSTRATING CONCEPTS.
hdfs.setwd("/tmp")
#this would output to /tmp/test/something.out
rhmr(map=map, N=10, input=c("lapply","text"), ofolder="test/something.out")
rhread("test/something.out",type="sequence")  #reads a sequence file in /tmp/test/something.out
this is NOT relative thus it reads from /test/something.out
rhread("/test/something.out",type="sequence")
}
}
\author{
  Jeremiah Rounds
}
\seealso{
  \code{\link{hdfs.getwd}}
}

