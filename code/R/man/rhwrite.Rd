\name{rhwrite}
\alias{rhwrite}
\title{Write R data to the HDFS}
\usage{
  rhwrite(lo, dest, N = NULL)
}
\arguments{
  \item{lo}{List of R objects to place on the HDFS.}

  \item{dest}{Absolute path to destination directory on the
  HDFS.}

  \item{N}{See Details.}
}
\value{
  NULL
}
\description{
  Takes a list of objects, found in \code{lo} and writes
  them to the folder pointed to by \code{dest} which will
  be located on the HDFS.
}
\details{
  The file \code{dest} will be in a format interpretable by
  RHIPE, i.e it can be used as input to a MapReduce job.
  The values of the list of are written as key-value pairs
  in a SequenceFileFormat format. \code{N} specifies the
  number of files to write the values to. For example, if
  \code{N} is 1, the entire list \code{list} will be
  written to one file in the folder \code{dest}.
  Computations across small files do not parallelize well
  on Hadoop. If the file is small, it will be treated as
  one split and the user does not gain any (hoped for)
  parallelization. Distinct files are treated as distinct
  splits. It is better to split objects across a number of
  files. If the list consists of a million objects, it is
  prudent to split them across a few files. Thus if $N$ is
  10 and \code{list} contains 1,000,000 values, each of the
  10 files (located in the directory \code{dest}) will
  contain 100,000 values.

  Since the list only contains values, the keys are the
  indices of the value in the list, stored as strings. Thus
  when used as a source for a MapReduce job, the variable
  \code{map.keys} will contain numbers in the range $[1,
  length(list)]$. The variable \code{map.values} will
  contain elements of \code{list}.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhget}}, \code{\link{rhput}},
  \code{\link{rhdel}}, \code{\link{rhread}},
  \code{\link{rhwrite}}, \code{\link{rhsave}}
}
\keyword{HDFS}
\keyword{write}

