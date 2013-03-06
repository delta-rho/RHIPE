\name{rhread}
\alias{rhread}
\title{Read Key/Value Pairs From The HDFS}
\usage{
  rhread(files, type = c("sequence"), max = -1L,
    skip = rhoptions()$file.types.remove.regex,
    mc = lapply, ...)
}
\arguments{
  \item{files}{Path to file or directory containing map,
  sequence, or text file to be read on the HDFS. This can
  also be the output from rhwatch(provided read=FALSE) or
  rhmr.}

  \item{type}{Type of file on HDFS.  Must be "sequence",
  "map", or "text".}

  \item{max}{Maximum number of key/value pairs to read for
  map and sequence files.  Maximum number of lines to read
  for text files.}

  \item{mc}{Set to lapply by default. User can change this
  to \code{mclapply} for parallel lapply.}

  \item{skip}{Files to skip while reading the hdfs.
  Various installs of Hadoop add additional log info to
  HDFS output from MapReduce.  Attempting to read these
  files is not what we want to do in rhread.  To get around
  this we specify pieces of filenames to grep and remove
  from the read.  skip is a vector argument just to have
  sensible defaults for a number of different systems.  You
  can learn which if any files need to be skipped by using
  rhls on target directory.}
}
\value{
  For map and sequence files, a list of key, pairs of up to
  length MAX.  For text files, a matrix of lines, each row
  a line from the text files.
}
\description{
  Reads all or a limited number of key/value pairs from
  HDFS files.
}
\details{
  Reads the key,value pairs from the files pointed to by
  \code{files}. The source \code{files} can end in a
  wildcard (*) e.g. \emph{/path/input/p*} will read all the
  key,value pairs contained in files starting with \emph{p}
  in the folder \emph{/path/input/}.

  The parameter \code{type} specifies the format of
  \code{files}. This can be one of \code{text}, \code{map}
  or \code{sequence} which imply a Text file, MapFile or a
  SequenceFile respectively.

  The parameter \code{max} specifies the maximum number of
  entries to read, by default all the key,value pairs will
  be read.  Specifying \code{max} for text files, limits
  the number of lines read.

  \code{mc} is by default \code{lapply}. The user can
  change this to mclapply for faster throughput.

  Data written by \code{\link{rhwrite}} can be read using
  \code{rhread}.
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhget}}, \code{\link{rhput}},
  \code{\link{rhdel}}, \code{\link{rhwrite}},
  \code{\link{rhsave}}
}
\keyword{file}
\keyword{HDFS}
\keyword{read}

