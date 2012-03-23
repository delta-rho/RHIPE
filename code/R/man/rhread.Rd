\name{rhread}
\alias{rhread}
\title{Read Key/Value Pairs From The HDFS}
\usage{
  rhread(files, type = c("sequence"), max = -1L,
    skip = c("/_SUCCESS", "/_LOG", "/_log"), mc = FALSE,
    asraw = FALSE, size = 3000, buffsize = 1024 * 1024,
    quiet = FALSE, ...)
}
\arguments{
  \item{files}{Absolute path to file or directory
  containing map, sequence, or text file to be read on the
  HDFS.}

  \item{type}{Type of file on HDFS.  Must be "sequence",
  "map", or "text".}

  \item{max}{Maximum number of key/value pairs to read for
  map and sequence files.  Maximum number of bytes to read
  for text files.}

  \item{asraw}{Return key/value pairs as Raw data type (ie
  not deserialized).}

  \item{mc}{Setting \code{mc} to TRUE will use the the
  \code{multicore} package to convert the data to R objects
  in parallel. The user must have first loaded
  \code{multicore} via call to library. This often does
  accelerate the process of reading data into R.}

  \item{skip}{Files to skip while reading the hdfs.
  Various installs of Hadoop add additional log info to
  HDFS output from MapReduce.  Attempting to read these
  files is not what we want to do in rhread.  To get around
  this we specify pieces of filenames to grep and remove
  from the read.  skip is a vector argument just to have
  sensible defaults for a number of different systems.  You
  can learn which if any files need to be skipped by using
  rhls on target directory.}

  \item{size}{Increment the return list by this amount as
  reading in data.}

  \item{buffsize}{Size of byte buffer used to read in
  data.}

  \item{quiet}{If FALSE prints additional information about
  the read to STDOUT.}

  \item{\ldots}{Additional arguments to hmerge which is
  used internally for reading text and gzip files.}
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
  the number of bytes read and is currently alpha quality.

  Setting \code{mc} to TRUE will use the the
  \code{multicore} package to convert the data to R objects
  in parallel. The user must have first loaded
  \code{multicore} via call to library. This often does
  accelerate the process of reading data into R.

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

