\name{rhwrite}
\alias{rhwrite}
\title{Write R data to the HDFS}
\usage{
  rhwrite(object, file, numfiles = 1, chunk = 1,
    passByte = 1024 * 1024 * 20, style = "classic")
}
\arguments{
  \item{object}{An object whose elements are written}

  \item{file}{where to write(it is overwritten)}

  \item{numfiles}{number of files to write to}

  \item{chunks}{an integer specificed to chunk data frames
  into rows or lists into sublists}
}
\description{
  Takes a list of objects, found in \code{object} and
  writes them to the folder pointed to by \code{file} which
  will be located on the HDFS.
}
\details{
  This code, will chunk a data frame(or matrix) or list
  into sub objects, defined by chunks and then written to
  the HDFS across numfiles files. Thus if chunks is 10, and
  numfiles is 20, then a data frame is divided into sub
  data frames of rows 10 each and written across 20 files.
  In order to improve the R-Java switch, this is buffered,
  the buffer size defined by passByte(bytes).
}
\examples{
\dontrun{
 O=data.frame(x=1:100,y=1:100)
rhwrite(O,file="/user/sguha/x1", chunk=10,numperfile=3)
writes the above as sub data frames of 10 rows each. Each sub data frame is written to a distinct file.
}
}
\keyword{HDFS}
\keyword{write}

