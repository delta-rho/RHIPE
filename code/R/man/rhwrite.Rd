\name{rhwrite}
\alias{rhwrite}
\title{Write R data to the HDFS}
\usage{
  rhwrite(object, file, numperfile = 1, chunks = NULL)
}
\arguments{
  \item{object}{An object whose elements are written}

  \item{file}{where to write(it is overwritten)}

  \item{numperfile}{number of elements per file before a
  new file is created}

  \item{chunks}{an integer specificed to chunk data frames
  into rows}
}
\description{
  Takes a list of objects, found in \code{object} and
  writes them to the folder pointed to by \code{file} which
  will be located on the HDFS.
}
\details{
  For a list, each element has to have two elements(key and
  value). This is written to a file. For a data frame(or
  matrix) it is broken into chunks(specified by chunk size)
}
\examples{
\dontrun{
 O=data.frame(x=1:100,y=1:100)
rhwrite(O,file="/user/sguha/x1", chunk=10)
writes the above as sub data frames of 10 rows each. Each sub data frame is written to a distinct file.
}
}
\keyword{HDFS}
\keyword{write}

