\name{rhwrite2}
\alias{rhwrite2}
\title{Write R data to the HDFS}
\usage{
  rhwrite2(object, file, numperfile = 1, chunked = NULL,
    elementWriter = NULL)
}
\arguments{
  \item{object}{An object whose elements are written}

  \item{file}{where to write(it is overwritten)}

  \item{numperfile}{number of elements per file before a
  new file is created}

  \item{elementWriter}{a list with 'howmany' (a function)
  that returns the numbeer of elements in the object and
  'mu' (a function) that writes the elements to a handle.
  'mu' takes the object,file handle to write to and chunked
  which can be NULL or an integer for chunking.}
}
\description{
  Takes a list of objects, found in \code{lo} and writes
  them to the folder pointed to by \code{dest} which will
  be located on the HDFS.
}
\details{
  This code writes NULL keys! The pairs written will be
  (NULL, element of object). For a list, element of object
  is self explanatory. For a data frame element is every
  row.
}
\examples{
\dontrun{
 O=data.frame(x=1:100,y=1:100)
rhwrite2(O,file="/user/sguha/x1", chunked=10)
writes the above as sub data frames of 10 rows each. Each sub data frame is written to a distinct file.
}
}
\keyword{HDFS}
\keyword{write}

