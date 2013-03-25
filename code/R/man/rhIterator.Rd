\name{rhIterator}
\alias{rhIterator}
\title{Iterates Through the Records of Sequence Files}
\usage{
  rhIterator(files, type = "sequence", chunksize = 1000,
    chunk = "records",
    skip = rhoptions()$file.types.remove.regex,
    mc = lapply)
}
\arguments{
  \item{files}{Path to file or directory containing
  sequence files.  This can also be the output from
  rhwatch(provided read=FALSE) or rhmr.}

  \item{chunksize}{Number of records or bytes to read.
  Depends on 'chunk'}

  \item{type}{Is it "sequence" or "map'. Ideally should be
  auto-determined.}

  \item{skip}{Files to skip while reading the hdfs.
  Various installs of Hadoop add additional log info to
  HDFS output from MapReduce.  Attempting to read these
  files is not what we want to do.  To get around this we
  specify pieces of filenames to grep and remove from the
  read.  skip is a vector argument just to have sensible
  defaults for a number of different systems.  You can
  learn which if any files need to be skipped by using rhls
  on the target directory.}

  \item{type}{Either 'records' or 'bytes'}
}
\description{
  Can be used to iterate through the records of a Sequence
  File(or collection thereof)
}
\examples{
\dontrun{
   j <- rhwatch(map=rhmap(rhcollect(r,k)),reduce=0, input=c(36,3),read=FALSE)
   a <- rhIterator(j,chunk=11)
   while( length(b<-a())>0) doSomethingWith(b)
}
}

