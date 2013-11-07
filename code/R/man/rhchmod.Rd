\name{rhchmod}
\alias{rhchmod}
\title{Change Permissions of Files on HDFS}
\usage{
  rhchmod(path, permissions)
}
\arguments{
  \item{path}{is the location of the directory to be
  created. It is expanded using
  \code{\link{rhabsolute.hdfs.path}}}

  \item{permissions}{a character string such as "777"}
}
\description{
  Change permissions of files on HDFS
}
\author{
  Ryan Hafen
}
\seealso{
  \code{\link{rhwrite}}, \code{\link{rhmkdir}}
}

