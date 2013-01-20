\name{rh.init.cache}
\alias{rh.init.cache}
\title{Initializes the Value and Mapfile caches}
\usage{
  rh.init.cache(mbsize = 1024 * 1024 * 100,
    openhandles = 100)
}
\arguments{
  \item{mbsize}{is the number of bytes (i.e. key/values)
  read from a MapFile cached}

  \item{openhandles}{is the number of file handles cached}
}
\description{
  Initializes the Value and Mapfile caches
}
\details{
  Keep \code{openhandles} below the maximum number of open
  sockets
}

