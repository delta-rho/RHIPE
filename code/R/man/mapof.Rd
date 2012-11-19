\name{mapof}
\alias{mapof}
\title{convenience function to pass to inout[2].}
\usage{
  mapof(interval = 1, compression = "BLOCK")
}
\arguments{
  \item{interval}{Every 'interval' key to be indexed}

  \item{compression}{'NONE','BLOCK',or 'RECORD}
}
\description{
  Convenience function to pass inout[2], Sets custom
  options for map outputformat.
}
\details{
  Call this function as Rhipe:::mapof (e.g.
  Rhipe:::mapof(1,'RECORD') and pass the result to inout[2]
}

