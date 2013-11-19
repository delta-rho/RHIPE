\name{rhload}
\alias{rhload}
\title{Load an RData from the HDFS.}
\usage{
  rhload(file, envir = parent.frame())
}
\arguments{
  \item{file}{Path to the .RData file on the HDFS.}

  \item{envir}{Environment in which to load the .RData
  file.}
}
\value{
  data from HDFS
}
\description{
  Calls the function load after fetching an RData file from
  the HDFS.
}
\examples{
\dontrun{

## The function is currently defined as
function (file, envir = parent.frame())
{
    on.exit({
        unlink(x)
    })
    x <- tempfile(pattern = "rhipe.load")
    rhget(file, x)
    load(x, envir)
}
}
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhsave}} , \code{rhsaveimage}
}

