\name{rhsave.image}
\alias{rhsave.image}
\title{save.image to HDFS}
\usage{
  rhsave.image(..., file)
}
\arguments{
  \item{\ldots}{additional parameters for
  \code{save.image}}

  \item{file}{Absolute path to file on HDFS.  Creates the
  file or overwrites it.}
}
\value{
  NULL
}
\description{
  Puts the result of a \code{save.image} call unto the
  HDFS.  Useful if you have variables in the current
  environment you want to work with in a MapReduce as a
  shared object.
}
\examples{
\dontrun{

## The function is currently defined as
function (..., file)
{
    on.exit({
        unlink(x)
    })
    x <- tempfile(pattern = "rhipe.save")
    save.image(file = x, ...)
    rhput(src = x, dest = file)
 }

}
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhsave}} , \code{\link{rhload}}
}

