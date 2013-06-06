\name{rhsave}
\alias{rhsave}
\title{Save .RData to HDFS}
\usage{
  rhsave(..., file, envir = parent.frame())
}
\arguments{
  \item{\ldots}{additional parameters for \code{rhsave}}

  \item{file}{Absolute path to file on HDFS.  Creates the
  file or overwrites}

  \item{environment}{to search for objects to be saved it.}
}
\value{
  NULL
}
\description{
  Puts the result of a \code{save} call unto the HDFS.
  Useful if you have variables in the current environment
  you want to work with in a MapReduce as a shared object.
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
    save(file = x, ...)
    rhput(src = x, dest = file)
  }
}
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{rhsave.image}, \code{\link{rhload}}
}

