\name{rhload}
\alias{rhload}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
 Load an RData from the HDFS.
}
\description{
   Calls the function load after fetching an RData file from the HDFS.
}
\usage{
rhload(file, envir = parent.frame())
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{file}{
	Absolute path the .RData file on the HDFS.
}
  \item{envir}{
	Environment in which to load the .RData file.
}
}

\author{
Saptarshi Guha
}

\seealso{
\code{\link{rhsave}} , \code{\link{rhsaveimage}}
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

