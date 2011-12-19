\name{rhsave}
\alias{rhsave}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
 	Save .RData to HDFS
}
\description{
	Puts the result of a \code{save} call unto the HDFS.  Useful if you have variables in the current environment you want to work with in a MapReduce as a shared object.
}
\usage{
rhsave(..., file)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{\dots}{
	
}
  \item{file}{
	Absolute path to file on HDFS.  Creates the file or overwrites it.
}
}

\author{
	Saptarshi Guha
}

\seealso{
	\code{\link{rhsave.image}}, \code{\link{rhload}}
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

