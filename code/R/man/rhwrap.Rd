\name{rhwrap}
\alias{rhwrap}
\title{Macro to Wrap Boilerplate Around RHIPE Map code}
\usage{
  rhwrap(expr = NULL, before = NULL, after = NULL,
    trap = FALSE)
}
\arguments{
  \item{expr}{Any R expression, that operates on current
  map.keys, map.values and current index (given by
  \code{k},\code{r}, and \code{.index} respectively)}

  \item{before}{An R expression to run before the loop
  across map.values,map.keys and .index. If map.values is
  shortened, make map.keys the same length!}

  \item{after}{An R expression to run after the loop. The
  results of the loop is contained in \code{result}}

  \item{trap}{Any R error causes the MapReduce job to quit
  if set to TRUE.}
}
\description{
  Returns an expression corresponding to given input
}
\seealso{
  {\code{\link{rhmr}}
}
\keyword{code}
\keyword{Map}
\keyword{MapReduce}

