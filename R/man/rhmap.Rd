\name{rhmap}
\alias{rhmap}
\title{Macro to Wrap Boilerplate Around RHIPE Map code}
\usage{
  rhmap(co1 = NULL, before = NULL, after = NULL)
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
}
\description{
  Returns an expression corresponding to given input
}
\seealso{
  \code{\link{rhmr}}
}
\keyword{Map}
\keyword{MapReduce}

