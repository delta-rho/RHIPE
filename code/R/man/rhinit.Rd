\name{rhinit}
\alias{rhinit}
\title{Initialize Rhipe}
\usage{
  rhinit(errors = TRUE, buglevel = 0, info = FALSE,
    path = NULL, cleanup = FALSE,
    bufsize = as.integer(3 * 1024 * 1024), first = TRUE)
}
\arguments{
  \item{errors}{If TRUE errors are printed to the display.}

  \item{info}{Depreciated. Still used internally.}

  \item{path}{Depreciated. Still used internally.}

  \item{cleanup}{Depreciated.  Still used internally.}

  \item{bufsize}{Size of buffer from which data is sent to
  and from hadoop client.}

  \item{buglevel}{The higher the number the more
  information that is outputted during Rhipe operations.
  Currently 2000 prints all information.}

  \item{first}{Depreciated. Still used internally.}
}
\value{
  TRUE if successful
}
\description{
  Necessary to call before using Rhipe.  Rhipe likely will
  not work if any functions in the package are called
  before this.  Generally, a user just calls it with
  default arguments as \code{rhinit()}.
}
\examples{
# rhinit()   #typical use in day to day Rhipe
	# prints a slew of information; sometimes useful if Rhipe is not installed correctly.
	# rhinit(TRUE,TRUE,buglevel=2000)
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhoptions}}
}

