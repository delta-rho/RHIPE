\name{rhinit}
\alias{rhinit}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Initialize Rhipe 
}
\description{
 	Necessary to call before using Rhipe.  Rhipe likely will not work if any functions in the package are called before this.
 	Generally, a user just calls it with default arguments as \code{rhinit()}. 
}
\usage{
rhinit(errors = TRUE, info = FALSE, path = NULL, cleanup = FALSE, 
	bufsize = as.integer(3 * 1024 * 1024), buglevel = 0, first = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{errors}{
	If TRUE errors are printed to the display.
}


  \item{bufsize}{
	Size of buffer from which data is sent to and from hadoop client.
	
}
  \item{buglevel}{
 	The higher the number the more information that is outputted during Rhipe operations. Currently 2000 prints all information.
 	
}
}
  
\author{
	Saptarshi Guha
}
\seealso{
 \code{\link{rhoptions}}
}

\examples{
	rhinit()   #typical use in day to day Rhipe
	#prints a slew of information; sometimes useful if Rhipe is not installed correctly.
	rhinit(TRUE,TRUE,buglevel=2000) 
}
