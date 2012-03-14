\name{rhoptions}
\alias{rhoptions}
\title{Get or Set Rhipe Options}
\usage{
  rhoptions(li = NULL, ...)
}
\arguments{
  \item{li}{A list of options.  Names of elements indicate
  the option being modified.  Values of elements indicate
  the value the option is to take on.  If NULL then a list
  of current rhoptions is returned.  See details.}

  \item{\ldots}{Options maybe assigned values as key=value.
  See examples.}
}
\value{
  rhoptions() returns a list of current options.
}
\description{
  Used to set Rhipe options (called by \code{rhinit} for
  example).  Most often called by a user to set a
  \emph{runner} that starts the embedded R Rhipe
  executable.
}
\details{
  One may set the following options specific to Rhipe and
  many options specific to mapred (not listed here but
  present in the Hadoop documentation).
}
\note{
  Default values can be observed by simply typing
  \code{rhoptions()}

  \itemize{ \item runner This is the launcher for the
  embedded R Rhipe executable used in MapReduce jobs. }

  Often other options are easiest to set in the
  \code{mapred} argument of \code{\link{rhmr}}.
}
\examples{
#RUNNABLE BUT LIKELY DOES NOT APPLY TO MOST USERS:
	#sets the runner to be a shell script in the HOME directory.
	#NOT RUN
	#rhoptions(runner=paste(Sys.getenv("HOME"),"rhipe.runner.sh", sep="/"))
	#list all the options
	#rhoptions()

	#END NOT RUN
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhinit}}, \code{\link{rhmr}}
}

