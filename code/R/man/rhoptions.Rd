\name{rhoptions}
\alias{rhoptions}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Get or Set Rhipe Options 
}
\description{
	Used to set Rhipe options (called by \code{rhinit} for example).  Most often called by a user to set a \emph{runner} that starts the embedded R Rhipe executable.
}
\usage{
rhoptions(li = NULL, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{li}{
	A list of options.  Names of elements indicate the option being modified.  Values of elements indicate the value the option is to take on.  If NULL then a list of current rhoptions is returned.
	See details.
	}

}
\details{
   One may set the following options specific to Rhipe and many options specific to mapred (not listed here but present in the Hadoop documentation).
   
   Default values can be observed by simply typing \code{rhoptions()} 
   
\itemize{
\item runner

This is the launcher for the embedded R Rhipe executable used in MapReduce jobs.

}

Often other options are easiest to set in the \code{mapred} argument of \code{\link{rhmr}}.

}





   
   

\value{
	rhoptions() returns a list of current options.
}

\author{
	Saptarshi Guha
}
\seealso{
 \code{\link{rhinit}},  \code{\link{rhmr}}
}
\examples{
	#RUNNABLE BUT LIKELY DOES NOT APPLY TO MOST USERS:
	#sets the runner to be a shell script in the HOME directory.
	#NOT RUN
	rhoptions(runner=paste(Sys.getenv("HOME"),"rhipe.runner.sh", sep="/"))
	#list all the options
	rhoptions()
	
	#END NOT RUN

}

