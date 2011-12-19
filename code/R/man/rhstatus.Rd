\name{rhstatus}
\alias{rhstatus}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Report Status Of A MapReduce Job
}
\description{
    NOTE: This function does something different depending on if it is used in MapReduce expression during a MapReduce task.
    In a MapReduce task use this function to REPORT the status of your job to Hadoop.
    
    From the user side, this displays the status of a running MapReduce job and reports information accumulated about the Hadoop job.
}
\usage{
#From the user side:
rhstatus(x, mon.sec = 0, autokill = TRUE, showErrors = TRUE, verbose = FALSE)

#In a MapReduce expression entirely different function:
rhstatus(message)  #report status as message to Hadoop 
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
The parameter \code{x} can
either be a string with the format \emph{job_datetime_id}
(e.g. \emph{job_201007281701_0274}) or the value returned from \code{rhex} with the
\code{async} option set to TRUE.
}
  \item{mon.sec}{
If \code{mon.sec} is greater than 0, a small data frame indicating the progress will be returned every \code{mon.sec} seconds.
}
  \item{autokill}{
If \code{autokill} is TRUE, then any R errors caused by the map/reduce code will cause the job to be killed. 
}

  \item{verbose}{
If \code{verbose} is TRUE, also provided is the state of the job, the duration in seconds, a data frame with columns for the Map and Reduce phase. This data frame summarizes the number of tasks, the percent complete, and the number of tasks that are pending, running, complete or have failed. In addition the list has an element that consists of both user defined and Hadoop MapReduce built in counters (counters can be user defined with a call to \code{rhcounter}).
}
}
\details{

This returns the status of an running MapReduce job. The parameter \code{x} can
either be a string with the format \emph{job_datetime_id}
(e.g. \emph{job_201007281701_0274}) or the value returned from \code{rhex} with the
\code{async} option set to TRUE.


}
\value{
	Return depends upon values of mon.sec and verbose.
}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhex}}, \code{\link{rhmr}}, \code{\link{rhkill}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{MapReduce}
\keyword{job}
\keyword{status}
