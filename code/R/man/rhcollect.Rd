\name{rhcollect}
\alias{rhcollect}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	 Send Key,Value Pair To The Hadoop System. 
}
\description{
	From within a map or reduce expression send key,value pairs to Hadoop during a MapReduce job.
}
\usage{
rhcollect(key,value)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{key}{
	key sent to Hadoop
	
}
  \item{value}{
	value sent to Hadoop.
}


}
\details{

Called with two R objects. Sends a key,value pair to the Hadoop system. In the
Map phase, it will pass it on for reduction if  \code{mapred.reduce.tasks} is
not zero (by default it is non zero) or it will be written to disk if
\code{mapred.reduce.tasks} is zero. In the Reduce phase, it will be sent for further
reduction if \code{reduce} is being run as a combiner or it will be written to
the final destination if it is being run as the reducer. Don't forget to use this in the Map - if not called, nothing will be sent to the reducer.


Not all R objects may be key or value arguments. The key and value arguments must be compatible with \code{\link{rhsz}} (See details there).   In general, rhcollect is fairly permissive in what it allows sent to Hadoop, but specifically,  closures, environments and promises cannot be serialized with rhsz, and thus are not valid arguments to rhcollect.  


}

\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~



% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{MapReduce}
