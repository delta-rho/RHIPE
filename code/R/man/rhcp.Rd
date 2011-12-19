\name{rhcp}
\alias{rhcp}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
   Copy files (or folders) on the HDFS
	
}
\description{
Copies the file (or folder) \code{src} on the HDFS to the destination \code{dest}
also on the HDFS.
}
\usage{
rhcp(ifile, ofile)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{ifile}{
  Absolute path to be copied on the HDFS.
	
}
  \item{ofile}{
	Absolute path to place the copies on the HDFS.
}
}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhget}}, \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhdel}},  \code{\link{rhread}},  \code{\link{rhwrite}},  \code{\link{rhsave}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{copy} 
\keyword{HDFS} 
\keyword{file}
