\name{rhput}
\alias{rhput}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
   Put a file unto the HDFS
}
\description{
Copies the local file called \code{src} (not a folder) to the destination \code{dest}
on the HDFS. Uses \code{path.expand} to expand the \code{src} parameter.
}
\usage{
rhput(src, dest)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{src}{
	Path to the local file to be copied to the HDFS.
}
  \item{dest}{
	Absolute path to the file on the HDFS.  rhput creates the file at dest.
}

}
\details{
 Local filesystem copy remains after the operation is complete.
}

\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhget}}, \code{\link{rhmv}},  \code{\link{rhdel}},  \code{\link{rhread}},  \code{\link{rhwrite}},  \code{\link{rhsave}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{put} 
\keyword{HDFS} 
\keyword{file}
