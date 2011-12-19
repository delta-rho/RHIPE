\name{rhget}
\alias{rhget}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Copying from the HDFS
}
\description{
Moves files from the HDFS to a local directory.
}
\usage{
rhget(src, dest)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{src}{
	Absolute path to file or directory on HDFS to get.
}
  \item{dest}{
	Path to file or directory on local filesystem to create as the new copy.
}
}
\details{
Copies the files (or folder) at \code{src}, located on the HDFS to the
destination \code{dest} located on the local filesystem. If a file or folder of
the same name as \code{dest} exists on the local filesystem, it will be
deleted. The \code{dest} can contain ~ which will be expanded. The original copy of the file or folder is left on the HDFS after this operation.
}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhdel}}, \code{\link{rhmv}},  \code{\link{rhread}},  \code{\link{rhwrite}},  \code{\link{rhsave}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{get} 
\keyword{HDFS} 
\keyword{directory}
