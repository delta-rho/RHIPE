\name{rhdel}
\alias{rhdel}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
HDFS File Deletion
}
\description{
This function deletes the folders contained in the character vector \code{folders}
which are located on the HDFS. The deletion is recursive, so all subfolders will
be deleted too. Nothing is returned.
}
\usage{
rhdel(folder)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{folder}{
	The absolute path on the hdfs to the directory(s) to be deleted.
}
}
\details{
Deletes all files and directories contained in the target directory.
}

\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhls}}, \code{\link{rhmv}},  \code{\link{rhread}},  \code{\link{rhwrite}},  \code{\link{rhsave}}, \code{\link{rhget}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{ delete} 
\keyword{HDFS} 
\keyword{directory}
