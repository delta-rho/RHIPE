\name{rhls}
\alias{rhls}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
 List Files On HDFS
}
\description{
 List all files and directories contained in a directory on the HDFS.
}
\usage{
rhls(folder, recurse = FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{folder}{
	Absolute path of directory on HDFS
}
  \item{recurse}{
	If TRUE list all files and directories in sub-directories.
}
}
\details{

}
\value{
Returns a data.frame of filesystem information for the files located at \code{path}. If
\code{recurse} is TRUE, \code{rhls} will recursively travel the directory tree
rooted at \code{path}. The returned object is a data.frame consisting of the
columns: \emph{permission, owner, group, size (which is numeric), modification time},
and the \emph{file name}. \code{path} may optionally end in `*' which is the
wildcard and will match any character(s).
}

\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhdel}}, \code{\link{rhmv}},  \code{\link{rhread}},  \code{\link{rhwrite}},  \code{\link{rhsave}}, \code{\link{rhget}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{list} 
\keyword{HDFS} 
\keyword{directory}
