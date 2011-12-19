\name{rhread}
\alias{rhread}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
	Read Key/Value Pairs From The HDFS
}
\description{
	Reads all or a limited number of key/value pairs from HDFS files.
}
\usage{
rhread(files, type = c("sequence", "map", "text"), max = -1L, 
	mc = FALSE, asraw = FALSE, size = 3000, 
	buffsize = 1024 * 1024, quiet = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{files}{
	Absolute path to file or directory containing map, sequence, or text file to be read on the HDFS.
}
  \item{type}{
	Type of file on HDFS.  Must be "sequence", "map", or "text".
}
  \item{max}{
	Maximum number of key/value pairs to read for map and sequence files.  Maximum number of bytes to read for text files.
}
	\item{mc}{
		Setting \code{mc} to TRUE will use the the \code{multicore} package to convert the data to R
objects in parallel. The user must have first loaded \code{multicore} via call to
library. This often does accelerate the process of reading data into R.
	}


}
\details{

Reads the key,value pairs from the files pointed to by \code{files}. The source
\code{files} can end in a wildcard (*) e.g. \emph{/path/input/p*} will read all the
key,value pairs contained in files starting with \emph{p} in the folder
\emph{/path/input/}.  

The parameter \code{type} specifies the format of \code{files}. This
can be one of \code{text}, \code{map} or \code{sequence} which imply a Text file, MapFile or a
SequenceFile respectively. 

The parameter \code{max} specifies the maximum number of entries
to read, by default all the key,value pairs will be read. 
Specifying \code{max} for text files, limits the number of bytes read and is currently alpha quality.

Setting \code{mc} to TRUE will use the the \code{multicore} package to convert the data to R
objects in parallel. The user must have first loaded \code{multicore} via call to
library. This often does accelerate the process of reading data into R.

Data written by \code{\link{rhwrite}} can be read using \code{rhread}. 
}
\value{
	For map and sequence files, a list of key, pairs of up to length MAX.
	For text files, a matrix of lines, each row a line from the text files.
}
\author{
	Saptarshi Guha
}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
 \code{\link{rhget}}, \code{\link{rhput}}, \code{\link{rhmv}},  \code{\link{rhdel}},   \code{\link{rhwrite}},  \code{\link{rhsave}}
}

% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{read} 
\keyword{HDFS} 
\keyword{file}
