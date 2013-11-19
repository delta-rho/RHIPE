\name{rhcacheStats}
\alias{rhcacheStats}
\title{Returns Cache Information}
\usage{
  rhcacheStats(which = c("filehandles", "valuebytes"))
}
\arguments{
  \item{which}{is one of 'filehandles' or 'valuebytes'}
}
\description{
  Returns Cache Information
}
\details{
  This functions returns a data frame containg the cache
  statistics for the value cache and filehandle cache set
  in \code{rh.init.cache}. See
  \url{http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/cache/CacheStats.html}
  for an explanation
}

