\name{bashRhipeArchive}
\alias{bashRhipeArchive}
\title{THIS ONLY IS EXPECTED TO WORK FOR SOMEONE USING A BASH SHELL.
Creates an archive on the HDFS with a runner script appropriate for running Rhipe jobs.
Uploads that archive to the HDFS}
\usage{
  bashRhipeArchive(archive.base.name = "RhipeLib",
    delete.local.tmp.dir = TRUE)
}
\arguments{
  \item{archive.base.name}{Base file name.  This will be
  the name of a temp directory left in the current working
  directory and the name of the tar.gz placed on the HDFS.}

  \item{delete.local.tmp.dir}{Delete the local.tmp.dir
  before starting if TRUE.}
}
\description{
  WARNING: CHANGES YOUR RUNNER AND ZIPS RHOPTIONS TO USE
  THAT ARHIVE.
}
\details{
  YOU MUST SET BOTH WORKING DIRECTORIES AND HDFS WORKING
  DIRECTORIES PRIOR TO CALLING THIS. USE setwd() AND
  hdfs.setwd() PRIOR TO CALLING THIS.
}
\examples{
\dontrun{
library("Rhipe")
rhinit()
setwd("~")    		# MUST SET THIS
hdfs.setwd("/tmp") 	# MUST SET THIS
system("rm -Rf RhipeLib_0.67")
createBashRhipeLibArchive("RhipeLib_0.67")
}
}
\author{
  Jeremiah Rounds
}

