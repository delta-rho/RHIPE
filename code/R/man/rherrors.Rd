\name{rherrors}
\alias{rherrors}
\title{Retrieves errors from a job when debug=='collect' in rhwatch}
\usage{
  rherrors(job, prefix = "rhipe_debug", num.file = 1)
}
\arguments{
  \item{job}{is the result of rhwatch (when read=FALSE) or
  rhmr}

  \item{prefix}{is the prefix string of the debug files}

  \item{num.file}{is the number of debug files to read}
}
\description{
  Retrieves errors from a job when debug=='collect' in
  rhwatch
}
\author{
  Saptarshi Guha
}

