\name{rhfmt}
\alias{rhfmt}
\title{A function that returns a function to specify input/output formats}
\usage{
  rhfmt(..., type)
}
\arguments{
  \item{...}{arguments passed to the function}

  \item{type}{the name of the function handler}

  \item{envir}{the environment that calls your function}
}
\description{
  Returns a function to spec out the input output formats
}
\details{
  the function returned must take 3 arguments
  'lines',direction(input or output), call signature see
  \code{rhoptions()$ioformats} for examples on how to write
  your own.
}

