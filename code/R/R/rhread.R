#' Read Key/Value Pairs From The HDFS
#'
#' Reads all or a limited number of key/value pairs from HDFS files.
#' 
#' @param files Path to file or directory containing map, sequence, or
#'   text file to be read on the HDFS. This can also be the output from rhwatch(provided read=FALSE) or rhmr.
#' @param type Type of file on HDFS.  Must be "sequence", "map", or "text".
#' @param max Maximum number of key/value pairs to read for map and sequence
#'   files.  Maximum number of lines to read for text files.
#' @param mc Set to lapply by default. User can change this to \code{mclapply} for parallel lapply.
#' @param skip Files to skip while reading the hdfs.  Various installs of Hadoop add additional log
#'			info to HDFS output from MapReduce.  Attempting to read these files is not what we want to do 
#'	        in rhread.  To get around this we specify pieces of filenames to grep and remove from the read.
#'          skip is a vector argument just to have sensible defaults for a number of different systems.
#'          You can learn which if any files need to be skipped by using rhls on target directory.
#' @return For map and sequence files, a list of key, pairs of up to length
#'   MAX.  For text files, a matrix of lines, each row a line from the text
#'   files.
#' @details Reads the key,value pairs from the files pointed to by \code{files}. The
#' source \code{files} can end in a wildcard (*) e.g. \emph{/path/input/p*}
#' will read all the key,value pairs contained in files starting with \emph{p}
#' in the folder \emph{/path/input/}.
#' 
#' The parameter \code{type} specifies the format of \code{files}. This can be
#' one of \code{text}, \code{map} or \code{sequence} which imply a Text file,
#' MapFile or a SequenceFile respectively.
#' 
#' The parameter \code{max} specifies the maximum number of entries to read, by
#' default all the key,value pairs will be read.  Specifying \code{max} for
#' text files, limits the number of lines read.
#' 
#' \code{mc} is by default \code{lapply}. The user can change this to mclapply for faster throughput.
#' 
#' Data written by \code{\link{rhwrite}} can be read using \code{rhread}.
#' @author Saptarshi Guha
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhwrite}}, \code{\link{rhsave}}
#' @keywords read HDFS file
#' @export
rhread <- function(files,type=c("sequence"),max=-1,skip=rhoptions()$file.types.remove.regex,mc=lapply,...){
  if(is(files, "rhwatch"))
    files <- rhofolder(files)
  files = rhabsolute.hdfs.path(files)
  files <- getypes(files,type,skip)
  max <- as.integer(max)
  switch(type,
         "gzip" = {
           stop("GZIP not supported")
         },
         "text" = {
           rhread.text(files, max=max)
         },
         "sequence" = {
           rhread.sequence(files, max=max,mc=mc)
         })
}

rhread.text <- function(files, max){
  READER <- function(server,a,b) hdfsReadLines(a,b)
  READER.PARSE <- function(f) f
  reader.generic(files,max,READER,READER.PARSE,SIZE=nrow)
}
rhread.sequence <- function(files, max, mc){
  mc <- eval(mc)
  READER <- function(server,a,b) { server$readSequence(a,b) }
  READER.PARSE <- function(f) mc(rhuz(f), function(r) list(rhuz(r[[1]]),rhuz(r[[2]])))
  reader.generic(files,max,READER,READER.PARSE,SIZE=length)
}

reader.generic <- function(files,max,READER,READER.PARSE,SIZE){
  a1 <- proc.time()['elapsed']
  cont <- vector('list',length=length(files))
  server <- rhoptions()$server
  index <-  1
  num.to.read <- as.integer(max)
  L <- length(files)
  bytes <- 0
  if(num.to.read>0){
    while(num.to.read > 0 && index <= L ){
      f <- READER(server,files[index], num.to.read)
      bytes <- bytes+SIZE(f)
      cont[[index]] <- READER.PARSE(f)
      num.to.read <- num.to.read - SIZE(cont[[index]])
      index <- index+1L
    }
  }else{
    while(index <= L ){
      f <- READER(server,files[index], -1L)
      bytes <- bytes+SIZE(f)
      cont[[index]] <-READER.PARSE(f)
      index <- index+1
    }
  }
  v <- unlist(cont,rec=FALSE)
  a2 <-  proc.time()['elapsed']
  message(makeMessage(bytes,length(v), a2-a1))
  v
}

makeMessage <- function(b, l, d){
  units <- "KB"
  if(b< 1024*1024)
    b <- b/1024
  else if(b< 1024*1024*1024){
    units <- "MB"
    b <- b/(1024*1024)
  }else {
    units <- "GB"
    b <- b/(1024*1024*1024)
  }
  tt <- d/60
  sprintf("Read %s objects(%s %s) in %s minutes", l, round(b,2), units,round(tt,2))
}

## rhGenerator <- function(files, type=c("sequence"),blocksize=1000,skip=rhoptions()$file.types.remove.regex,mc=lapply){
##   if(is(files, "rhwatch"))
##     files <- rhofolder(files)
##   files = rhabsolute.hdfs.path(files)
##   files <- getypes(files,type,skip)
##   file.index <- 1
##   L <- length(files)
##   server <- rhoptions()$server
##   handle <- server$openSequence(files[file.index])
##   function(howmany=blocksize){
##     if(file.index > L) return(NULL)
##     res <- rhuz(server$readSequence(files[file.index], blocksize))
##     if(is.null(res))
##       file.
    





  }
