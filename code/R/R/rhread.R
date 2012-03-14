#' Read Key/Value Pairs From The HDFS
#'
#' Reads all or a limited number of key/value pairs from HDFS files.
#' 
#' @param files Absolute path to file or directory containing map, sequence, or
#'   text file to be read on the HDFS.
#' @param type Type of file on HDFS.  Must be "sequence", "map", or "text".
#' @param max Maximum number of key/value pairs to read for map and sequence
#'   files.  Maximum number of bytes to read for text files.
#' @param asraw Return key/value pairs as Raw data type (ie not deserialized).
#' @param mc Setting \code{mc} to TRUE will use the the \code{multicore}
#'   package to convert the data to R objects in parallel. The user must have
#'   first loaded \code{multicore} via call to library. This often does
#'   accelerate the process of reading data into R.
#' @param skip Depreciated as user argument.  May still be used internally.
#' @param size Increment the return list by this amount as reading in data.
#' @param buffsize Size of byte buffer used to read in data.
#' @param quiet If FALSE prints additional information about the read to STDOUT.
#' @param \ldots Additional arguments to hmerge which is used internally for reading text and gzip files.
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
#' text files, limits the number of bytes read and is currently alpha quality.
#' 
#' Setting \code{mc} to TRUE will use the the \code{multicore} package to
#' convert the data to R objects in parallel. The user must have first loaded
#' \code{multicore} via call to library. This often does accelerate the process
#' of reading data into R.
#' 
#' Data written by \code{\link{rhwrite}} can be read using \code{rhread}.
#' @author Saptarshi Guha
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhwrite}}, \code{\link{rhsave}}
#' @keywords read HDFS file
#' @export
rhread <- function(files,type=c("sequence"),max=-1L,skip=c("/_logs"),mc=FALSE,asraw=FALSE,size=3000,buffsize=1024*1024,quiet=FALSE,...){
  files <- getypes(files,type,skip)
  max <- as.integer(max)
  p <- if(type %in% c("text","gzip") ){
    Rhipe:::hmerge(files, buffsize=as.integer(buffsize),max=max,type=type,...)
  }else{
    Rhipe:::send.cmd(rhoptions()$child$handle, list("sequenceAsBinary", files,max,as.integer(rhoptions()$child$bufsize)),
                          getresponse=0L,
                          continuation = function() Rhipe:::rbstream(rhoptions()$child$handle,size,mc,asraw,quiet))
  }
  p
}

# rhread <- function(files,type="sequence",max=-1,asraw=FALSE,ignore.stderr=T,verbose=F,mc=FALSE,debug=FALSE){
#   ## browser()
#   type = match.arg(type,c("sequence","map","text"))
#   on.exit({
#     if(!keepfile)
#       unlink(tf1)
#   })
#   keep <- NULL
#   keepfile=F
#   files <- switch(type,
#                   "text"={
#                     unclass(rhls(files)['file'])$file
#                     stop("cannot read text files")
#                   },
#                   "sequence"={
#                     unclass(rhls(files)['file'])$file
#                   },
#                   "map"={
#                     uu=unclass(rhls(files,rec=TRUE)['file'])$file
#                     uu[grep("data$",uu)]
#                   })
#   remr <- c(grep("/_logs",files))
#   if(length(remr)>0)
#     files <- files[-remr]
#   tf1<- tempfile(pattern=paste('rhread_',
#                    paste(sample(letters,4),sep='',collapse='')
#                    ,sep="",collapse=""),tmpdir="/tmp")
#   if(!is.null(keep))
#     {
#       tf2 <- keep
#       keepfile=T
#     }else{
#       tf2<- tempfile(pattern=paste(sample(letters,8),sep='',collapse=''))
#     }
#   v <- Rhipe:::doCMD(rhoptions()$cmd['s2b'], infiles=files,ofile=tf1,ilocal=TRUE,howmany=max,ignore.stderr=ignore.stderr,
#         verbose=verbose,rhreaddebug = debug)
#   if(mc) LL=mclapply else LL=lapply
#   if(!asraw) LL(v,function(r) list(rhuz(r[[1]]),rhuz(r[[2]]))) else v
# }
# #hread("/tmp/f")
# ## ffdata2=hread("/tmp/d/")
