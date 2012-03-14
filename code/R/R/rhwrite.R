#' Write R data to the HDFS
#'
#' Takes a list of objects, found in \code{lo} and writes them to the folder
#' pointed to by \code{dest} which will be located on the HDFS.
#' 
#' @param lo List of R objects to place on the HDFS.
#' @param dest Absolute path to destination directory on the HDFS.
#' @param N See Details.
#' @details The file
#' \code{dest} will be in a format interpretable by RHIPE, i.e it can be used
#' as input to a MapReduce job. The values of the list of are written as
#' key-value pairs in a SequenceFileFormat format. \code{N} specifies the
#' number of files to write the values to. For example, if \code{N} is 1, the
#' entire list \code{list} will be written to one file in the folder
#' \code{dest}. Computations across small files do not parallelize well on
#' Hadoop. If the file is small, it will be treated as one split and the user
#' does not gain any (hoped for) parallelization. Distinct files are treated as
#' distinct splits. It is better to split objects across a number of files. If
#' the list consists of a million objects, it is prudent to split them across a
#' few files. Thus if $N$ is 10 and \code{list} contains 1,000,000 values, each
#' of the 10 files (located in the directory \code{dest}) will contain 100,000
#' values.
#' 
#' Since the list only contains values, the keys are the indices of the value
#' in the list, stored as strings. Thus when used as a source for a MapReduce
#' job, the variable \code{map.keys} will contain numbers in the range $[1,
#' length(list)]$. The variable \code{map.values} will contain elements of
#' \code{list}.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}
#' @keywords write HDFS
#' @export
rhwrite <- function(lo,dest,N=NULL){
  if(!is.list(lo))
    stop("lo must be a list")
  namv <- names(lo)
  if(is.null(N)){
    x1 <- rhoptions()$mropts[[1]]$mapred.map.tasks
    x2 <- rhoptions()$mropts[[1]]$mapred.tasktracker.map.tasks.maximum
    N <- as.numeric(x1)*as.numeric(x2) #number of files to write to
    if(is.null(N)) warning("Cannot infer N (because at least one of mapred.map.tasks and mapred.trasktracker.map.tasks.maximum is missing), defaulting to 1")
    N <- 1
  }
  if(is.null(N) || N==0 || N>length(lo))
    N<- length(lo) ##why should it be zero????
  ## convert lo into a list of key-value lists
  if(is.null(namv)) namv <- as.character(1:length(lo))
  if(!(is.list(lo[[1]]) && length(lo[[1]])==2)){
    ## we just checked the first element to see if it conforms
    ## if not we convert, where keys
    lo <- lapply(1:length(lo),function(r) {
      list( namv[[r]], lo[[r]])
    })
  }
  howmanyfiles <- as.integer(N)
  groupsize <- as.integer(length(lo)/howmanyfiles) #number per file
  numelems <- as.integer(length(lo))
  p <- Rhipe:::send.cmd(rhoptions()$child$handle,list("binaryAsSequence",dest,
                  groupsize,howmanyfiles,numelems),
           getresponse=FALSE,
           conti = function(){
             by=0
             z <- rhoptions()$child$handle
             lapply(lo,function(l){
               lapply(l,function(r){
                 k <- rhsz(r);kl <- length(k)
                 by<<- by+kl
                 writeBin(kl,z$tojava, endian='big')
                 writeBin(k, z$tojava, endian='big')
               })
             })
             sz <- readBin(z$fromjava,integer(),n=1,endian="big")
             resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
             resp <- rhuz(resp)
             message(sprintf("Wrote %s pairs occupying %s bytes", length(lo), by))
             return(resp)
           })
  p[[1]]=="OK"
}


# rhwrite <- function(lo,f,N=NULL,ignore.stderr=T,verbose=F){
#   on.exit({
#     unlink(tmf)
#   })
#   if(!is.list(lo))
#     stop("lo must be a list")
#   namv <- names(lo)
# 
#   if(is.null(N)){
#     x1 <- rhoptions()$mropts$mapred.map.tasks
#     x2 <- rhoptions()$mropts$mapred.tasktracker.map.tasks.maximum
#     N <- as.numeric(x1)*as.numeric(x2)
#   }
#   if(is.null(N) || N==0 || N>length(lo)) N<- length(lo) ##why should it be zero????
#   tmf <- tempfile()
#   ## convert lo into a list of key-value lists
#   if(is.null(namv)) namv <- as.character(1:length(lo))
#   if(!(is.list(lo[[1]]) && length(lo[[1]])==2)){
#     ## we just checked the first element to see if it conforms
#     ## if not we convert, where keys
#     lo <- lapply(1:length(lo),function(r) {
#       list( namv[[r]], lo[[r]])
#     })
#   }
#   .Call("writeBinaryFile",lo,tmf,as.integer(16384))
#   doCMD(rhoptions()$cmd['b2s'],tempf=tmf,
#         output=f,groupsize = as.integer(length(lo)/N),
#         howmany=as.integer(N),
#         N=as.integer(length(lo)),needoutput=F,ignore.stderr=ignore.stderr,verbose=verbose)
# }