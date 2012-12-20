#' Write R data to the HDFS
#'
#' Takes a list of objects, found in \code{lo} and writes them to the folder
#' pointed to by \code{dest} which will be located on the HDFS.
#' 
#' @param lo List of R objects to place on the HDFS or a data frame/matrix.
#' @param dest Path to destination directory on the HDFS.
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
#' If \code{lo} is a data frame/matrix, it will converted into a list of pairs,
#' each pair a list of the i'th row.
#' @author Saptarshi Guha
#' @return NULL
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}
#' @keywords write HDFS
#' @export
rhwrite <- function(lo,dest,N=NULL){
  dest = rhabsolute.hdfs.path(dest)
  if(!(is.list(lo) || is.data.frame(lo)))
    stop("lo must be a list or data frame")
  if(is.data.frame(lo)){
    rlo <- rownames(lo)
    if(is.null(rlo)) rlo <- 1:nrow(lo)
    lo <- lapply(1:nrow(lo),function(i) list(rlo[i], lo[i,]))
  }
  namv <- names(lo)
  if(is.null(N)){
    x1 <- rhoptions()$mropts$mapred.map.tasks
    x2 <- rhoptions()$mropts$mapred.tasktracker.map.tasks.maximum
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


#' Write R data to the HDFS
#'
#' Takes a list of objects, found in \code{lo} and writes them to the folder
#' pointed to by \code{dest} which will be located on the HDFS.
#' 
#' @param object An object whose elements are written
#' @param file where to write(it is overwritten)
#' @param numperfile number of elements per file before a new file is created
#' @param elementWriter a list with 'howmany' (a function) that returns the numbeer of elements in the object and 'mu' (a function) that writes the elements to a handle. 'mu' takes the object and handle as a parameter
#' @details This code writes NULL keys! The pairs written will be (NULL, element of object). For a list, element of object is self explanatory. For a data frame element is every row.
#' 
#' @keywords write HDFS
#' @export
rhwrite2 <- function(object,file,numperfile,elementWriter=NULL){
  dest <- rhabsolute.hdfs.path(file)
  if(any(is(object,c("character","integer", "numeric"))))
    object <- as.list(object)
  
  info <- if(!is.null(elementWriter)){
    elementWriter
  } else if(is(object, "list")){
    list(howmany = function(o) length(o)
         ,mu  = function(O,handle){
           lapply(O, function(s){
             sz <- rhsz(s)
             writeBin(length(sz), handle, endian='big')
             writeBin(sz, handle, endian='big')
           })
         })
  } else if (is(object,"data.frame")){
    list(howmany = function(o) nrow(o)
         ,mu  = function(O,handle){
           for(i in 1:nrow(O)){
             sz <- rhsz(O[i,])
             writeBin(length(sz), handle, endian='big')
             writeBin(sz, handle, endian='big')
           }})
  }
  
  p <- Rhipe:::send.cmd(rhoptions()$child$handle,list("binaryAsSequence2"
                                                      ,as.character(dest)
                                                      ,as.integer(numperfile)
                                                      ,as.integer(info$howmany(object)))
                        ,getresponse=FALSE
                        ,conti = function(){
                          z <- rhoptions()$child$handle
                          info$mu(object,z$tojava)
                          sz <- readBin(z$fromjava,integer(),n=1,endian="big")
                          resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
                          resp <- rhuz(resp)
                          return(resp)
                        })
  p[[1]]=="OK"
}
