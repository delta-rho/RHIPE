
#' Write R data to the HDFS
#'
#' Takes a list of objects, found in \code{object} and writes them to the folder
#' pointed to by \code{file} which will be located on the HDFS.
#' 
#' @param object An object whose elements are written
#' @param file where to write(it is overwritten)
#' @param numfiles number of files to write to
#' @param chunks an integer specificed to chunk data frames into rows or lists into sublists
#' @details This code, will chunk a data frame(or matrix) or list into sub objects, defined by chunks
#' and then written to the HDFS across numfiles files. Thus if chunks is 10, and numfiles is 20, then
#' a data frame is divided into sub data frames of rows 10 each and written across 20 files.
#' In order to improve the R-Java switch, this is buffered, the buffer size defined by passByte(bytes).
#' @examples
#'
#' \dontrun{
#'  O=data.frame(x=1:100,y=1:100)
#' rhwrite(O,file="/user/sguha/x1", chunk=10,numperfile=3)
#' writes the above as sub data frames of 10 rows each. Each sub data frame is written to a distinct file.
#' }
#' @keywords write HDFS
#' @export
rhwrite <- function(object,file,numfiles=1,chunk=1,passByte=1024*1024*20,style='classic'){
  ## rhdel(file)
  if(style=="classic") {
     if(!(inherits(object,"list") && length(object)>=1 && length(object[[1]])==2)){
       stop("You requested 'classic' write, for that one must provide a list each element of which is a list of length 2")
     }
  }
  
  file <- rhabsolute.hdfs.path(file)
  if(any(class(object) %in% c("data.frame","matrix"))){
    writeGeneric(object, file, numfiles, chunk,passByte,LENGTH=nrow, SLICER=function(o,r) o[r,,drop=FALSE] ,style!='classic' )
  }else if(any(class(object) %in% "list")){
    writeGeneric(object, file, numfiles, chunk,passByte,LENGTH=length, SLICER=function(o,r) o[r],style!='classic')
  }else stop("Invalid Class of object")
}

writeGeneric <- function(object,file, numfiles, chunks,passByte,LENGTH,SLICER,style){
  ss <- unique(c(seq(1,LENGTH(object),by=chunks),LENGTH(object)+1))
  numperfile <- as.integer((length(ss)-1)/numfiles)
  cont <- list();byt <- 0;tot <- 0
  numelems <- 0;
  server <- rhoptions()$server
  fh <- .jnew("org/godhuli/rhipe/RHWriter",as.character(file), as.integer(numperfile),server)
  for(i in 1:(length(ss)-1)){
    ranges <- ss[i]:(ss[i+1]-1)
    chunk <- SLICER(object, ranges) #object[ranges,,drop=FALSE]
    numelems <- numelems+ LENGTH(chunk)
    cont[[ length(cont) +1 ]] <- chunk
    byt <- byt+object.size(chunk)
    tot <- tot+object.size(chunk)
    if(byt>= passByte){
      fh$write(.jbyte(rhsz(cont)),style)
      cont <- list()
      byt <- 0
      message(sprintf("Wrote %s,%s chunks, and %s elements (%s%%  complete)",aP(tot),i,numelems,round(100*numelems/LENGTH(object),2)))
    }
  }

  if(length(cont)>0){
    fh$write(.jbyte(rhsz(cont)),style)
    message(sprintf("Wrote %s,%s chunks, and %s elements (%s%% complete)",aP(tot),i,numelems,round(100*numelems/LENGTH(object),2)))
  }
  fh$close()
}

aP <- function(b){
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
  sprintf("%s %s", round(b,2), units)
}
