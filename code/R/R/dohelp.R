doCMD <- function(CMD=0,needoutput=F,opts=rhoptions(),verbose=T,ignore.stderr=F
                  ,fold=NA,src=NA,dest=NA,locals=NA
                  ,overwrite=NA,tempf=NA,output=NA
                  ,groupsize=NA,howmany=NA,N=NA, sequence=F,skip=0,
                  infiles=NA, ofile=NA,ilocal=NA,keys=NA,recursive='FALSE', jobid=NULL,
                  joinwordy=TRUE,rhreaddebug=FALSE){
  on.exit({unlink(tm)})
  tm <- paste(tempfile(),paste(sample(letters,5),sep="",collapse=""),sep="",collapse="")
##   cp <- opts$cp
 ##  ip <- paste("java -cp ",paste(cp,sep="",collapse=":")," ",
##               "org.godhuli.rhipe.FileUtils",sep="",collapse="")

  ip <- paste(Sys.getenv("HADOOP"),"/bin/hadoop jar ",opts$jarloc," ",
              "org.godhuli.rhipe.FileUtils",sep="",collapse="")
  
  p <- switch(as.integer(CMD),
              {
                rhsz(list(fold,recursive))
              },
              {
                rhsz(c(src,dest)) #
              },
              {
                rhsz(fold)
              },
              {
                rhsz(list(locals,dest,overwrite))
              },
              {
                rhsz(c(tempf,output,groupsize,howmany,N))
              },
              {
                rhsz(c(ofile,ilocal*1,howmany,infiles)) ##rhread
              },
              {
                rhsz(list(keys,src,dest,sequence,skip)) ##getkey
              },
              {
                rhsz(c(infiles,ofile,ilocal*1))
              },
              {
                rhsz(list(infiles,ofile)) ##rename
              },
              {
                rhsz(list(jobid,joinwordy)) ##join a running RHIPE job
              },
              {
                rhsz(list(jobid)) ##find status of a running job
              }
              )
  if(!is.null(p)){
    f <- file(tm, open='wb')
    writeBin(p,f)
    close(f);
  }    
  cmd <- paste(ip," ",CMD, " ",tm,sep='',collapse='')
  if(verbose) cat(cmd,"\n")
  if(CMD==6){ ##rhread
    if(ignore.stderr) cmd <- sprintf("%s 2>/dev/null",cmd)
    ## 
    ## system(cmd,
    ##           intern=F,ignore.stderr=ignore.stderr)
    ## cmd = ofile
    return(.Call("readSQFromPipe",cmd,as.integer(1024*1024L),rhreaddebug,PACKAGE="Rhipe"))
  }
  r <- system(cmd,
              intern=F,ignore.stderr=ignore.stderr)
  if(verbose) message("Error Code is ",r)
  if(r==256){
    f <- file(tm, open='rb')
    s <- readBin(f,'int',1,size=4,endian='big')
##     close(f)
    xx=rhuz(readBin(f,'raw',n=s))
    close(f)
    stop(xx)
  }else
  if(needoutput){
    f <- file(tm, open='rb')
    s <- readBin(f,'int',1,size=4,endian='big')
    x <- rhuz(readBin(f,'raw',n=s))
    close(f)
    return(x)
  }
}



## checkEx <- function(socket){
##   v <- readBin(socket,'raw',n=1)
##   if(v==as.raw(0x0)){
##     v <- readBin(socket,'int',n=1,size=4,endian='big')
##     v <- readBin(socket,'raw',n=v)
##     stop(rawToChar(v))
##   }
## }



## ## doDeletableFile <- function(fold,sock){
## ##   pl <- rhsz(fold)
## ##   writeBin(2L,sock,size=4,endian='big')
## ##   writeBin(length(pl),sock,size=4,endian='big')
## ##   writeBin(pl,sock,endian='big')
## ##   checkEx(sock)
## ## }

## doPut <- function(fold1,dest,bool,sock){
##   pl <- rhsz(list(fold1,dest,bool))
##   writeBin(3L,sock,size=4,endian='big')
##   writeBin(length(pl),sock,size=4,endian='big')
##   writeBin(pl,sock,endian='big')
##   checkEx(sock)
## }

## doGet <- function(fold1,fold2,sock){
##   pl <- rhsz(c(fold1,fold2))
##   writeBin(4L,sock,size=4,endian='big')
##   writeBin(length(pl),sock,size=4,endian='big')
##   writeBin(pl,sock,endian='big')
##   checkEx(sock)
## }
## doLS <- function(fold,sock){
##   pl <- rhsz(fold)
##   writeBin(5L,sock,size=4,endian='big')
##   writeBin(length(pl),sock,size=4,endian='big')
##   writeBin(pl,sock,endian='big')
##   checkEx(sock)
##   v <- readBin(sock,'int',n=1,endian='big')
##   v <- readBin(sock,'raw',n=v)
##   rhuz(v)
## }
## doDel <- function(fold,sock){
##   pl <- rhsz(fold)
##   writeBin(6L,sock,size=4,endian='big')
##   writeBin(length(pl),sock,size=4,endian='big')
##   writeBin(pl,sock,endian='big')
##   checkEx(sock)
## }
## doConvertBinToSequence <- function(infile,outfile,group,nfiles, total,sock){
##   pl <- rhsz(c(infile,outfile, group,nfiles, total))
##   writeBin(7L,sock,size=4,endian='big')
##   writeBin(length(pl),sock,size=4,endian='big')
##   writeBin(pl,sock,endian='big')
##   checkEx(sock)
## }
