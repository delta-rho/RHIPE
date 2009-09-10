

###############################
## Other functions
###############################


 

## 7. rhread(src=,pattern,n=) src can be a file or list of files, n is max, get list from rhls
## 8. rhlapply == rhmr(infile=tempfile,outfile=tempfile, ... )

## rhreadText <- function(file,sep="\t"){
##   d=read.table(file,header=F,sep="\t",stringsAsFactors=F,colClasses='character')
##   apply(d,1,function(r) {
##     f=lapply(strsplit(r," "),function(rr) as.raw(as.numeric(rr)))
##     l=lapply(f,rhuz);names(l)=c("key","value")
##     l
##   })}
## Was useful before, but not anymore

rhreadBin <- function(file,maxnum=-1, readbuf=0){
  .Call("readBinaryFile",file[1],as.integer(maxnum),as.integer(readbuf))
}

rhsz <- function(r) .Call("serializeUsingPB",r)

rhuz <- function(r) .Call("unserializeUsingPB",r)

rhsave <- function(...,file){
  on.exit({unlink(x)})
  x <- tempfile(pattern='rhipe.save')
  save(file=x,...)
  rhput(src=x,dest=file)
}
rhsave.image <- function(...,file){
  on.exit({unlink(x)})
  x <- tempfile(pattern='rhipe.save')
  save.image(file=x,...)
  rhput(src=x,dest=file)
}


rhput <- function(src,dest,deleteDest=TRUE){
  srcarr <- .jarray(src)
  .jcheck(silent=T);
  .jcall(rhoptions()$fsshell,"V","copyFromLocalFile",srcarr,dest,deleteDest)
  .jcheck()
}

rhget <- function(src,dest){
  ## Copies src to dest
  ## If src is a directory and dest exists,
  ## src is copied inside dest(i.e a folder inside dest)
  ## If not, src's contents is copied to a new folder called dest
  ##
  ## If source is a file, and dest exists as a dire
  ## source is copied inside dest
  ## If dest does not exits, it is copied to that file
  ## Wildcards allowed
  ## OVERWRITES!
  .jcheck()
  .jcall(rhoptions()$fsshell,"V","copyMain",src,dest)
  .jcheck();
}

rhls <- function(dir){
  ## List of files,
  .jcheck()
  v <- .jcall(rhoptions()$fsshell,"S","ls",dir[1])
  .jcheck()
  if(v=="") return(NULL);
  .jcheck()
  k <- strsplit(v,"\n")[[1]]
  k1 <- do.call("rbind",sapply(k,strsplit,"\t"))
  f <- as.data.frame(do.call("rbind",sapply(k,strsplit,"\t")),stringsAsFactors=F)
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f
}

rhdel <- function(dir){
  .jcheck()
  v <- .jcall(rhoptions()$fsshell,"V","delete",dir[1],TRUE)
  .jcheck()
}


rhwrite <- function(lo,f,N=NULL,...){
  if(!is.list(lo))
    stop("lo must be a list")
  .jcheck()
  namv <- names(lo)
  cfg <- rhoptions()$hadoop.cfg

  if(is.null(N))
    N <- as.numeric(.jcall(cfg,"S","get","mapred.map.tasks"))*
      as.numeric(.jcall(cfg,"S","get", "mapred.tasktracker.map.tasks.maximum"))
  if(is.null(N) || N==0 ) N=length(lo) ##why should it be zero????
  .jcheck()
  
  if(is.null(namv))
    namv=1:length(lo)
  splits.id <- split(1:length(lo), sapply(1:length(lo), "%%",n))
  
  .jcheck();writer <- .jnew("org.godhuli.rhipe.RHWriter");.jcheck();
  filenames <- paste(f,names(splits.id),sep=".")
  names(filenames) <- names(splits.id)
  for(id in names(splits.id)){
    x <- splits.id[[id]]
    .jcheck();.jcall(writer,"V","set",filenames[id],cfg);.jcheck()
    nameaaray <- as.vector(unlist(sapply(namv[x],function(r) {
      d <- rhsz(r)
      c(.Call("returnBytesForVInt",length(d)),d)
    },USE.NAMES=F)))
    valarray <- unlist(as.vector(sapply(lo[x],function(r){
      d <- rhsz(r)
      c(.Call("returnBytesForVInt",length(d)),d)
      },USE.NAMES=F)))
    .jcheck()
    .jcall(writer,"V","setKeyArray",as.integer(length(x)),
           .jarray(nameaaray),.jarray(valarray))
    .jcheck()
    ## browser()
    .jcall(writer,"V","doWrite"); .jcheck()
    .jcall(writer,"V","close"); .jcheck()
  }
}
    
rhread <- function(files,max=NA,batch=100,length=1000,verbose=F){
  ## browser()
  ## reads upto max - 1
  ## batch is how many to get from java side
  ## length is initial size of vector
  files <- unclass(rhls(files)['file'])$file
  cfg <- rhoptions()$hadoop.cfg
  j=vector(mode='list',length=if(is.na(max)) length else max)
  ## j=vector(mode='list')
  ibatch <- as.integer(batch)
  count <- 0
  nms <- vector(mode='character',length=length)
  fin <- F
  for(x in files){
    .jcheck();reader <- .jnew("org.godhuli.rhipe.RHReader");.jcheck();
    .jcheck();.jcall(reader,"V","set",x,cfg);.jcheck()
     numread <- 1
    ## cat("FIle=",x,"\n")
    while(numread>0){
      numread <- .jcall(reader,"I","readKVByteArray",ibatch)
      .jcheck()
      if(numread==0) break;
      if((!is.na(max) && count >=max)) {
        fin=T;break}
      rawkv <- .jcall(reader,"[B","getKVByteArray");
      off <- 1
      for(k in 1:numread){
        ## vintinfo <- .C("readVInt_from_R",rawkv[ off ], res=integer(2))$res
        vintinfo <- c(4,readBin( rawkv[ off:(off+3)], "int",n=1,endian="big"))
        off <- off+vintinfo[1]
        keydata <- rawkv[  off:(off+vintinfo[2]) ]
        kee <- rhuz(keydata)
        off <- off+vintinfo[2]
        ## nms[[ count+k ]] <- kee
        kee0=kee
        ## vintinfo <- .C("readVInt_from_R",rawkv[ off ], res=integer(2))$res
        vintinfo <- c(4,readBin( rawkv[ off:(off+3)], "int",n=1,endian="big"))

        off <- off+vintinfo[1]
        keydata <- rawkv[  off:(off+vintinfo[2]) ]
        kee <- rhuz(keydata)
        off <- off+vintinfo[2]
        j[[   count+k ]] <- list(key=kee0,value=kee)
        
      }
      count <- count+numread
      if(verbose) cat("Read ",count," items\n")
    }
    .jcheck();.jcall(reader,"V","close");.jcheck()
    if(fin) break;
  }
  j <- j[1:count];
  ## names(j) <- nms[1:count]
  j
}


## rhkill <- function(w){
##   if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
##   .jcheck()
##   jc <- .jnew("org.apache.hadoop.mapred.JobClient")
##   .jcheck()
##   jid <- .jcall("org.apache.hadoop.mapred.JobID","Lorg/apache/hadoop/mapred/JobID;",
##                 "forName",w)
##   rj <- .jcall(jc,"Lorg/apache/hadoop/mapred/RunningJob;","getJob",jid)
##   .jcheck()
##   if(!is.null(rj)) .jcall(rj,"V","killJob") else cat("No such job\n")
##   .jcheck()
## }
rhkill <- function(w,...){
  if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
  system(command=paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",sep=.Platform$file.sep,collapse=""),"job","-kill",w,collapse=" "),...)
}
