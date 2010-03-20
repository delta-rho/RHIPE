

###############################
## Other functions
###############################



rhreadBin <- function(file,maxnum=-1, readbuf=0){
  sz=file.info(file[1])['size']
  x= .Call("readBinaryFile",file[1],as.integer(maxnum),as.integer(readbuf))
  cat(sprintf("Read binary data(%s MB), deserializing\n",round(sz/1024^2),4))
  lapply(x,function(r) list(rhuz(r[[1]]),rhuz(r[[2]])))
}

rhsz <- function(r) .Call("serializeUsingPB",r)

rhuz <- function(r) .Call("unserializeUsingPB",r)

rhload <- function(file,...){
  on.exit({unlink(x)})
  x <- tempfile(pattern='rhipe.load')
  rhget(file,x)
  load(x,...) ##Fix this..., does not load in current frame
}

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

rhls <- function(fold,ignore.stderr=T,verbose=F){
  ## List of files,
  v <- doCMD(rhoptions()$cmd['ls'],fold=fold,needoutput=T,ignore.stderr=ignore.stderr,verbose=verbose)
  if(v=="") return(NULL)
  k <- strsplit(v,"\n")[[1]]
  k1 <- do.call("rbind",sapply(k,strsplit,"\t"))
  f <- as.data.frame(do.call("rbind",sapply(k,strsplit,"\t")),stringsAsFactors=F)
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f$size <- as.numeric(f$size)
  f
}

rhget <- function(src,dest,ignore.stderr=T,verbose=F){
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
  doCMD(rhoptions()$cmd['get'],src=src,dest=dest,needout=F,ignore.stderr=ignore.stderr,verbose=verbose)
##   doGet(src,dest,if(is.null(socket)) rhoptions()$socket else socket)
}


rhdel <- function(fold,ignore.stderr=T,verbose=F){
  doCMD(rhoptions()$cmd['del'],fold=fold,needout=F,ignore.stderr=ignore.stderr,verbose=verbose)
}


rhput <- function(src,dest,deleteDest=TRUE,ignore.stderr=T,verbose=F){
  doCMD(rhoptions()$cmd['put'],locals=src,dest=dest,overwrite=deleteDest,needoutput=F
        ,ignore.stderr=ignore.stderr,verbose=verbose)
##   doCMD(src,dest,deleteDest,if(is.null(socket)) rhoptions()$socket else socket)
}

#test!
## rhrename <- function(src,dest,delete=T,ignore.stderr=T,verbose=F){
##   Rhipe:::doCMD(rhoptions()$cmd['rename'],infiles=src,ofile=dest,ignore.stderr=T,verbose=F)
##   if(delete) rhdel(src)
## }
rhgetkey <- function(keys,paths,sequence=NULL,ignore.stderr=T,verbose=F){
  on.exit({
    if(dodel) unlink(tmf)
  })
  if(is.null(sequence)){
    dodel=T
    tmf <- tempfile()
  }else{
    tmf=sequence;dodel=F
  }
  if(!all(is.character(paths)))
    stop('paths must be a character vector of mapfiles( a directory containing them or a single one)')
  keys <- lapply(keys,rhsz)
  paths=unlist(paths)
  Rhipe:::doCMD(rhoptions()$cmd['getkey'], keys=keys,src=paths,dest=tmf,sequence=!is.null(sequence),
                ignore.stderr=ignore.stderr,verbose=verbose)
  if(is.null(sequence)) rhreadBin(tmf)
}
## rhgetkey(list(c("f405ad5006b8dda3a7a1a819e4d13abfdbf8a","1"),c("f2b6a5390e9521397031f81c1a756e204fb18","1")) ,"/tmp/small.seq")

##returns the data files in a directory of map files (the index files can't be sent to ##a mapreduce), which can be used for mapreduce jobs as the ifolder param
rhmap.sqs <- function(x){
  v=rhls(x)
  sapply(v$file,function(r){
    sprintf("%s/data",r)
  },USE.NAMES=F)}

rhwrite <- function(lo,f,N=NULL,ignore.stderr=T,verbose=F){
  on.exit({
    unlink(tmf)
  })
  if(!is.list(lo))
    stop("lo must be a list")
  namv <- names(lo)

  if(is.null(N)){
    x1 <- rhoptions()$mropts$mapred.map.tasks
    x2 <- rhoptions()$mropts$mapred.tasktracker.map.tasks.maximum
    N <- as.numeric(x1)*as.numeric(x2)
  }
  if(is.null(N) || N==0 || N>length(lo)) N<- length(lo) ##why should it be zero????
  tmf <- tempfile()
  ## convert lo into a list of key-value lists
  if(is.null(namv)) namv <- as.character(1:length(lo))
  if(!(is.list(lo[[1]]) && length(lo[[1]])==2)){
    ## we just checked the first element to see if it conforms
    ## if not we convert, where keys
    lo <- lapply(1:length(lo),function(r) {
      list( namv[[r]], lo[[r]])
    })
  }
  .Call("writeBinaryFile",lo,tmf,as.integer(16384))
  doCMD(rhoptions()$cmd['b2s'],tempf=tmf,
        output=f,groupsize = as.integer(length(lo)/N),
        howmany=as.integer(N),
        N=as.integer(length(lo),needoutput=F),ignore.stderr=ignore.stderr,verbose=verbose)
}

rhS2M <- function(files,ofile,dolocal=T,ignore.stderr=F,verbose=F,keep=NULL){
  files <- unclass(rhls(files)['file'])$file
  doCMD(rhoptions()$cmd['s2m'], infiles=files,ofile=ofile,ilocal=dolocal,
        ignore.stderr=ignore.stderr,
        verbose=verbose)
}
rhM2M <- function(files,ofile,dolocal=T,ignore.stderr=F,verbose=F,keep=NULL){
  Rhipe:::doCMD(rhoptions()$cmd['s2m'], infiles=files,ofile=ofile,ilocal=dolocal,
        ignore.stderr=ignore.stderr,
        verbose=verbose)
}

rhread <- function(files,dolocal=T,ignore.stderr=T,verbose=F,keep=NULL){
  ##need to specify /f/p* if there are other
  ##files present (not sequence files)
  on.exit({
    if(!keepfile)
      unlink(tf2)
  })
  
  keepfile=F
  files <- unclass(rhls(files)['file'])$file
  tf1<- tempfile(pattern=paste('rhread_',
                   paste(sample(letters,4),sep='',collapse='')
                   ,sep="",collapse=""),tmpdir="/tmp")

  if(!is.null(keep))
    {
      tf2 <- keep
      keepfile=T
    }else{
      tf2<- tempfile(pattern=paste(sample(letters,8),sep='',collapse=''))
    }
  message("----- converting to binary -----")
  doCMD(rhoptions()$cmd['s2b'], infiles=files,ofile=tf1,ilocal=dolocal,ignore.stderr=ignore.stderr,
        verbose=verbose)
  message("------ merging -----")

  rhmerge(paste(tf1,"/p*",sep="",collapse=""),tf2)
  rhdel(tf1);
  message("------ reading binary,please wait -----")

  v <- rhreadBin(tf2)
  return(v)
}

rhmerge <- function(inr,ou){
  system(paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}



rhkill <- function(w,...){
  if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
  system(command=paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",sep=.Platform$file.sep,collapse=""),"job","-kill",w,collapse=" "),...)
}

rhwordcount <- function(infile,outfile,local=F){
  m <- expression({
    for(x in map.values){
      y <- strsplit(x," +")[[1]]
      for(w in y) rhcollect(w,T)
    }})
  r <- expression(pre={
    count <- 0
  },
      reduce={
        count <- count+sum(unlist(reduce.values))
      },
      post={
        rhcollect(reduce.key,as.integer(count))
      })
  if(local) 
  z=rhmr(map=m,reduce=r,comb=F,inout=c("text","sequence"),ifolder=infile,ofolder=outfile,mapred=list(mapred.job.tracker='local'))
  else
    z=rhmr(map=m,reduce=r,comb=F,inout=c("text","sequence"),ifolder=infile,ofolder=outfile)

  rhex(z)
  x <- rhread(paste(outfile,"/p*",sep="",collapse=""),dolocal=local)
  ##rhdel(outfile)
  return(x)
}

  
## rhSequenceToBin <- function(infile,outfile,local=F){
##     pl <- rhsz(c(infile,outfile,local*1))
##     writeBin(8L,if(is.null(socket)) rhoptions()$socket else socket,size=4,endian='big')
##     writeBin(length(pl),if(is.null(socket)) rhoptions()$socket else socket,size=4,endian='big')
##     writeBin(pl,if(is.null(socket)) rhoptions()$socket else socket,endian='big')
##     checkEx(if(is.null(socket)) rhoptions()$socket else socket)
##   }


## rhGetConnection <- function(cp,port,show=F){
##   cmd <- paste("java -cp ",paste(cp,collapse=":")," org.godhuli.rhipe.RHIPEClientDispatcher ",port,sep="",collapse="")
##   if(show)  print(cmd)
##   tryCatch({
##     opts <- rhoptions()
##     if(!is.null(opts$socket)) close(opts$socket)
##     opts$cp <- cp
##     opts$port <- port
##     suppressWarnings(sock <- socketConnection('127.0.0.1',port,open='wb',blocking=T))
##     opts$socket <- sock
##     rhsetoptions(opts)
##     message("===================================\n")
##     message(paste("Connected to existing Client Server\n"))
##     message("===================================\n")
##     return(sock)
##   },error=function(e){
##     message("=============================\n")
##     message(paste("Starting Client Server on ",port),"\n")
##     message("=============================\n")
##     system(cmd,wait=F)
##     Sys.sleep(5)
##     opts <- rhoptions()
##     if(!is.null(opts$socket)) close(opts$socket)
##     opts$cp <- cp
##     opts$port <- port
##     opts$socket <- socketConnection('127.0.0.1',port,open='wb',blocking=T)
##     rhsetoptions(opts)
##     return(opts$socket)
##   })
## }
