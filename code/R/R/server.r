rhinit <- function(errors=FALSE, info=FALSE,path=NULL,cleanup=TRUE,bufsize=as.integer(3*1024*1024)){
  ntimeout <- options("timeout")[[1]]
  options(timeout = if(!is.null(rhoptions()$timeout)) as.integer(rhoptions()$timeout) else 15552000L)
  on.exit({
    options(timeout = ntimeout)
    unlink(r)
    unlink(r2)
  })
  f1 <- "localhost"
  r <- tempfile();r2 <- tempfile()
  if(is.null(path))
    cmda <- paste(c("$HADOOP/bin/hadoop jar ",rhoptions()$jarloc,"org.godhuli.rhipe.PersonalServer",f1,r,r2),collapse=" ")
  else cmda <- path
  j <- .Call("createProcess", cmda, c(as.integer(errors),as.integer(info)),as.integer(bufsize))
  ## This is a potential race here, the child starts the Java server
  ## but before it even starts we arrive here ...
  ## so we busy wait
  ## to fix this I simply need to read from the Java standard output.
  ## will implement one day
  while(TRUE){
    if(!is.na(file.info(r2)[1,]$size))
      break
  }
  x <- read.table(r,head=TRUE)
  y <- new.env()
  y$ports <- x
  y$tojava <- socketConnection(f1,as.numeric(y$ports['fromR']),open='wb',blocking=TRUE)
  y$fromjava <- socketConnection(f1,as.numeric(y$ports['toR']),open='rb',blocking=TRUE)
  y$err <- socketConnection(f1,as.numeric(y$ports['err']),open='rb',blocking=TRUE)

  reg.finalizer(y, function(r){
    if(cleanup) {
      if(!is.null(rhoptions()$quiet) && !rhoptions()$quiet)
         cat(sprintf("RHIPE: Cleaning up  associated server (PID=%s)\n",r$ports['PID']));
      tryCatch({writeBin(as.integer(-1),con=r$tojava,endian="big")},error=function(e) {},warning=function(e){})
      for(x in list(r$tojava, r$fromjava,r$err)) tryCatch(close(x),error=function(e){})
      system(sprintf("kill -9 %s", r$ports['PID']))
    }
  },onexit=TRUE)
  if(is.null(errors)) errors <- FALSE
  if(is.null(info)) info <- FALSE
  rhoptions(child=list(errors=errors,info=info,handle=y,bufsize=bufsize))
}
 
isalive <- function(z) {
  tryCatch({
    writeBin(as.integer(0),con=z$tojava,endian="big")
    o <- readBin(con=z$fromjava,what=raw(),n=1,endian="big")
    if(length(o) > 0  && o==0x01) TRUE else FALSE
  },error=function(e){
    return(FALSE)
  })
}
         

restartR <- function(){
    z <- rhoptions()$child$hdl
    rm(z);gc()
    if(!is.null(rhoptions()$quiet) && !rhoptions()$quiet)
      warning("RHIPE: restarting server")
    rhinit(errors = rhoptions()$child$errors,info=rhoptions()$child$info)
    z <- rhoptions()$child$hdl
}

send.cmd <- function(z,command, getresponse=TRUE,continuation=NULL...){

  if(!Rhipe:::isalive(z)){
    rm(z);gc()
    if(!is.null(rhoptions()$quiet) && !rhoptions()$quiet)
      warning("RHIPE: Creating a new RHIPE connection object, previous one died!")
    rhinit(errors = rhoptions()$child$errors,info=rhoptions()$child$info)
    z <- rhoptions()$child$handle
  }
  ## browser()
  command <- rhsz.1(command)
  writeBin(length(command),z$tojava, endian='big')
  writeBin(command, z$tojava, endian='big')
  if(getresponse){
    sz <- readBin(z$fromjava,integer(),n=1,endian="big")
    if(sz<0) {
      resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
      resp <- rhuz.1(resp)
      stop(resp)
    }
    resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
    resp <- rhuz.1(resp)
    nx <- unclass(resp)
    return(nx)
  }
  if(!is.null(continuation)) return(continuation())
}

rhmropts.1 <- function(){
  ## List of files,
  v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhmropts"))
  v
}

rhsz.1 <- function(r) .Call("serializeUsingPB",r)

rhuz.1 <- function(r) .Call("unserializeUsingPB",r)

rhcp.1 <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
           sep=.Platform$file.sep), "fs", "-cp", ifile, ofile, sep=" "))
  ## v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhcp",ifile, ofile))
}

rhmv.1 <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
           sep=.Platform$file.sep), "fs", "-mv", ifile, ofile, sep=" "))
  ## v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhmv",ifile, ofile))
}



rhls.1 <- function(folder,recurse=FALSE){
  ## List of files,
  v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhls",folder, if(recurse) 1L else 0L))
  if(is.null(v)) return(NULL)
  f <- as.data.frame(do.call("rbind",sapply(v,strsplit,"\t")),stringsAsFactors=F)
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f$size <- as.numeric(f$size)
  unique(f)
}

rhdel.1 <- function(folder){
  x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhdel",folder))
  x[[1]]=="OK"
}

rhget.1 <- function(src, dest){
  x <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhget",src,dest))
}

rhput.1 <- function(src, dest,deletedest=TRUE){
  x <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhput",src,dest,as.logical(deletedest)))
}


rhread.1 <- function(files,type=c("sequence"),max=-1L,mc=FALSE,asraw=FALSE,size=3000,buffsize=1024*1024,...){
  type = match.arg(type,c("sequence","map","text"))
  files <- switch(type,
                  "text"={
                    unclass(rhls.1(files)['file'])$file
                  },
                  "sequence"={
                    unclass(rhls.1(files)['file'])$file
                  },
                  "map"={
                    uu=unclass(rhls.1(files,rec=TRUE)['file'])$file
                    uu[grep("data$",uu)]
                  })
  remr <- c(grep("/_logs",files))
  if(length(remr)>0)
    files <- files[-remr]
  max <- as.integer(max)
  p <- if(type=="text"){
    Rhipe:::hmerge(files, buffsize=as.integer(buffsize),max=max,...)
  }else{
    Rhipe:::send.cmd(rhoptions()$child$handle, list("sequenceAsBinary", files,max,as.integer(rhoptions()$child$bufsize)),
                          getresponse=0L,
                          continuation = function() Rhipe:::rbstream(rhoptions()$child$handle,size,mc,asraw))
  }
  p
}

rhgetkey.1 <- function(keys,paths,sequence="",skip=0L,mc=FALSE,size=3000,...){
  pat <- rhls.1(paths)
  if (substr(pat[1, "permission"], 1, 1) != "-")  paths <- pat$file
  if (!all(is.character(paths))) 
    stop("paths must be a character vector of mapfiles( a directory containing them or a single one)")
  keys <- lapply(keys, rhsz.1)
  paths <- unlist(paths)
  p <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhgetkeys", list(keys,paths,sequence,
                   if(sequence=="") FALSE else TRUE,
                   as.integer(skip)))
           ,getresponse=0L,
           conti = function(){
             return(Rhipe:::rbstream(rhoptions()$child$handle,size,mc))
           })
  p
}

rhwrite.1 <- function(lo,dest,N=NULL){
  if(!is.list(lo))
    stop("lo must be a list")
  namv <- names(lo)
  if(is.null(N)){
    x1 <- rhoptions()$mropts$mapred.map.tasks
    x2 <- rhoptions()$mropts$mapred.tasktracker.map.tasks.maximum
    N <- as.numeric(x1)*as.numeric(x2) #number of files to write to
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
                 k <- rhsz.1(r);kl <- length(k)
                 by<<- by+kl
                 writeBin(kl,z$tojava, endian='big')
                 writeBin(k, z$tojava, endian='big')
               })
             })
             sz <- readBin(z$fromjava,integer(),n=1,endian="big")
             resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
             resp <- rhuz.1(resp)
             message(sprintf("Wrote %s pairs occupying %s bytes", length(lo), by))
             return(resp)
           })
  p[[1]]=="OK"
}

rhkill.1 <- function(x){
  if(class(x)!="jobtoken" && class(x)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(x)=="character") id <- x else {
    x <- x[[1]]
    id <- x[['job.id']]
  }
  result <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhkill", list(job.id)))
}

## print.jobtoken <- function(s,verbose=1,...){
##   r <- s[[1]]
##   v <- sprintf("RHIPE Job Token Information\n--------------------------\nURL: %s\nName: %s\nID: %s\nSubmission Time: %s\n",
##                r[1],r[2],r[3],r[4])
##   cat(v)
##   if(verbose>0){
##     result <- rhstatus.1(s)
##     cat(sprintf("State: %s\n",result[[1]]))
##     cat(sprintf("Duration(sec): %s\n",result[[2]]))
##     cat(sprintf("Progess\n"))
##     print(result[[3]])
##     if(verbose==2)
##       print(result[[4]])
##   }
## }

rhstatus.1 <- function(x){
  if(class(x)!="jobtoken" && class(x)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(x)=="character") id <- x else {
    x <- x[[1]]
    id <- x[['job.id']]
  }
  result <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhstatus", list(id)))[[1]]
  d <- data.frame("pct"=result[[3]],"numtasks"=c(result[[4]][1],result[[5]][[1]]),
                  "pending"=c(result[[4]][2],result[[5]][[2]]),
                  "running" = c(result[[4]][3],result[[5]][[3]]),
                  "complete" = c(result[[4]][4],result[[5]][[4]])
                  ,"failed" = c(result[[4]][5],result[[5]][[5]]))

  rownames(d) <- c("map","reduce")
  duration = result[[2]]
  state = result[[1]]
  return(list(state=state,duration=duration,progress=d, counters=result[[6]]));
}


rhjoin.1 <- function(x,verbose=TRUE){
  if(class(x)!="jobtoken" && class(x)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(x)=="character") id <- x else {
    x <- x[[1]]
    id <- x[['job.id']]
  }
  result <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhjoin", list(id,
                                                                          needoutput=as.character(TRUE),
                                                                          joinwordy = as.character(as.logical(verbose)))))[[1]]
  if(length(x)==2){
    ## from rhlapply
    return(x[[2]]())
  }
  return(    list(result=result[[1]], counters=result[[2]]))
}



rbstream <- function(z,size=3000,mc,asraw=FALSE){
  v <- vector(mode='list',length=size)
  i <- 0;by <- 0;ed <- 0
  while(TRUE){
    sz1 <- readBin(z$fromjava,integer(),n=1,endian="big")
    if(sz1<=0) { ed=sz1;break}
    rw.k <- readBin(z$fromjava,raw(),n=sz1,endian="big")
    sz2 <- readBin(z$fromjava,integer(),n=1,endian="big")
    if(sz2<=0) {ed = sz2;break}
    rw.v <- readBin(z$fromjava,raw(),n=sz2,endian="big")
    i <- i+1
    if(i %% size == 0) v <- append(v,vector(mode='list',length=size))
    v[[i]] <- list(rw.k,rw.v)
    by <- by+ sz1+sz2
  }
  if(ed<0) {
    rwe <- rhuz(readBin(z$fromjava,raw(),n=-ed,endian="big"))
    stop(rwe)
  }
  prs <- if(i>1) "pairs" else "pair"
  if( (by < 1024))
    message(sprintf("RHIPE: Read %s %s occupying %s bytes, deserializing", i,prs,by))
  else if( (by < 1024*1024))
    message(sprintf("RHIPE: Read %s %s occupying %s KB, deserializing", i,prs, round(by/1024,3)))
  else
    message(sprintf("RHIPE: Read %s %s occupying %s MB, deserializing", i,prs, round(by/1024^2,3)))
  MCL <- if(mc) {
    require(multicore)
    mclapply
  }else lapply
  p <- v[unlist(MCL(v,function(r) !is.null(r)))]
  if (!asraw) MCL(p,function(r) list(rhuz(r[[1]]),rhuz(r[[2]]))) else p

}

rhmerge.1 <- function(inr,ou){
  system(paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",
                     sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}

hmerge <- function(inputfiles,buffsize=2*1024*1024,max=-1L,verb=FALSE){
  
  x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhcat",inputfiles,as.integer(buffsize),as.integer(max)),
                        getresponse=0L,conti=function(){
                     k <- length(inputfiles)
                     z <- rhoptions()$child$handle
                      su <- 0;nlines <- 0
                     byt <- c()
                     while(TRUE){
                       a=readBin(z$fromjava,integer(),n=1,endian="big")
                       if(a<0) break
                       byt <- c(byt,readBin(z$fromjava,raw(),n=a))
                       su <- su+a
                       if(verb) cat(sprintf("Read %s bytes\n", su))
                     }
                     if(verb) cat("Converting to characters\n")
                     lines <- rawToChar(byt)
                     if(verb) cat("Splitfiying\n")
                     t.t <- strsplit(lines,"\n")
                     if(verb) cat("Extracting\n")
                     t.t <- t.t[[1]]
                     if(verb) cat("As Matrix\n")
                     lines <- matrix(t.t,ncol=1)
                     nlines <- nrow(lines);
                     pfx <- if(k>1) "s" else ""
                     cat(sprintf("Read %s bytes, %s lines from %s file%s\n",prettyNum(su,big.mark = ",")
                                 ,prettyNum(nlines,big.mark = ","),prettyNum(k,big.mark = ","),pfx))
                     lines
                   })
  x
}
