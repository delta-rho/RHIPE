rhinit <- function(errors=TRUE, info=TRUE,path=NULL){
  f1 <- c(tempfile(pattern=c("rhipe.toworker.","rhipe.fromworker.","rhipe.error.")))
  names(f1) <- c("toj","fromj","error")
  if(is.null(path))
    cmda <- paste(c("$HADOOP/bin/hadoop jar ",rhoptions()$jarloc,"org.godhuli.rhipe.Richmond",f1),collapse=" ")
  else cmda <- path
  j <- .Call("createProcess", cmda, f1,c(as.integer(errors),as.integer(info)))
  if(!is.character(j))
    .Call("robjectForRef",j)
  else stop(j)
  if(!is.character(j)){
    x=rhuz(.Call("readSomething",j,1L))
    cat(x[[1]])
  }
  rhsetoptions(child=list(errors=errors,info=info,hdl=j))
}

cmd <- function(f,cmd,getresponse=1L,contin=NULL,...){
  p <- .Call("isalive",f)
  ret <- NULL
  if(! (is.numeric(p) && p<0)){
    rm(f);gc()
    warning("Creating a new RHIPE connection object, previous one died!")
    rhinit(errors = rhoptions()$child$errors,info=rhoptions()$child$info)
    f <- rhoptions()$child$hdl
  }
  a <- .Call("send_command_1",f,cmd,getresponse)
  if(is.raw(a) && length(a)>0){
    p <- rhuz(a)
    if(class(p) == "worker_error")
      stop(p)
    else ret <- p[[1]]
  } else if(is.character(a)) stop(a)
  else if(is.null(a)) NULL
  else {
    print(a)
    stop("RHIPE: UNKNOWN RETURN VALUE")
  }
  if(!is.null(contin))
    {
      ret <- contin()
    }
  return(ret)
}

rhls.1 <- function(folder,recurse=FALSE){
  ## List of files,
  v <- Rhipe:::cmd(rhoptions()$child$hdl,list("rhls",folder, if(recurse) 1L else 0L))
  if(is.null(v)) return(NULL)
  f <- as.data.frame(do.call("rbind",sapply(v,strsplit,"\t")),stringsAsFactors=F)
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f$size <- as.numeric(f$size)
  unique(f)
}

rhdel.1 <- function(folder){
  Rhipe:::cmd(rhoptions()$child$hdl,list("rhdel",folder))
}

rhgetkey.1 <- function(keys,paths,sequence="",skip=0L,mc=FALSE,...){
  pat <- rhls(paths)
  if (substr(pat[1, "permission"], 1, 1) != "-")  paths <- pat$file
  if (!all(is.character(paths))) 
    stop("paths must be a character vector of mapfiles( a directory containing them or a single one)")
  keys <- lapply(keys, rhsz)
  paths <- unlist(paths)
  p <- Rhipe:::cmd(rhoptions()$child$hdl, list("rhgetkeys", list(keys,paths,sequence,
                   if(sequence=="") FALSE else TRUE,
                   as.integer(skip)))
           ,getresponse=0L,
           conti = function(){
             return(.Call("rbFile",rhoptions()$child$hdl))
           })
  if(is.null(p)) stop("RHIPE: Got a null, do not know why")
  if( is.character(p)){
    stop(sprintf("RHIPE: select returned an error: %s",p))
  }else{
    if(is.raw(p)) stop(rhuz(p))
    MCL <- if(mc) mclapply else lapply
    MCL(p,function(r) list(rhuz(r[[1]]),rhuz(r[[2]])),...)
  }
}

rhread.1 <- function(files,type=c("sequence"),max=-1L,mc=FALSE){
  type = match.arg(type,c("sequence","map","text"))
  files <- switch(type,
                  "text"={
                    unclass(rhls(files)['file'])$file
                  },
                  "sequence"={
                    unclass(rhls(files)['file'])$file
                  },
                  "map"={
                    uu=unclass(rhls(files,rec=TRUE)['file'])$file
                    uu[grep("data$",uu)]
                  })
  remr <- c(grep("/_logs",files))
  if(length(remr)>0)
    files <- files[-remr]
  max <- as.integer(max)
  p <- Rhipe:::cmd(rhoptions()$child$hdl, list("sequenceAsBinary", files,max),
           getresponse=0L,
           conti = function(){
             return(.Call("rbFile",rhoptions()$child$hdl))
           })
  if(is.null(p)) stop("RHIPE: Got a null, do not know why")
  if( is.character(p))
    stop(sprintf("RHIPE: select returned an error: %s",p))
  if(is.raw(p)) stop(rhuz(p))
  MCL <- if(mc) mclapply else lapply
  MCL(p,function(r) list(rhuz(r[[1]]),rhuz(r[[2]])))
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
  p <- Rhipe:::cmd(rhoptions()$child$hdl,list("binaryAsSequence",dest,
                  groupsize,howmanyfiles,numelems),
           getresponse=0L,
           conti = function(){
                      return(.Call("wbFile",rhoptions()$child$hdl,lo,as.integer(1024*1024)))
                      })
  if(!(is.raw(p) && length(p)>0))
    stop(p)
  p <- rhuz(p)
  if(!(class(p)=="worker_result" && p[[1]] == "OK"))
    stop(p)
}
## rhwrite.1(list("a","b","c"),N=2,dest="/tmp/fofo")

rhget.1 <- function(src, dest){
  Rhipe:::cmd(rhoptions()$child$hdl, list("rhget",src,dest))
}

rhput.1 <- function(src, dest,deletedest=TRUE){
  Rhipe:::cmd(rhoptions()$child$hdl, list("rhput",src,dest,as.logical(deletedest)))
}

rhsz.1 <- function(r) .Call("serializeUsingPB",r)

rhuz.1 <- function(r) .Call("unserializeUsingPB",r)

rhcp.1 <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
           sep=.Platform$file.sep), "fs", "-cp", ifile, ofile, sep=" "))

}

rhmv.1 <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
           sep=.Platform$file.sep), "fs", "-mv", ifile, ofile, sep=" "))
}

rhmerge.1 <- function(inr,ou){
  system(paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",
                     sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}

rhkill.1 <- function(w,...){
  if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
  system(command=paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",
           sep=.Platform$file.sep,collapse=""),"job","-kill",w,collapse=" "),...)
}


print.jobtoken <- function(s,verbose=1,...){
  r <- s[[1]]
  v <- sprintf("RHIPE Job Token Information\n--------------------------\nURL: %s\nName: %s\nID: %s\nSubmission Time: %s\n",
               r[1],r[2],r[3],r[4])
  cat(v)
  if(verbose>0){
    result <- rhstatus.1(s)
    cat(sprintf("State: %s\n",result[[1]]))
    cat(sprintf("Duration(sec): %s\n",result[[2]]))
    cat(sprintf("Progess\n"))
    print(result[[3]])
    if(verbose==2)
      print(result[[4]])
  }
}

rhstatus.1 <- function(x){
  if(class(x)!="jobtoken" && class(x)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(x)=="character") id <- x else {
    x <- x[[1]]
    id <- x[['job.id']]
  }
  result <- Rhipe:::cmd(rhoptions()$child$hdl, list("rhstatus", list(id)))
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
  if(class(x)!="jobtoken") stop("Must give a jobtoken object(as obtained from rhex)")
  svd <- c(rhoptions()$child$errors,rhoptions()$child$info)
  if(verbose && (rhoptions()$child$info==FALSE || rhoptions()$child$errors==FALSE)){
    rhinit(errors=TRUE,info=TRUE)
  }
  job.id <-  x[[1]]['job.id']
  result <- Rhipe:::cmd(rhoptions()$child$hdl,list("rhjoin", list(job.id,FALSE)))
  if(length(x)==2){
    ## from rhlapply
    return(x[[2]]())
  }
  rhinit(errors=svd[1],info=svd[2])
  return(    list(result=result[[1]], counters=result[[2]]))
}
