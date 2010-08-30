

###############################
## Other functions
###############################



rhreadBin <- function(file,maxnum=-1, readbuf=0,mc=FALSE,verb=FALSE){
  sz=file.info(file[1])['size']
  x= .Call("readBinaryFile",file[1],as.integer(maxnum),as.integer(readbuf),as.logical(verb))
  if(sz < 0.9*(1024^2)) { U="kb"; pw=1}
  else if(sz < 0.9*(1024^3)) {U="mb";pw=2} else {U="gb";pw=3}
  cat(sprintf("%s %s read,unserializing, please wait\n",round(sz/(1024^pw),2),U))
  if(mc) LL=mclapply else LL=lapply
  LL(x,function(r) list(rhuz(r[[1]]),rhuz(r[[2]])))
}

rhsz <- function(r) .Call("serializeUsingPB",r)

rhuz <- function(r) .Call("unserializeUsingPB",r)

rhload <- function (file, envir=parent.frame()) 
{
    on.exit({
        unlink(x)
    })
    x <- tempfile(pattern = "rhipe.load")
    rhget(file, x)
    load(x,envir )
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

rhls <- function(fold,recurse=FALSE,ignore.stderr=T,verbose=F){
  ## List of files,
  v <- Rhipe:::doCMD(rhoptions()$cmd['ls'],fold=fold,recur=as.integer(recurse),needoutput=T,ignore.stderr=ignore.stderr,verbose=verbose)
  if(is.null(v)) return(NULL)
  if(length(v)==0) {
    warning(sprintf("Is not a readable directory %s",fold))
    return(v)
  }
  ## k <- strsplit(v,"\n")[[1]]
  ## k1 <- do.call("rbind",sapply(v,strsplit,"\t"))
  f <- as.data.frame(do.call("rbind",sapply(v,strsplit,"\t")),stringsAsFactors=F)
  rownames(f) <- NULL
  colnames(f) <- c("permission","owner","group","size","modtime","file")
  f$size <- as.numeric(f$size)
  unique(f)
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
rhgetkey <- function(keys,paths,sequence=NULL,skip=0,ignore.stderr=T,verbose=F,...){
  on.exit({
    if(dodel) unlink(tmf)
  })
  if(is.null(sequence)){
    dodel=T
    tmf <- tempfile()
  }else{
    tmf=sequence;dodel=F
  }
  pat <- rhls(paths)
  if(substr(pat[1,'permission'],1,1)!='-'){
    paths <- pat$file
  } ## if == "-", user gave a single map file folder, dont expand it
  
  if(!all(is.character(paths)))
    stop('paths must be a character vector of mapfiles( a directory containing them or a single one)')
  keys <- lapply(keys,rhsz)
  paths=unlist(paths)
  Rhipe:::doCMD(rhoptions()$cmd['getkey'], keys=keys,src=paths,dest=tmf,skip=as.integer(skip),sequence=!is.null(sequence),
                ignore.stderr=ignore.stderr,verbose=verbose)
  if(is.null(sequence)) rhreadBin(tmf,...,verb=verbose)
}
## rhgetkey(list(c("f405ad5006b8dda3a7a1a819e4d13abfdbf8a","1"),c("f2b6a5390e9521397031f81c1a756e204fb18","1")) ,"/tmp/small.seq")

##returns the data files in a directory of map files (the index files can't be sent to ##a mapreduce), which can be used for mapreduce jobs as the ifolder param
## rhmap.sqs <- function(x){
##   v=rhls(x)
##   sapply(v$file,function(r){
##     sprintf("%s/data",r)
##   },USE.NAMES=F)}

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
        N=as.integer(length(lo)),needoutput=F,ignore.stderr=ignore.stderr,verbose=verbose)
}


rhmerge <- function(inr,ou){
  system(paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}



rhkill <- function(w,...){
  if(class(w)=="jobtoken")
    w= w[[1]][['job.id']] else {
      if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
    }
  system(command=paste(paste(Sys.getenv("HADOOP"),"bin","hadoop",sep=.Platform$file.sep,collapse=""),"job","-kill",w,collapse=" "),...)
}



rhread <- function(files,type="sequence",max=-1,asraw=FALSE,ignore.stderr=T,verbose=F,mc=FALSE,debug=FALSE){
  ## browser()
  type = match.arg(type,c("sequence","map","text"))
  on.exit({
    if(!keepfile)
      unlink(tf1)
  })
  keep <- NULL
  keepfile=F
  files <- switch(type,
                  "text"={
                    unclass(rhls(files)['file'])$file
                    stop("cannot read text files")
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
  v <- Rhipe:::doCMD(rhoptions()$cmd['s2b'], infiles=files,ofile=tf1,ilocal=TRUE,howmany=max,ignore.stderr=ignore.stderr,
        verbose=verbose,rhreaddebug = debug)
  if(mc) LL=mclapply else LL=lapply
  if(!asraw) LL(v,function(r) list(rhuz(r[[1]]),rhuz(r[[2]]))) else v
}
#hread("/tmp/f")
## ffdata2=hread("/tmp/d/")

print.rhversion <- function(x,...){
  al <- paste(sapply(seq_along(attr(x,"notes")),function(y) sprintf("%s. %s",y,attr(x,"notes")[y])),collapse="\n")
  y <- sprintf("RHIPE: major is %s , minor is %s\nDate: %s\nNotes:\n%s\n", x,attr(x,"minor"),attr(x,'date'),al)
  y <- sprintf("%sEnjoy a cookie\n--------------\n%s\n",y,attr(x,'fortune'))
  attr(y, "class") <- NULL
  ## NextMethod("print", x, quote = FALSE, right = TRUE, ...)
  cat(y)
  invisible(y)
}

rhcp <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
           sep=.Platform$file.sep), "fs", "-cp", ifile, ofile, sep=" "))

}


rhmv <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP"), "bin", "hadoop",
sep=.Platform$file.sep), "fs", "-mv", ifile, ofile, sep=" "))

}

