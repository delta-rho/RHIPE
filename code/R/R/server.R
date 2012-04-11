
isalive <- function(z) {
  tryCatch({
    writeBin(as.integer(0),con=z$tojava,endian="big")
    o <- readBin(con=z$fromjava,what=raw(),n=1,endian="big")
    #if(length(o) == 0)
    #	warning("Zero length read in isalive")            #now understand why this happens (ctrl + C kills RunJar)
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
    rhinit(errors = rhoptions()$child$errors,info=rhoptions()$child$info,cleanup=TRUE,first=FALSE)
    z <- rhoptions()$child$hdl
}

send.cmd <- function(z,command, getresponse=TRUE,continuation=NULL,...){
	if(is.null(z))
		stop("Rhipe not initialized.")
  if(!Rhipe:::isalive(z)){
    # rm(z);gc()   #removed this line.  gc does almost nothing here because z is not being rm from other objects that point to it.
    if(!is.null(rhoptions()$quiet) && !rhoptions()$quiet)
      warning("RHIPE: Creating a new RHIPE connection object, previous one died!")
    rhinit(errors = rhoptions()$child$errors,info=rhoptions()$child$info,first=FALSE)
    z <- rhoptions()$child$handle	
  }
  ## browser()
  command <- rhsz(command)
  writeBin(length(command),z$tojava, endian='big')
  writeBin(command, z$tojava, endian='big')
  flush(z$tojava)
  if(getresponse){
    sz <- readBin(z$fromjava,integer(),n=1,endian="big")
    if(sz<0) {
      #abs(sz) because sendMessage in Java PersonalServer sends negative sizes on error.
      #We must read this to clean the pipe for subsequent commands.
      resp <- readBin(z$fromjava,raw(),n=abs(sz),endian="big")
      #resp <- rhuz(resp)
      #Used to report resp, but it goes to stderr too.
      stop(paste("Error response from Rhipe Java server."))
    }
    resp <- readBin(z$fromjava,raw(),n=sz,endian="big")
    resp <- rhuz(resp)
    nx <- unclass(resp)
    return(nx)
  }
  if(!is.null(continuation)) return(continuation())
}


getypes <- function(files,type,skip){
	type = match.arg(type,c("sequence","map","text","gzip"))
	files <- switch(type,
		          "text"={
		            unclass(rhls(files)['file'])$file
		          },
		          "gzip"={
		            uu=unclass(rhls(files)['file'])$file
		            uu[grep("gz$",uu)]
		          },
		          "sequence"={
		            unclass(rhls(files)['file'])$file
		          },
		          "map"={
		            uu=unclass(rhls(files,rec=TRUE)['file'])$file
		            uu[grep("data$",uu)]
		          })
	for(s in skip){
		remr <- c(grep(s,files))
		if(length(remr)>0)
			files <- files[-remr]
	}
	return(files)
}





## print.jobtoken <- function(s,verbose=1,...){
##   r <- s[[1]]
##   v <- sprintf("RHIPE Job Token Information\n--------------------------\nURL: %s\nName: %s\nID: %s\nSubmission Time: %s\n",
##                r[1],r[2],r[3],r[4])
##   cat(v)
##   if(verbose>0){
##     result <- rhstatus(s)
##     cat(sprintf("State: %s\n",result[[1]]))
##     cat(sprintf("Duration(sec): %s\n",result[[2]]))
##     cat(sprintf("Progess\n"))
##     print(result[[3]])
##     if(verbose==2)
##       print(result[[4]])
##   }
## }



rbstream <- function(z,size=3000,mc,asraw=FALSE,quiet=FALSE){
	v <- vector(mode='list',length=size)
	i <- 0;by <- 0;ed <- 0
	while(TRUE){
		sz1 <- readBin(z$fromjava,integer(),n=1,endian="big")
		if(sz1<=0) { 
			ed=sz1;
			break
		}
		rw.k <- readBin(z$fromjava,raw(),n=sz1,endian="big")
		sz2 <- readBin(z$fromjava,integer(),n=1,endian="big")
		if(sz2<=0) {
			ed = sz2;
			break
		}
		rw.v <- readBin(z$fromjava,raw(),n=sz2,endian="big")
		i <- i+1
		if(i %% size == 0) 
			v <- append(v,vector(mode='list',length=size))
		v[[i]] <- list(rw.k,rw.v)
		by <- by+ sz1+sz2
	}
	if(ed<0) {
		rwe <- rhuz(readBin(z$fromjava,raw(),n=-ed,endian="big"))
		stop(rwe)
	}
	prs <- if(i>1) "pairs" else "pair"
	if(!quiet){
		if( (by < 1024))
		  message(sprintf("RHIPE: Read %s %s occupying %s bytes, deserializing", i,prs,by))
		else if( (by < 1024*1024))
		  message(sprintf("RHIPE: Read %s %s occupying %s KB, deserializing", i,prs, round(by/1024,3)))
		else
		  message(sprintf("RHIPE: Read %s %s occupying %s MB, deserializing", i,prs, round(by/1024^2,3)))
	}
	MCL <- mc
	p <- v[unlist(MCL(v,function(r) !is.null(r)))]
	if (!asraw) 
		MCL(p,function(r) list(rhuz(r[[1]]),rhuz(r[[2]]))) 
	else 
		p

}

rhstreamsequence <- function(inputfile,type='sequence',batch=1000,quiet=TRUE,...){
  ## We can't afford the java server to crash now, else it will
  ## throw all the reads off sync
  calledcode <- rhoptions()[[".code.in"]]
  files <- Rhipe:::getypes(inputfile,type)
  index <- 1;max.file <- length(files)
  if(!quiet) cat(sprintf("Moved to file %s (%s/%s)\n", files[index],index,max.file))
  x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhopensequencefile",files[1]),getresponse=1L)
  if(x[[1]]=="OK"){
    return(list(get=function(mc=FALSE){
      quantum <- batch
      ## if (rhoptions()[[".code.in"]]!=calledcode) warning("Server has been restarted, excpect an error")
      p <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhgetnextkv", files[index],as.integer(quantum))
                               ,getresponse=0L,
                               conti = function(){
                                 return(Rhipe:::rbstream(rhoptions()$child$handle,size=quantum,mc=mc,quiet=quiet,...))
                               })
      if(length(p)==quantum) return(p)
      ## if p is of length 0, either fast forward to next file in files
      ## that is not empty! OR if already at end, return empty list
      ## also user requested quantum but we got less, so read some more
      ## p <- list()
      while(TRUE){
        index<<-index+1
        if(index> max.file) break
        if(!quiet) cat(sprintf("Moved to file %s (%s/%s)\n", files[index],index,max.file))
        x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhopensequencefile",files[index]),getresponse=1L)
        if(x[[1]]!="OK") stop(sprintf("Problem reading next in sequence %s",files[index]))
        p <- append(p,Rhipe:::send.cmd(rhoptions()$child$handle, list("rhgetnextkv", files[index],as.integer(quantum))
                               ,getresponse=0L,
                               conti = function(){
                                 return(Rhipe:::rbstream(rhoptions()$child$handle,size=quantum,mc=mc,quiet=quiet,...))
                               }))
        if(length(p)==quantum) break
      }
      return(p)
        },close=function(){
         ## if (rhoptions()[[".code.in"]]!=calledcode) warning("Server has been restarted, excpect an error")
         x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhclosesequencefile",files[index],getresponse=1L))
         }))
  }else stop(sprintf("Could not open %s for readin",inputfile))
}


rhbiglm.stream.hdfs <- function(filename,type='sequence',modifier=NULL,batch=100,...){
  a <- NULL
  index = 1
  return(function(reset=FALSE){
    if(reset){
      index<<-1
      if(!is.null(a)) a$close()
      a <<- Rhipe::rhstreamsequence(filename,type,batch,...)
      modifier(NULL,TRUE)
    }else{
      dd <- a$get()
      if(length(dd)==0) return(NULL)
      p=do.call("rbind",lapply(dd,"[[",2))
      p <- if(!is.null(modifier)) modifier(p,reset) else p
      return(p)
    }})}


scalarSummer <- expression(
    pre={ total=0 },
    reduce = { total <- total+sum(unlist(reduce.values)) },
    post = { rhcollect(reduce.key, total)}
    )
    
    
    
################################################################################################
# shutdownJavaServer
# Tries to shutdown a java process from handle
################################################################################################
javaServerCommand = function(con,command	){
	command <- rhsz(command)
	writeBin(length(command),con, endian='big')
	writeBin(command, con, endian='big')
	flush(con)
	
}



################################################################################################
# HELPER FUNCTION TO KILL THE PROCESS WITH THE HANDLE GENERATED BY .rhinit
# Tries to let the Java server shut itself off and then kill it
# Author: Jeremiah Rounds
################################################################################################
killServer = function(handle){	
	if(is.null(handle))
		stop("Handle is NULL")
	if(!is.environment(handle))
		stop("Handle must be environment.")
		
		
	killed=FALSE
	if(!is.null(handle$killed))
		killed= handle$killed
	if(killed)
		return(TRUE)
	handle$killed = TRUE
		

	#Decided best practice at the moment is to do nothing if the handle won't respond to heart beats.
	#We did nothing for a year in all cases.  Reverting to that in the case where the server is 
	#unresponsive cannot hurt compared to older versions of Rhipe.
	#necessary to wait for a response before trying to shutdown
	if(isalive(handle)){
		if(!is.null(rhoptions()$quiet) && !rhoptions()$quiet)
			cat(sprintf("Rhipe shutting down old Java server.\n")) #,handle$ports['PID']));
		#assumes it will shutdown if it is responding to heartbeats.
		try({javaServerCommand(handle$tojava, list("shutdownJavaServer"))}, silent=TRUE)
		for(x in list(handle$tojava, handle$fromjava,handle$err)) 
			try({close(x)},silent=TRUE)
	}	



	return(TRUE)

}
