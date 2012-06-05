rhmerge <- function(inr,ou){
	inr = rhabsolute.hdfs.path(inr)
  system(paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",
                     sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}

hmerge <- function(inputfiles,buffsize=2*1024*1024,max=-1L,type,verb=FALSE){

  type=switch(type, "text"=0L, "gzip"=1L,-1L)
  if(type<0) stop(sprintf("In reading a file, wrong value of type"))
  x <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhcat",inputfiles,as.integer(buffsize),as.integer(max),as.integer(type)),
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
