###############################
## Other functions
###############################

rhreadBin <- function(file,maxnum=-1, readbuf=0,mc=FALSE,verb=FALSE){
  sz=file.info(file[1])['size']
  x= .Call("readBinaryFile",file[1],as.integer(maxnum),as.integer(readbuf),as.logical(verb),PACKAGE="Rhipe")
  if(sz < 0.9*(1024^2)) { U="kb"; pw=1}
  else if(sz < 0.9*(1024^3)) {U="mb";pw=2} else {U="gb";pw=3}
  cat(sprintf("%s %s read,unserializing, please wait\n",round(sz/(1024^pw),2),U))
  if(mc) LL=mclapply else LL=lapply
  LL(x,function(r) list(rhuz(r[[1]]),rhuz(r[[2]])))
}

#test!
## rhrename <- function(src,dest,delete=T,ignore.stderr=T,verbose=F){
##   Rhipe:::doCMD(rhoptions()$cmd['rename'],infiles=src,ofile=dest,ignore.stderr=T,verbose=F)
##   if(delete) rhdel(src)
## }

##returns the data files in a directory of map files (the index files can't be sent to ##a mapreduce), which can be used for mapreduce jobs as the ifolder param
## rhmap.sqs <- function(x){
##   v=rhls(x)
##   sapply(v$file,function(r){
##     sprintf("%s/data",r)
##   },USE.NAMES=F)}


rhmerge <- function(inr,ou){
  system(paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-cat",inr,">", ou,collapse=" "))
}

print.rhversion <- function(x,...){
  al <- paste(sapply(seq_along(attr(x,"notes")),function(y) sprintf("%s. %s",y,attr(x,"notes")[y])),collapse="\n")
  y <- sprintf("RHIPE: major is %s , minor is %s\nDate: %s\nNotes:\n%s\n", x,attr(x,"minor"),attr(x,'date'),al)
  y <- sprintf("%sEnjoy a cookie\n--------------\n%s\n",y,attr(x,'fortune'))
  attr(y, "class") <- NULL
  ## NextMethod("print", x, quote = FALSE, right = TRUE, ...)
  cat(y)
  invisible(y)
}

rhmv <- function(ifile, ofile) {
  system(command=paste(paste(Sys.getenv("HADOOP_BIN"),  "hadoop",
sep=.Platform$file.sep), "fs", "-mv", ifile, ofile, sep=" "))

}

############################################################################
### from old rhmr.R
############################################################################


print.jobtoken <- function(s,verbose=1,...){
  r <- s[[1]]
  v <- sprintf("RHIPE Job Token Information\n--------------------------\nURL: %s\nName: %s\nID: %s\nSubmission Time: %s\n",
               r[1],r[2],r[3],r[4])
  cat(v)
  if(verbose>0){
    result <- rhstatus(s)
    cat(sprintf("State: %s\n",result[[1]]))
    cat(sprintf("Duration(sec): %s\n",result[[2]]))
    cat(sprintf("Progess\n"))
    print(result[[3]])
    if(verbose==2)
      print(result[[4]])
  }
}


## rhsubset <- function(ifolder,ofolder,subs,inout=c('text','text'),local=T){
##   if(!is.function(subs)) stop('subs must be a function')
##   setup <- list(map=parse(text=paste("userFUN...=",paste(deparse(subs),collapse="\n"))),
##                 reduce=expression())

##   m <- expression({
##     for(x1 in 1:length(map.values)){
##       y <- userFUN...(map.keys[[x1]],map.values[[x1]])
##       if(!is.null(y))
##         rhcollect(map.keys[[x1]],y)
##   }})
##   mpr <- list(mapred.textoutputformat.separator=" ")
##   if(local) mpr$mapred.job.tracker <- 'local'
##   z <- rhmr(map=m,ifolder=ifolder,ofolder=ofolder,inout=inout,setup=setup,mapred=mpr)
##   rhex(z)
## }
## rhsubset("/tmp/small","/tmp/s",msub)

