.rhipeEnv <- new.env()
vvvv <- "0.66"
attr(vvvv,"minor") <- '1'
attr(vvvv,"date") <- 'Thu Jun 16'

class(vvvv) <- "rhversion"

assign("rhipeOptions" ,list(version=vvvv) ,envir=.rhipeEnv )
Mode <-  "experimental"

if(TRUE){
  rhls <- rhls.1
  rhcp <- rhcp.1
  rhmv <- rhmv.1
  rhput <- rhput.1
  rhget <- rhget.1
  rhread <- rhread.1
  rhwrite <- rhwrite.1
  rhgetkey <- rhgetkey.1
  rhsz <- rhsz.1
  rhuz <- rhuz.1
  rhdel <- rhdel.1
  rhstatus <- rhstatus.1
  rhjoin <- rhjoin.1
  rhmerge <- rhmerge.1
  rhkill <- rhkill.1
  rhstreamsequence <- rhstreamsequence.1
  rhbiglm.stream.hdfs<- rhbiglm.stream.hdfs.1
}

.onLoad <- function(libname,pkgname){
  require(methods)
  onload.2(libname,pkgname)
}
onload.2 <- function(libname, pkgname){
  opts <- get("rhipeOptions",envir=.rhipeEnv)
  opts$jarloc <- list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
 
  if(Sys.getenv("HADOOP")=="" && Sys.getenv("HADOOP_BIN")=="")
  warning("Rhipe requires the HADOOP or HADOOP_BIN environment variable to be present\n $HADOOP/bin/hadoop or $HADOOP_BIN/hadoop should exists")
  
  if(Sys.getenv("HADOOP_BIN")==""){
    warning("HADOOP_BIN is missing, using $HADOOP/bin")
    Sys.setenv(HADOOP_BIN=sprintf("%s/bin",Sys.getenv("HADOOP")))
  }
  if(.Platform$r_arch!="")
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",.Platform$r_arch,
                                    sep=.Platform$file.sep),pattern="imperious.so",full=T)
  else
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",
                                    sep=.Platform$file.sep),pattern="imperious.so",full=T)
  opts$runner <- c("R","CMD", opts$runner,"--slave","--silent","--vanilla","--max-ppsize=100000",
                   "--max-nsize=1G")
  opts$runner <-opts$runner[-c(1,2)]
  assign("rhipeOptions",opts,envir=.rhipeEnv)
  ## Rhipe:::.rh.first.run()
  message("--------------------------------------------------------
| IMPORTANT: Before using Rhipe call rhinit() |
| Rhipe will not work or most probably crash            |
--------------------------------------------------------")
}

first.run <- function(buglevel=0){
  ## if(buglevel>0) message("Initial call to personal server")
  ## Rhipe::rhinit(errors=TRUE,info=if(buglevel) TRUE else FALSE,buglevel=buglevel)
  ## rhoptions(mode = Rhipe:::Mode,mropts=rhmropts.1(),quiet=FALSE) # "experimental"
  ## if(buglevel>0) message("Secondary call to personal server")
  ## Rhipe::rhinit(errors=TRUE,info=if(buglevel) TRUE else FALSE,buglevel=buglevel)
  ## Sys.sleep(2)
  ## message("Rhipe first run complete")
  ## return(TRUE)
  stop("Function has been deprecated, call rhinit(first=TRUE)")
}









onload.1 <- function(libname, pkgname){
  opts <- get("rhipeOptions",envir=.rhipeEnv)
  
  ## start server
  opts$jarloc <- list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
##   cp <- c(list.files(Sys.getenv("HADOOP"),pattern="jar$",full=T),
##           list.files(Sys.getenv("HADOOP_LIB"),pattern="jar$",full=T),
##           Sys.getenv("HADOOP_CONF_DIR"),
##           opts$jarloc
##           )
##   opts$cp <- cp
##   opts$port <- 12874
##   opts$socket <- rhGetConnection(paste(cp,collapse=":"),opts$port)
  ##  print("WHY2")
  ##$HADOOP should be such that $HADOOP/bin contains the hadoop executable
  if(Sys.getenv("HADOOP")=="") stop("Rhipe requires the HADOOP environment variable to be present")
  if(.Platform$r_arch!="")
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",.Platform$r_arch,
                                    sep=.Platform$file.sep),pattern="imperious.so",full=T)
  else
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",
                                    sep=.Platform$file.sep),pattern="imperious.so",full=T)

  
  opts$runner <- c("R","CMD", opts$runner,"--slave","--silent","--vanilla","--max-ppsize=100000",
                   "--max-nsize=1G")
##   opts$runner <- c(paste(R.home(component='bin'),"/R",sep=""), opts$runner,"--slave","--silent","--vanilla")
##  print("WHY3")

  opts$runner <-opts$runner[-c(1,2)]
  opts$cmd <- list(opt=0,ls=1,get=2,del=3,put=4,b2s=5,s2b=6,getkey=7,s2m=8,rename=9,join=10,status=11)
  ##print("WHY4")
  ## print(opts)
  opts$mropts <- doCMD(opts$cmd['opt'],opts=opts,needo=T,ignore=FALSE,verbose=FALSE)
  opts$mode <- Mode #mode = "current"
  assign("rhipeOptions",opts,envir=.rhipeEnv)
##  print("WHY")
}

##  c(paste(R.home(component='bin'),"/R",sep=""), rhoptions()$runner[3],"--slave","--silent","--vanilla")


