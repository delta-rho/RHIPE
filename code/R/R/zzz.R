.rhipeEnv <- new.env()
vvvv <- "0.66"
attr(vvvv,"minor") <- '1'
attr(vvvv,"date") <- 'Thu Jun 16'

class(vvvv) <- "rhversion"

assign("rhipeOptions" ,list(version=vvvv) ,envir=.rhipeEnv )
Mode <-  "experimental"

.onLoad <- function(libname,pkgname){
  library.dynam("Rhipe", pkgname, libname)
  #require(methods)
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
                                    sep=.Platform$file.sep),pattern="RhipeMapReduce",full=T)
  else
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",
                                    sep=.Platform$file.sep),pattern="RhipeMapReduce",full=T)
  opts$RhipeMapReduce = opts$runner
  #RhipeMapReduce is the executable, but the simpliest way to run it is via R CMD which sets up environment variables.
  opts$runner <- c("R","CMD", opts$RhipeMapReduce ,"--slave","--silent","--vanilla") #,"--max-ppsize=100000","--max-nsize=1G")
  #opts$runner <-opts$runner[-c(1,2)]
  opts$templates <- list()
  opts$templates$scalarsummer <-  expression(
      pre={.sum <- 0},
      reduce={.sum <- .sum+ sum(unlist(reduce.values),na.rm=TRUE)},
      post = { {rhcollect(reduce.key,.sum)}} )
  opts$templates$colsummer <-  expression(
      pre={.sum <- 0},
      reduce={.sum <- .sum + apply(do.call('rbind', reduce.values),1,sum)},
      post = { {rhcollect(reduce.key,.sum)}} )
  opts$templates$rbinder <-  expression(
      pre    = { data <- list()},
      reduce = { data[[length(data) + 1 ]] <- reduce.values },
      post   = { {data <- do.call("rbind", unlist(data,recursive=FALSE));}; {rhcollect(reduce.key, data)}}
      )
  
  assign("rhipeOptions",opts,envir=.rhipeEnv)
  message("--------------------------------------------------------
| IMPORTANT: Before using Rhipe call rhinit()           |
| Rhipe will not work or most probably crash            |
--------------------------------------------------------")
}

first.run <- function(buglevel=0){
  ## if(buglevel>0) message("Initial call to personal server")
  ## Rhipe::rhinit(errors=TRUE,info=if(buglevel) TRUE else FALSE,buglevel=buglevel)
  ## rhoptions(mode = Rhipe:::Mode,mropts=rhmropts(),quiet=FALSE) # "experimental"
  ## if(buglevel>0) message("Secondary call to personal server")
  ## Rhipe::rhinit(errors=TRUE,info=if(buglevel) TRUE else FALSE,buglevel=buglevel)
  ## Sys.sleep(2)
  ## message("Rhipe first run complete")
  ## return(TRUE)

  stop("Function has been deprecated, call rhinit(first=TRUE)")
}










onload <- function(libname, pkgname){
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
                                    sep=.Platform$file.sep),pattern="RhipeMapReduce",full=T)
  else
    opts$runner <- list.files(paste(system.file(package="Rhipe"),"libs",
                                    sep=.Platform$file.sep),pattern="RhipeMapReduce",full=T)

  
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



