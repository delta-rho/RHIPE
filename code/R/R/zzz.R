.rhipeEnv <- new.env()
vvvv <- "0.59"
attr(vvvv,"minor") <- '3'
attr(vvvv,"date") <- 'Thu May 06 21:30:15 EDT 2010'
attr(vvvv,'fortune') <- 'More are taken in by hope than by cunning.
		-- Vauvenargues'
attr(vvvv,'notes') <- c('Added rhcp and rhmv',
                        'Once again, fixed the comparator','Fixed comparators and others for rhgetkey','Fixed rhlapply which failed to read in data properly')
class(vvvv) <- "rhversion"

assign("rhipeOptions" ,list(version=vvvv) ,envir=.rhipeEnv )

.onUnload <- function(libpath){
  file("/tmp/mogo")
  print("Bye")
}
.onLoad <- function(libname,pkgname){
  require(methods)

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
  opts$cmd <- list(opt=0,ls=1,get=2,del=3,put=4,b2s=5,s2b=6,getkey=7,s2m=8,rename=9)
  ##print("WHY4")
  ## print(opts)
  opts$mropts <- doCMD(opts$cmd['opt'],opts=opts,needo=T,ignore=F,verbose=FALSE)
  assign("rhipeOptions",opts,envir=.rhipeEnv)
##  print("WHY")
}

##  c(paste(R.home(component='bin'),"/R",sep=""), rhoptions()$runner[3],"--slave","--silent","--vanilla")
