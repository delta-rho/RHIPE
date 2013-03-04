.rhipeEnv <- new.env()
vvvv <- "0.73"
attr(vvvv,"minor") <- '0'
attr(vvvv,"date") <- 'Sunday 20th January'

class(vvvv) <- "rhversion"

assign("rhipeOptions" ,list(version=vvvv) ,envir=.rhipeEnv )


.onLoad <- function(libname,pkgname){
  library.dynam("Rhipe", pkgname, libname)
  onload.2(libname,pkgname)
}

onload.2 <- function(libname, pkgname){
  opts <- get("rhipeOptions",envir=.rhipeEnv)
  ## ##############################################################################################
  ## JAVA AND HADOOP
  ## #############################################################################################
  opts$jarloc <- list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="Rhipe.jar$",full=T)
  opts$mycp <-  list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
  opts$mycp <- setdiff(opts$mycp, opts$jarloc)
  if(Sys.getenv("HADOOP")=="" && Sys.getenv("HADOOP_HOME")=="" && Sys.getenv("HADOOP_BIN")=="")
    warning("Rhipe requires HADOOP_HOME or HADOOP or HADOOP_BIN environment variable to be present\n $HADOOP/bin/hadoop or $HADOOP_BIN/hadoop should exist")
  if(Sys.getenv("HADOOP_BIN")==""){
    warning("Rhipe: HADOOP_BIN is missing, using $HADOOP/bin")
    Sys.setenv(HADOOP_BIN=sprintf("%s/bin",Sys.getenv("HADOOP")))
  }

  if(Sys.getenv("HADOOP_HOME")=="")
    warning("HADOOP_HOME missing")
  if(Sys.getenv("HADOOP_CONF_DIR")=="")
    warning("HADOOP_CONF_DIR missing, you are probably going to have a problem running RHIPE.\nHADOOP_CONF_DIR should be the location of the directory that contains the configuration files")
  opts$hadoop.env <-Sys.getenv(c("HADOOP_HOME","HADOOP_CONF_DIR"))
  ## ##############################################################################################
  ## RhipeMapReduce, runner, and checks
  ## ##############################################################################################
  opts$RhipeMapReduce <- list.files(paste(system.file(package="Rhipe"),"bin",sep=.Platform$file.sep),
                                    pattern="^RhipeMapReduce$",full=T)
  if(is.null(opts$RhipeMapReduce) || length(opts$RhipeMapReduce) != 1){
    warning("RhipeMapReduce executable not found in package bin folder as expected")
  }
  ##RhipeMapReduce is the executable, but the simpliest way to run it is via R CMD which sets up environment variables.
  opts$runner <-paste("R","CMD", opts$RhipeMapReduce ,"--slave","--silent","--vanilla") #,"--max-ppsize=100000","--max-nsize=1G")
	
  ## ##############################################################################################
  ## OTHER DEFAULTS
  ## ##############################################################################################
  opts$file.types.remove.regex     ="(/_SUCCESS|/_LOG|/_log|rhipe_debug|rhipe_merged_index_db)"
  opts$max.read.in.size <- 200*1024*1024 ## 100MB
  opts$reduce.output.records.warn <- 200*1000
  opts$rhmr.max.records.to.read.in <- NA
  opts$HADOOP.TMP.FOLDER <- "/tmp"
  opts$readback <- TRUE
  opts$zips <- c()
  opts$hdfs.working.dir = "/"
  ## other defaults
  opts$copyObjects <- list(auto=TRUE,maxsize=100*1024*1024, exclude=c(".Random.seed","map.values","map.keys","reduce.values","reduce.key","rhcollect","rng"))
  opts$templates <- list()
  opts$templates$scalarsummer <-  expression(
      pre={.sum <- 0},
      reduce={.sum <- .sum+ sum(unlist(reduce.values),na.rm=TRUE)},
      post = { {rhcollect(reduce.key,.sum)}} )
  opts$templates$scalarsummer <- structure(opts$templates$scalarsummer,combine=TRUE)
  opts$templates$colsummer <-  expression(
      pre={.sum <- 0},
      reduce={.sum <- .sum + apply(do.call('rbind', reduce.values),2,sum)},
      post = { {rhcollect(reduce.key,.sum)}} )
  opts$templates$colsummer <- structure(opts$templates$colsummer,combine=TRUE)
  
  opts$templates$rbinder <-  function(r=NULL,combine=FALSE,dfname='adata'){
    ..r <- substitute(r)
    r <- if( is(..r,"name")) get(as.character(..r)) else ..r
    def <- if(is.null(r)) TRUE else FALSE
    r <- if(is.null(r)) substitute({rhcollect(reduce.key, adata)}) else r
    y <-bquote(expression(
        pre    = { adata <- list()},
        reduce = { adata[[length(adata) + 1 ]] <- reduce.values },
        post   = {
          adata <- do.call("rbind", unlist(adata,recursive=FALSE));
          .(P)
        }), list(P=r))
    y <- if(combine || def) structure(y,combine=TRUE) else y
    environment(y) <- .BaseNamespaceEnv
    y
  }
  opts$templates$raggregate <-  function(r=NULL,combine=FALSE,dfname='adata'){
    ..r <- substitute(r)
    ..r <- if( is(..r,"name")) get(as.character(..r)) else r
    def <- if(is.null(..r)) TRUE else FALSE
    r <- if(is.null(..r)) substitute({ adata <- unlist(adata, recursive = FALSE);  rhcollect(reduce.key, adata)}) else ..r
    y <-bquote(expression(
        pre    = { adata <- list()},
        reduce = { adata[[length(adata) + 1 ]] <- reduce.values },
        post   = {
          .(P)
        }), list(P=r))
    y <- if(combine || def) structure(y,combine=TRUE) else y
    environment(y) <-.BaseNamespaceEnv ## Using GlobalEnv screws thing sup ...
    
    y
  }
  opts$templates$identity <-  expression(reduce={ lapply(reduce.values,function(r) rhcollect(reduce.key,r)) })
  opts$templates$range <-  expression(
      pre = {
        rng <- c(Inf,-Inf)
      },
      reduce = {
        rx <- unlist(reduce.values)
        rng <- c(min(rng[1],rx,na.rm=TRUE),max(rng[2],rx,na.rm=TRUE))
      },
      post={rhcollect(reduce.key,rng)}
      )
  opts$templates$range <- structure(opts$templates$range,combine=TRUE)
  opts$debug <- list()
  opts$debug$map <- list()
  opts$debug$map$collect <- list(setup= expression({
    rhAccumulateError <- local({
      maxm <- tryCatch(rh.max.errors,error=function(e) 20)
      x <- function(maximum.errors=maxm){
        errors <- list()
        maximum.errors <- maximum.errors
        counter <- 0
        return(function(X,retrieve=FALSE){
          if(retrieve) return(errors)
          if(counter<maximum.errors){
            counter <<- counter+1
            errors[[counter]] <<- X
          }
        })}
      x()
    })
  }), cleanup =  expression({
    rhipe.errors=rhAccumulateError(ret=TRUE)
    if(length(rhipe.errors)>0){
      save(rhipe.errors,file=sprintf("./tmp/rhipe_debug_%s",Sys.getenv("mapred.task.id")))
      rhcounter("@R_DebugFile","saved.files",1)
    }
  }), handler=function(e,k,r){
    rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1)
    rhAccumulateError(list(as.character(e),k,r))
  })
  opts$debug$map$count <- list(setup=NA, cleanup=NA, handler=function(e,k,r) rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1))
  opts$debug$map[["stop"]] <- list(setup=NA, cleanup=NA, handler=function(e,k,r)  rhcounter("R_ERRORS", as.character(e),1))
  
  ## #####################
  ## Handle IO Formats
  ## ######################
  opts <- handleIOFormats(opts)
  
  ## ##############################################################################################
  ## FINSHING
  ## #############################################################################################
  
  assign("rhipeOptions",opts,envir=.rhipeEnv)
  ## initialize()
  a <- "| Please call rhinit() else RHIPE will not run |"
  a <- sprintf("%s\n%s\n%s",paste(rep("-",nchar(a)),collapse=""),a,paste(rep("-",nchar(a)),collapse=""))
  message(a)
}

#' Initializes the RHIPE subsystem
#'
#' 
#' @export 
rhinit <- function(){
  opts <- rhoptions()
  hadoop <- opts$hadoop.env
  library(rJava)
  c1 <- list.files(hadoop["HADOOP_HOME"],pattern="jar$",full=T,rec=TRUE)
  c2 <- hadoop["HADOOP_CONF_DIR"]
  .jinit()
  .jaddClassPath(c(c2,c1,opts$jarloc,opts$mycp)) #,hbaseJars,hbaseConf))
  message(sprintf("Initializing Rhipe v%s",vvvv))
  server <-  .jnew("org/godhuli/rhipe/PersonalServer")
  dbg <- as.integer(Sys.getenv("RHIPE_DEBUG_LEVEL"))
  tryCatch(server$run(if(is.na(dbg)) 0L else dbg),Exception=function(e) e$printStackTrace())
  rhoptions(server=server,clz=list(fileutils = server$getFU(),filesystem = server$getFS(), config=server$getConf()))
  server$getConf()$setClassLoader(.jclassLoader())
  rhoptions(mropts = Rhipe:::rhmropts()) 
  message("Initializing mapfile caches")
  rh.init.cache()
}

#' @export 
## rhinit <- function(...){
##   warning("Defunct. Will disappear soon, no need to call this")
## }
