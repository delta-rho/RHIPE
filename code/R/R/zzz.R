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
  opts$jarloc <- list.files(file.path(system.file(package="Rhipe"), "java"), pattern="Rhipe.jar$", full=TRUE)
  

  
  opts$mycp <-  list.files(file.path(system.file(package="Rhipe"), "java"), pattern="jar$", full=TRUE)
  
  # need to exclude all Rhipe jars as they are already taken care of
  mycp_exclude <- list.files(file.path(system.file(package="Rhipe"), "java"), pattern="Rhipe",full=TRUE)
  opts$mycp <- setdiff(opts$mycp, mycp_exclude)

  if(Sys.getenv("HADOOP")=="" && Sys.getenv("HADOOP_HOME")=="" && Sys.getenv("HADOOP_BIN")=="")
    cat("Rhipe requires HADOOP_HOME or HADOOP or HADOOP_BIN environment variable to be present\n $HADOOP/bin/hadoop or $HADOOP_BIN/hadoop should exist\n")
  if(Sys.getenv("HADOOP_BIN")==""){
    cat("Rhipe: HADOOP_BIN is missing, using $HADOOP/bin\n")
    Sys.setenv(HADOOP_BIN=sprintf("%s/bin",Sys.getenv("HADOOP")))
  }

  if(Sys.getenv("HADOOP_HOME")=="")
    cat("HADOOP_HOME missing\n")
  if(Sys.getenv("HADOOP_CONF_DIR")=="")
    cat("HADOOP_CONF_DIR missing, you are probably going to have a problem running RHIPE.\nHADOOP_CONF_DIR should be the location of the directory that contains the configuration files\n")
  opts$hadoop.env <-Sys.getenv(c("HADOOP_HOME","HADOOP_CONF_DIR","HADOOP_LIBS"))
  ## ##############################################################################################
  ## RhipeMapReduce, runner, and checks
  ## ##############################################################################################
  opts$RhipeMapReduce <- list.files(paste(system.file(package="Rhipe"),"bin",sep=.Platform$file.sep),
                                    pattern="^RhipeMapReduce$",full=T)
  if(is.null(opts$RhipeMapReduce) || length(opts$RhipeMapReduce) != 1){
    cat("RhipeMapReduce executable not found in package bin folder as expected\n")
  }
  ##RhipeMapReduce is the executable, but the simpliest way to run it is via R CMD which sets up environment variables.
  opts$runner <-paste("R","CMD", opts$RhipeMapReduce ,"--slave","--silent","--vanilla") #,"--max-ppsize=100000","--max-nsize=1G")
	
  ## ##############################################################################################
  ## OTHER DEFAULTS
  ## ##############################################################################################
  opts$file.types.remove.regex     ="(/_rh_meta|/_SUCCESS|/_LOG|/_log|rhipe_debug|rhipe_merged_index_db)"
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
    ..r <- if( is(..r,"name")) get(as.character(..r)) else ..r
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
  if(hadoop["HADOOP_HOME"] != "") {
    if(any(c(rhoptions()$useCDH4 ,grepl("cdh4", c(list.files(hadoop['HADOOP_HOME'])))) ,na.rm=TRUE)){
      cat("Rhipe: Detected CDH4 jar files, using RhipeCDH4.jar\n")
      opts$jarloc <- list.files(file.path(system.file(package="Rhipe"), "java"), pattern="RhipeCDH4.jar$", full=TRUE)
    }else{
      cat("Rhipe: Using Rhipe.jar file\n")
    }
  }
  library(rJava)
  c1 <- list.files(hadoop["HADOOP_HOME"],pattern="jar$",full=T,rec=TRUE)
  c15 <- tryCatch(unlist(sapply( strsplit(hadoop["HADOOP_LIBS"],":")[[1]],function(r){
    list.files(r,pattern="jar$",full=T,rec=TRUE)
  })),error=function(e) NULL)
  
  c2 <- hadoop["HADOOP_CONF_DIR"]
  .jinit(parameters=c(getOption("java.parameters"), "-Xrs"))
  ## mycp needs to come first as hadoop distros such as cdh4 have an older version jar for guava
  .jaddClassPath(c(opts$mycp, c2, c15,c1, opts$jarloc)) #,hbaseJars,hbaseConf))
  cat(sprintf("Initializing Rhipe v%s\n",vvvv))
  server <-  .jnew("org/godhuli/rhipe/PersonalServer")
  dbg <- as.integer(Sys.getenv("RHIPE_DEBUG_LEVEL"))
  tryCatch(server$run(if(is.na(dbg)) 0L else dbg),Exception=function(e) e$printStackTrace())
  rhoptions(jarloc=opts$jarloc,server=server,clz=list(fileutils = server$getFU(),filesystem = server$getFS(), config=server$getConf()))
  server$getConf()$setClassLoader(.jclassLoader())
  rhoptions(mropts = Rhipe:::rhmropts()) 
  cat("Initializing mapfile caches\n")
  rh.init.cache()
}

#' @export 
## rhinit <- function(...){
##   warning("Defunct. Will disappear soon, no need to call this")
## }
