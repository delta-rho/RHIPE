.rhipeEnv <- new.env()
vvvv <- "0.68"
attr(vvvv,"minor") <- '0'
attr(vvvv,"date") <- 'Thu May 31'

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
  ################################################################################################
  # JAVA AND HADOOP
  ################################################################################################
  
	opts$jarloc <- list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)

	if(Sys.getenv("HADOOP")=="" && Sys.getenv("HADOOP_BIN")=="")
	warning("Rhipe requires the HADOOP or HADOOP_BIN environment variable to be present\n $HADOOP/bin/hadoop or $HADOOP_BIN/hadoop should exists")

	if(Sys.getenv("HADOOP_BIN")==""){
	warning("Rhipe: HADOOP_BIN is missing, using $HADOOP/bin")
	Sys.setenv(HADOOP_BIN=sprintf("%s/bin",Sys.getenv("HADOOP")))
	}

	################################################################################################
	# RhipeMapReduce, runner, and checks
	################################################################################################
	
	opts$RhipeMapReduce <- list.files(paste(system.file(package="Rhipe"),"bin",sep=.Platform$file.sep),
										pattern="^RhipeMapReduce$",full=T)

	if(is.null(opts$RhipeMapReduce) || length(opts$RhipeMapReduce) != 1){
		warning("RhipeMapReduce executable not found in package bin folder as expected")
	}
	#RhipeMapReduce is the executable, but the simpliest way to run it is via R CMD which sets up environment variables.
	opts$runner <-paste("R","CMD", opts$RhipeMapReduce ,"--slave","--silent","--vanilla") #,"--max-ppsize=100000","--max-nsize=1G")
	
	################################################################################################
	# OTHER DEFAULTS
	################################################################################################

        opts$file.types.remove.regex     ="(/_SUCCESS|/_LOG|/_log|rhipe_debug)"
	opts$max.read.in.size <- 200*1024*1024 ## 100MB
	opts$reduce.output.records.warn <- 200*1000
	opts$rhmr.max.records.to.read.in <- NA
	opts$HADOOP.TMP.FOLDER <- "/tmp"
	opts$zips <- c()
	opts$hdfs.working.dir = "/"
	## other defaults
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
          r <- substitute(r)
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
       opts$template$range <-  expression(
           pre = {
             rng <- c(Inf,-Inf)
           },
           reduce = {
             rx <- unlist(reduce.values)
             rng <- c(min(rng[1],rx,na.rm=TRUE),max(rng[2],rx,na.rm=TRUE))
           },
           post={rhcollect(reduce.key,rng)}
           )
       opts$templates$range <- structure(opts$template$range,combine=TRUE)
       opts$debug <- list()
       opts$debug$map <- list()
       opts$debug$map$setup <- expression({
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
       })
       opts$debug$map$cleanup <- expression({
         rhipe.errors=rhAccumulateError(ret=TRUE)
         if(length(rhipe.errors)>0){
           save(rhipe.errors,file=sprintf("./tmp/rhipe_debug_%s",Sys.getenv("mapred.task.id")))
           rhcounter("@R_DebugFile","saved.files",1)
         }
       })
       opts$debug$map$handler <- list(
                                      "count"    =function(e,k,r) rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1)
                                      ,"stop"    =function(e,k,r) rhcounter("R_ERRORS",as.character(e),1)
                                      ,"collect" =function(e,k,r){
                                        rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1)
                                        rhAccumulateError(list(as.character(e),k,r))
                                      }
                                      )

                        
  ################################################################################################
  # FINSHING
  ################################################################################################
  
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










