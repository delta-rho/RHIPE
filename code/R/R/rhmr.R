#' Defunct funciton to prepare mapreduce jobs. See \code{rhwatch}
#' @export
rhmr <- function(...){
  stop("STOP! Do not call rhmr, call rhwatch with the same arguments you would have done with rhmr")
}

.rhmr <- function(map         = NULL,
                  reduce      = NULL,
                  combiner    = FALSE,
                  setup       = NULL,
                  cleanup     = NULL,
                  input       = NULL,
                  output      = NULL,
                  orderby     = 'bytes',
                  mapred      = NULL,
                  shared      = c(),
                  jarfiles    = c(),
                  zips        = c(),
                  partitioner = NULL,
                  copyFiles   = F,
                  jobname     = "",
                  parameters  = NULL,
                  envir = NULL
                  ){
  
  
  ## ##############################################################################################
  ## Now continue into the sea of code known as "lines"
  ## #############################################################################################
  
  lines <- list();
  is.Expression <- function(r) is.expression(r) || class(r)=="{"

  if(is.function(map)){
    map <- rhmap(body(map), .fnformals=formals(map))
  }
  
  if(!is.Expression(map))
    stop("'map' must be an expression")
  lines$rhipe_reduce_justcollect <- "FALSE"
  combiner <- if(!is.null(attr(reduce,"combine")) && attr(reduce,"combine"))
    combiner <- TRUE
  else combiner
  
  if(is.null(reduce)){
    combiner <- FALSE
  } else if(!(is.expression(reduce)) && (is.numeric(reduce) || is.integer(reduce))){
    ##Can't check if reduce is.na unless you make sure it is not NULL
    lines$mapred.reduce.tasks <- reduce
    reduce <- NULL
  }
  lines$rhipe_reduce         <- rawToChar(serialize(reduce$reduce,NULL,ascii=T))
  lines$rhipe_reduce_prekey  <- rawToChar(serialize(reduce$pre ,NULL,ascii=T))
  lines$rhipe_reduce_postkey <- rawToChar(serialize(reduce$post,NULL,ascii=T))

  lines$rhipe_jobname=jobname


  ## setup can either be an expression or NULL
  ## also, if an expression it can either have no components or two (map/reduce)              
  if(is.null(setup)){
    setup <- expression()
    setup$map <- expression()
    setup$reduce <- expression()
  }else if(!is.Expression(setup)){
    stop("'setup' should be an expression (named or not). See ?rhmr")
  }else if(is.Expression(setup)){
    ## is it without  names?
    if(is.null(names(setup))){
      setup <- bquote(expression(map=.(setup),reduce=.(setup)),list(setup=setup[[1]]))
    }
  }
                
  
  if(is.null(cleanup)){
    cleanup$map <- expression()
    cleanup$reduce <- expression()
  }

  if(!is.Expression(cleanup) && !is.list(cleanup))
    stop("'cleanup' is either a list of expressions (map=,redce=) or expression")

  if(is.list(cleanup)){
    if(! all(unlist(lapply(cleanup,is.Expression))))
      stop("elements of 'cleanup' must be expressions")
    if(is.null(cleanup$reduce)) cleanup$reduce <- expression()
    if(is.null(cleanup$map)) cleanup$map <- expression()
  }
  if(is.null(cleanup))
    cleanup <- expression()
  if(is.Expression(cleanup)){
    cleanup <- list(map=cleanup,reduce=cleanup)
  }
  
  lines<- append(lines,list(
                            R_HOME=R.home()
                            ,rhipe_command=paste(rhoptions()$runner,collapse=" ")
                            ))

  lines$rhipe_map <- rawToChar(serialize(map,NULL,ascii=T))
  lines$rhipe_setup_map <- setup$map
  lines$rhipe_setup_reduce <- setup$reduce
  lines$rhipe_cleanup_map <- cleanup$map
  lines$rhipe_cleanup_reduce <- cleanup$reduce

  
  paramaters <- parameters
  aparamaters <- list()
  ## ###########################
  ## HANDLE paramaters                                                  
  ## ############################
  if(rhoptions()$copyObjects$auto){
    sampbody <- function(){}
    empty.exp <- expression()
    require(codetools)
    exp <- list( setup$map, setup$reduce, cleanup$map, cleanup$reduce, map,reduce$pre,reduce$post,reduce$reduce)
    ## calling.frame <- sys.frame(-2) #since rhwatch calls this
    calling.frame <- envir
    seen.vars <- new.env()
    old.vars <- new.env()
    getV <- function(mu,cf){
      omit <- makeOmits() 
      ## see http://comments.gmane.org/gmane.comp.lang.r.general/284792
      elim <- function(p,cfd){
        ## if(identical(cfd, .GlobalEnv)) return(p)
        p[ unlist(sapply(p,function(axs) exists(axs, envir=.GlobalEnv)))]
      }
      .getV <- function(mu){
        x <- findGlobals(mu,merge=FALSE);
        varns <-  x$variables[ !sapply(x$variables,exists, where= omit,inherits=FALSE,USE.NAMES=FALSE) ]
        fns <- as.logical(unlist(sapply(varns, function(a) is.function(tryCatch(get(a,cf),error=function(e) NULL )))))
        xfns <- unique(c(x$functions,varns[fns]))
        xfns <- xfns[! sapply(xfns,exists, where=omit,inherits=FALSE,USE.NAMES=FALSE) ]
        varns <- varns[!fns]
        varns <- elim(varns, cf); xfns <- elim(xfns,cf)
        list(funs=xfns,varns=varns)
      }
    
      res <- .getV(mu)
      
      return(unique( unlist(c( res$varns, res$funs
                            ,sapply(res$varns, function(kap) {
                              moz <- tryCatch(get(kap,envir=cf),error=function(e) NULL)
                              if(is(moz,"list")){
                                sapply(moz, function(af){
                                  if(is(af,"function"))
                                      getV(af,environment(af))
                                })
                              }})
                            ,sapply(res$funs, function(kap) {
                              moz <- tryCatch(get(kap,envir=cf),error=function(e) NULL)
                              if(!is.null(moz)){
                                if(is.null(old.vars[[kap]])){
                                  old.vars[[kap]] <- NA
                                  getV(moz,environment(moz))
                                }
                              }
                              else
                                NULL
                            })))))
    }
    
    mux <- unique(unlist(lapply(exp, function(mu){
      if(identical(mu, empty.exp)) return(NULL)
      body(sampbody) <- mu
      getV(sampbody,cf=calling.frame)
    })))
    ## dx <- mux[!mux %in% rhoptions()$copyObjects$exclude ]
    for(x in mux){
      aparamaters[[x]] <- tryCatch(get(x, envir=calling.frame), error=function(e){
        ## if(!x %in% ls(seen.vars))
        ##   warning(sprintf("RHIPE: [param=auto], During symbol auto detect phase, object %s not found", x))
        assign(x,TRUE, seen.vars)
        NULL
      })
    }
    if(length(ls(seen.vars))>0)
      warning(sprintf("RHIPE(param='auto'): Following variables were discovered but not found: %s",paste(ls(seen.vars,all.names=TRUE),collapse=",")),immediate. =TRUE)
  }

  if(is.null(paramaters) && rhoptions()$copyObjects$auto){
    paramaters <- aparamaters
  }
  if(!is.null(paramaters)){

    if(is.list(paramaters)){
      ## get any parameters in 'aparamaters' that aren't already defined in 'paramaters'
      autoPar <- setdiff(names(aparamaters), names(paramaters))
      ## append these to 'paramaters'
      if(length(autoPar) > 0)
        paramaters <- c(paramaters, aparamaters[autoPar])
    }
    
    if(!is.null(paramaters) && is(paramaters,"character") && tolower( paramaters)=="all")
      paramaters<- getObjects()
    if(!is.null(paramaters) && is(paramaters,"character") && tolower( paramaters)=="auto"){
      stop("Defunct,set the auto field of rhoptions('copyObjects') to TRUE")
    }

    if(!is.list(paramaters))
      stop("parameters must be a named list or the string 'all' or 'auto'")
    lines$param.temp.file <- Rhipe:::makeParamTempFile(file="rhipe-temp-params",paramaters=paramaters,aframe=sys.frame(-1))
  }else{
    lines$param.temp.file <- NULL
  }
  
  
  lines$rhipe_map_output_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
  lines$rhipe_map_output_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'


  ## #################################################
  ## Handle Custom Comparators
  ## #################################################
  ordert <- c("bytes","integer","numeric","character")
  orderp <- switch(
                   pmatch(orderby,ordert),
                   "1"={"org.godhuli.rhipe.RHBytesWritable"},
                   "2"={"org.godhuli.rhipe.RHInteger"},
                   "3"={"org.godhuli.rhipe.RHNumeric"},
                   "4"={"org.godhuli.rhipe.RHText"},
                   )
  if(is.null(orderp)) stop(sprintf("Wrong ordering %s: try bytes,integer,numeric,character"))
  lines$rhipe_map_output_keyclass <- orderp
  if(lines$rhipe_map_output_keyclass != c("org.godhuli.rhipe.RHBytesWritable")
     && is.null(reduce)){
    stop("If using ordered keys, provide a reduce even e.g. rhoptions()$templates$identity")
  }

  lines$rhipe_string_quote <- "\r\n"
  lines$rhipe_string_quote <- ''
  lines$rhipe_send_keys_to_map <- 1L
  lines$rhipe_map_output_valueclass <- "org.godhuli.rhipe.RHBytesWritable"

  ## #######################################################
  ## Partitioners
  ## #######################################################
  lines$rhipe_partitioner_class <- "none"
  if(!is.null(partitioner) && is.list(partitioner)){
    if(is.null(partitioner$lims) || is.null(partitioner$type))
      stop("Must provide partitioner limits and type")
    if(length(partitioner$lims)==1) partitioner$lims = rep(partitioner$lims,2)
    lines$rhipe_partitioner_start <- partitioner$lims[1] 
    lines$rhipe_partitioner_end <- partitioner$lims[2]
    if(!all(partitioner$lims>0)) stop("limits must be greater than zero")
    ttable <- c("string","numeric",'integer')
    lines$rhipe_partitioner_type <- ttable[pmatch(partitioner$type,ttable)]
    if(is.na(lines$rhipe_partitioner_type))
      stop(sprintf("Invalid type:%s",partitioner$type))
    lines$rhipe_partitioner_class <- if(!is.null(partitioner$class)) partitioner$class
    else {
          switch(lines$rhipe_partitioner_type,
           "string"= { "org.godhuli.rhipe.RHPartitionerText" },
           "numeric"= { "org.godhuli.rhipe.RHPartitionerNumeric" },
           "integer"= { "org.godhuli.rhipe.RHPartitionerInteger" }
                 )
        }
  }

  lines$rhipe_reduce_buff_size <- 6000
  lines$rhipe_map_buff_size <- 3000
  lines$rhipe_map_bytes_read <- 150*1024*1024
  lines$rhipe_reduce_bytes_read <- 150*1024*1024
  lines$rhipe_job_verbose <- "TRUE"
  lines$rhipe_stream_buffer <- 10*1024
  lines$rhipe.use.hadoop.combiner="FALSE"
  lines$mapred.output.compress <- "true"
  lines$mapred.compress.map.output="true"

  

  ## ###########################################################
  ## Handle Copy Files
  ## ###########################################################
  lines$rhipe_copy_file <- paste(copyFiles)
  if(!is.null(mapred$mapred.job.tracker) && mapred$mapred.job.tracker=='local')
    lines$rhipe_copy_file <- 'FALSE'

  ## ###########################################################
  ## MISC
  ## ###########################################################
  lines$RHIPE_DEBUG <- 0
  lines$rhipe_input_folder <- ""
  lines$rhipe_output_folder <- ""
  lines$rhipe_map_input_type <- "default"
  lines$mapred.job.reuse.jvm.num.tasks <- -1
  lines$mapreduce.job.counters.groups.max <- "200"


  ################################################################################################
  # HANDLE MAPRED EXTRA from RHOPTIONS
  ################################################################################################
  filterOut <- function(alln,rem=c("mapred.reduce.tasks"))
    alln[sapply(alln,function(x) if( x %in% rem && x %in% names(lines)) FALSE else TRUE)]
  options.mapred = rhoptions()$mropts
  if(!is.null(options.mapred))
    for(n in filterOut(names(options.mapred))) lines[[n]] = options.mapred[[n]]

  if(is.null(reduce)){
    lines$rhipe_reduce_justcollect <- "TRUE"
  }

  ## ##########################################################
  ## Handle Input Output Formats
  ## ##########################################################

  if(is(input,"numeric") || is(input, "integer")){
    input <-   rhoptions()$ioformats[["N"]](input)
  }
  else if(is(input,"character")){
    input <- rhoptions()$ioformats[["seq"]](input)
  }
  else if(is(input,"rhwatch") ||is(input, "rhmr")){
    ifo <- rhofolder(input)
    if(Rhipe:::dir.contain.mapfolders(ifo))
      input <- rhoptions()$ioformats[["map"]](ifo)
    else
      input <- rhoptions()$ioformats[["seq"]](ifo)
  }

  lines <- input(lines,"input",match.call())

  if(is.null(output)){
    if(!is.null(rhoptions()$HADOOP.TMP.FOLDER)){
      output <- Rhipe:::mkdHDFSTempFolder(file="rhipe-temp")
      lines$rhipe.read.and.delete.ofolder <- TRUE
    }else{
      stop("No output specified,and RHIPE could not find a value for HADOOP.TMP.FOLDER
            in rhoptions(). Set this: rhoptions(HADOOP.TMP.FOLDER=path)")
    }
  }else{
    lines$rhipe.read.and.delete.ofolder <- FALSE
  }
  if(is.character(output)){
    output <- rhoptions()$ioformats[["seq"]](output)
  }
  lines <- output(lines,"output",match.call())

  ## ##########################################################
  ## Handle Shared Files
  ## ##########################################################
  if(length(shared) > 0)
    shared = rhabsolute.hdfs.path(shared)
  if(!is.null(lines$param.temp.file)){
    vnames <- ls(lines$param.temp.file$envir); vwhere <- lines$param.temp.file$envir
    paramaters <- list(envir=vwhere,file=lines$param.temp.file$file)
    shared <- c(shared, if(is.null(lines$param.temp.file)) NULL else lines$param.temp.file$file)
    ##Note also the setup has to be re-written ...
    lines$rhipe_setup_map<- c(lines$param.temp.file$setup,lines$rhipe_setup_map)
    lines$rhipe_setup_reduce <- c(lines$param.temp.file$setup,lines$rhipe_setup_reduce)
  }
  shared.files <- unlist(as.character(shared))
  if(! all(sapply(shared.files,is.character)))
    stop("shared  must be all characters")
  shared.files <- unlist(sapply(shared.files,function(r){
    r1 <- strsplit(r,"/")[[1]]
    return(paste(r,r1[length(r1)],sep="#",collapse=''))
  },simplify=T))
  shared.files <- paste(shared.files,collapse=",")
  lines$rhipe_shared <- shared.files
  lines$R_ENABLE_JIT <- 3

  lines$rhipe_setup_map  <- rawToChar(serialize(lines$rhipe_setup_map,NULL,ascii=TRUE))
  lines$rhipe_setup_reduce  <- rawToChar(serialize(lines$rhipe_setup_reduce,NULL,ascii=TRUE))
  lines$rhipe_cleanup_map<- rawToChar(serialize(lines$rhipe_cleanup_map,NULL,ascii=TRUE))
  lines$rhipe_cleanup_reduce <- rawToChar(serialize(lines$rhipe_cleanup_reduce,NULL,ascii=TRUE))

  ## #############################################################################################
  ##  HANDLE MAPRED EXTRA PARAMS
  ## #############################################################################################

  for(n in names(mapred)) lines[[n]] <- mapred[[n]]
  
  
  lines$rhipe_combiner <- paste(as.integer(combiner))
  if(lines$rhipe_combiner=="1")
    lines$rhipe_reduce_justcollect <- "FALSE"

  ## #############################################################################################
  ##  HANDLE JARFILES
  ## #############################################################################################
  if(!is.null(lines$jarfiles)){
    jarfiles <- c(jarfiles, lines$jarfiles)
    lines$jarfiles <- NULL
  }
  if(length(jarfiles)>0) {
    lines$rhipe_jarfiles <- paste(path.expand(jarfiles),collapse=",")
    p <- Rhipe:::mkdtemp(tempdir())
    invisible(sapply(jarfiles, function(r) rhget(r, p)))
    lines$rhipe_cp_tempdir <- p
    lines$rhipe_classpaths <- paste(list.files(p,full.names=TRUE),collapse=",")
  }else {
    lines$rhipe_jarfiles=""
    lines$rhipe_classpaths <- ""
  }
  
  ## ##############################################################################################
  ## HANDLE ZIPS
  ## ##############################################################################################

  if(!is.null(lines$zipfiles)){
    zips <- c(zips, lines$zipfiles)
    lines$zipfiles <- NULL
  }
  zips <- c(zips,rhoptions()$zips)
  zips = rhabsolute.hdfs.path(zips)
  if(length(zips)>0) lines$rhipe_zips <- paste(unlist(local({
    zips <- path.expand(zips)
    sapply(zips,function(r) {
      rsyml <- tail(strsplit(r,"/")[[1]],1)
      p <- grep("((\\.tar\\.gz)|(\\.tgz)|(\\.zip))$",rsyml)
      if(length(p)>0){
        paste(r,sub("\\.((tar\\.gz)|(tgz)|(zip))$","",rsyml),sep="#")
      }else NULL
    })})),collapse=",")
  else  lines$rhipe_zips=""

  ## ####################################
  ## Fixup Output Path
  ## ####################################
  if(is.null(lines$rhipe.fixup.output) || (lines$rhipe.fixup.output==TRUE)){
    lines$rhipe_output_folder <-  rhabsolute.hdfs.path(lines$rhipe_output_folder)
  }
    
  lines$param.temp.file <- NULL
  lines <- lapply(lines,as.character);
  conf <- tempfile(pattern='rhipe')
  
  
  h <- list(lines=lines,temp=conf,paramaters=paramaters)
  if(!is.null(mapred$class))
    class(h)=mapred$class
  else
    class(h)="rhmr"
  h
}



rhoptions <- function(li=NULL,...){
  if(missing(li) && length(list(...))==0){
    get("rhipeOptions",envir=.rhipeEnv)
  }else{
    N <- if(is.null(li)) list(...) else li
    v <- rhoptions()
    for(x in names(N))
      v[[x]] <- N[[x]]
    assign("rhipeOptions",v,envir=.rhipeEnv)
  }
}

rhsetoptions <- function(li=NULL,...){
  warning(sprintf("Use rhoptions instead\n"))
  rhoptions(li,...)
}

optmerge <- function(la,lb){
  ##merges lists la and lb
  ##lb overrides la

  x <- la
  for(n in names(lb)){
    x[[n]] <- lb[[n]]
  }
  x
}
       

## Contributed by Jeremiah Rounds
linesToTable <- function(lines,skip.regex = NULL,...){
  if(length(lines) == 0)
    return(NULL)
  lines = as.character(unlist(lines))
  if(!is.null(skip.regex)){
    keep = regexpr(skip.regex, lines) == -1
    lines = lines[keep]
  }
  if(length(lines) == 0)
    return(NULL)
  
  tc = textConnection(lines,"r")
  rtable = read.table(tc, ...)
  close(tc)
  return(rtable)
}

mkdHDFSTempFolder <- function(dirprefix=rhabsolute.hdfs.path(rhoptions()$HADOOP.TMP.FOLDER),pathsep=NULL,file,unqsalt=NULL){
  if(is.null(pathsep)){
    pathsep <- if(grepl("/$",dirprefix)) "" else "/"
  }
  if(is.null(unqsalt)){
    fnames <- c(rhls(dirprefix)$file, as.POSIXlt(Sys.time())$sec)
    unqsalt <- serialize(fnames,NULL)
  }
  sprintf("%s%s%s-%s",dirprefix, pathsep, file, .Call("md5", unqsalt,length(unqsalt),PACKAGE="Rhipe"))
}

makeParamTempFile <- function(file,paramaters,aframe){
    oldparam <- paramaters
    # need to use lapply (setting parameters[[i]] <- NULL removes the element)
    paramaters <- lapply(seq_along(oldparam), function(i) {
       if(is.name(oldparam[[i]])) get(as.character(oldparam[[i]]), envir=aframe) else oldparam[[i]]
    })
    ## where firstchocie == "", use second choice ssd 
    firstchoice <- names(oldparam)
    if(length(firstchoice)==0) firstchoice <- character(length(paramaters))
    for(i in seq_along(firstchoice)){
      if(is.null(firstchoice[i]) || firstchoice[i]=="") {
        if(!is.name(oldparam[[i]]))
          stop(sprintf("paramaters argument is improper at position %s",i))
        else
          firstchoice[i] <- as.character(oldparam[i])
      }
    }
    names(paramaters) <- firstchoice
  
    tfile <- Rhipe:::mkdHDFSTempFolder(file="rhipe-temp-params")
    list(file=tfile
         ,envir=as.environment(paramaters)
         ,setup= as.expression(bquote({
           load(.(paramfile))
         },list(paramfile = basename(tfile)))))
  }

getObjects <- function(maxsize=if(is.null(rhoptions()$copyObjects)) 100*1024*1024 else rhoptions()$copyObjects$maxsize
                       ,excludeObjects=if(is.null(rhoptions()$copyObjects)) character(0) else rhoptions()$copyObjects$exclude)
  {
    frames <- if(sys.nframe()==1) 0 else c(0,rev(-(2:sys.nframe())))
    varcol <- list()
    for(i in frames ){
      a <- sys.frame(i)
      for(varname in ls(a)){
        if(varname %in% excludeObjects) next
        x <- get(varname, envir=a)
        if(object.size(x)<=maxsize)
          varcol[[varname]] <- x
        else
          warning(sprintf("Dropped object: ",varname))
      }
    }
    varcol
  }

makeOmits <- function(){
  userProvided <- rhoptions()$copyObjects$exclude
  m <- new.env()
  ### installed.packages()[,"Package"]
  sapply(as.character(.packages()), function(r){
    sapply(tryCatch(ls(sprintf("package:%s",r),all.names=TRUE),error=function(e) NULL),function(s) assign(s,NA, m))
  })
  sapply(as.character(userProvided),function(s)  assign(s,TRUE, m))
  m
}
