rhoptions <- function(){
  get("rhipeOptions",envir=.rhipeEnv)
}

rhsetoptions <- function(li=NA,...){
  N <- list(...)
  v <- rhoptions()
  for(x in names(N))
    v[[x]] <- N[[x]]
  assign("rhipeOptions",v,envir=.rhipeEnv)
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
       
rhmr <- function(map,reduce=NULL,
                 combiner=F,
                 setup=NULL,
                 cleanup=NULL,
                 ofolder='',
                 ifolder='',
                 inout=c("text","text"),
                 orderby='bytes',
                 mapred=NULL,
                 shared=c(),
                 jarfiles=c(),
                 partitioner=NULL,
                 copyFiles=F,
                 N=NA,
                 opts=rhoptions(),
                 jobname=""){
  lines <- list();
  if(!is.expression(map))
    stop("'map' must be an expression")
  reduces <- T
  lines$rhipe_reduce_justcollect <- "FALSE"
  
  if(is.null(reduce)){
    reduces <- FALSE
  }
  
  ## rr <- eval(reduce$reduce); rpre <- eval(reduce$pre) ; rpos <- eval(reduce$post)
  ## lines$rhipe_reduce <- rawToChar(serialize( eval(substitute(rr)) ,NULL,ascii=T))
  ## lines$rhipe_reduce_prekey <- rawToChar(serialize( eval(substitute(rpre)) ,NULL,ascii=T))
  ## lines$rhipe_reduce_postkey <- rawToChar(serialize( eval(substitute(rpos)) ,NULL,ascii=T))
  lines$rhipe_reduce <- rawToChar(serialize(reduce$reduce,NULL,ascii=T))
  lines$rhipe_reduce_prekey <- rawToChar(serialize(reduce$pre ,NULL,ascii=T))
  lines$rhipe_reduce_postkey <- rawToChar(serialize(reduce$post,NULL,ascii=T))

  lines$rhipe_jobname=jobname
  if(combiner & is.null(reduce))
    stop("If combiner is T, give a reducer")
  if(is.null(setup)){
    setup$map <- expression()
    setup$reduce <- expression()
  }

  if(!is.expression(setup) && !is.list(setup))
    stop("'setup' is either a list of expressions (map=,redce=) or expression")

  if(is.list(setup)){
    if(! all(unlist(lapply(setup,is.expression))))
      stop("elements of 'setup' must be expressions")
    if(is.null(setup$reduce)) setup$reduce <- expression()
    if(is.null(setup$map)) setup$map <- expression()
  }
  if(is.null(setup))
    setup <- expression()
  if(is.expression(setup)){
    setup <- list(map=setup,reduce=setup)
  }
  
  if(is.null(cleanup)){
    cleanup$map <- expression()
    cleanup$reduce <- expression()
  }

  if(!is.expression(cleanup) && !is.list(cleanup))
    stop("'cleanup' is either a list of expressions (map=,redce=) or expression")

  if(is.list(cleanup)){
    if(! all(unlist(lapply(cleanup,is.expression))))
      stop("elements of 'cleanup' must be expressions")
    if(is.null(cleanup$reduce)) cleanup$reduce <- expression()
    if(is.null(cleanup$map)) cleanup$map <- expression()
  }
  if(is.null(cleanup))
    cleanup <- expression()
  if(is.expression(cleanup)){
    cleanup <- list(map=cleanup,reduce=cleanup)
  }


  ## map=eval(map);map <- eval(substitute(map))
  ## setup.m=eval(setup.m);setup.m <- eval(substitute(setup.m))
  ## setup.r=eval(setup.r);setup.r <- eval(substitute(setup.r))
  ## cleanup.m=eval(cleanup.m);cleanup.m <- eval(substitute(cleanup.m))
  ## cleanup.r=eval(cleanu.r);cleanup.r <- eval(substitute(cleanup.r))
  
  map.s <- serialize(map,NULL,ascii=T)
  
  setup.m <- serialize(setup$map,NULL,ascii=T)
  setup.r <- serialize(setup$reduce,NULL,ascii=T)
  cleanup.m <- serialize(cleanup$map,NULL,ascii=T)
  cleanup.r <- serialize(cleanup$reduce,NULL,ascii=T)

  if(ofolder!=''){
    ofolder <- ofolder[1]
    if(substr(ofolder,nchar(ofolder),nchar(ofolder))!="/")
      ofolder <- paste(ofolder,"/",sep="")
  }
  flagclz <- NULL
  if(length(inout)==1) inout=c(inout,"null") 

  iftable <- c("sequence","text","lapply","map","null")
  ## browser()
  inout.a <- sapply(inout,pmatch,iftable)
  inout <- iftable[inout.a]

  ifolder=switch(inout[1],
    "map"={
      flagclz="sequence"
      uu=unclass(rhls(ifolder,rec=TRUE)['file'])$file
      uu[grep("data$",uu)]
    },
    "sequence"={
      a <- rhls(ifolder,rec=TRUE)$file
    },
    "text"={
      rhls(ifolder)$file
    }
    )
  remr <- c(grep("/_logs",ifolder))
  if(length(remr)>0)
    ifolder <- ifolder[-remr]
  if(!is.null(flagclz)) inout <- c('sequence',inout[2])

  ## print(ifolder)
  ## stop("woo")
  lines<- append(lines,list(
                     R_HOME=R.home()
                     ,rhipe_map=rawToChar(map.s)
                     ,rhipe_setup_map=rawToChar(setup.m)
                     ,rhipe_cleanup_map= rawToChar(cleanup.m)
                     ,rhipe_cleanup_reduce= rawToChar(cleanup.r)
                     ,rhipe_setup_reduce= rawToChar(setup.r)
                     ,rhipe_command=paste(opts$runner,collapse=" ")
                     ,rhipe_input_folder=paste(ifolder,collapse=",")
                           
                     ,rhipe_output_folder=paste(ofolder)))

  shared.files <- unlist(as.character(shared))
  if(! all(sapply(shared.files,is.character)))
    stop("shared  must be all characters")
  shared.files <- unlist(sapply(shared.files,function(r){
    r1 <- strsplit(r,"/",extended=F)[[1]]
    return(paste(r,r1[length(r1)],sep="#",collapse=''))
  },simplify=T))
  shared.files <- paste(shared.files,collapse=",")
  lines$rhipe_shared <- shared.files
  
  inout <- as.vector(matrix(inout,ncol=2))
  lines$rhipe_map_output_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
  lines$rhipe_map_output_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
  switch(inout[1],
         'text' = {
           lines$rhipe_inputformat_class <- 'org.godhuli.rhipe.RXTextInputFormat'
           ## 'org.godhuli.rhipe.RXTextInputFormat'
           lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHNumeric'
           lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHText'
         },
         'sequence'={
           lines$rhipe_inputformat_class <-
             'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
           lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         },
         'lapply'={
           lines$rhipe_inputformat_class <-
             'org.godhuli.rhipe.LApplyInputFormat'
           lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHNumeric'
           lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHNumeric'
           lines$rhipe_lapply_lengthofinput <- as.integer(N)
         },
         'binary'={
           stop("'binary' cannot be used as input format")
         })

  switch(inout[2],
         'text' = {
           lines$rhipe_outputformat_class <-
             'org.godhuli.rhipe.RXTextOutputFormat'
              ## 'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
##'org.apache.hadoop.mapred.TextOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         },
         'sequence' = {
           lines$rhipe_outputformat_class <-
##'org.apache.hadoop.mapred.SequenceFileOutputFormat'
              'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         },
         'binary' = {
           lines$rhipe_outputformat_class <-'org.godhuli.rhipe.RXBinaryOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         },
         'null'= {
           lines$rhipe_outputformat_class <-'org.apache.hadoop.mapreduce.lib.output.NullOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.apache.hadoop.io.NullWritable'
           lines$rhipe_outputformat_valueclass <- 'org.apache.hadoop.io.NullWritable'
         },
         'map' = {
           lines$rhipe_outputformat_class <-'org.godhuli.rhipe.RHMapFileOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         })
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
  
  lines$rhipe_string_quote <- ''
  lines$rhipe_map_output_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
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
  lines$mapred.textoutputformat.usekey <-  "TRUE"
  lines$rhipe_reduce_buff_size <- 3000
  lines$rhipe_map_buff_size <- 10000
  lines$rhipe_job_verbose <- "TRUE"
  lines$rhipe_stream_buffer <- 10*1024
  lines$mapred.compress.map.output="true"
  lines$rhipe.use.hadoop.combiner="FALSE"
  ##If user does not provide
  ##a reduce function,set reduce to NULL
  ##however can be over ridden by
  ##mared.reduce.tasks
  
  lines$rhipe_copy_file <- paste(copyFiles)
  if(!is.null(mapred$mapred.job.tracker) &&
     mapred$mapred.job.tracker=='local')
    lines$rhipe_copy_file <- 'FALSE'

  if(is.null(reduce)){
    lines$rhipe_reduce_justcollect <- TRUE
    if(!is.null(lines$mapred.reduce.tasks))
      lines$mapred.reduce.tasks <- 0
  }
  lines$mapred.job.reuse.jvm.num.tasks <- -1
  for(n in names(mapred)) lines[[n]] <- mapred[[n]]
  
  lines$rhipe_combiner <- paste(as.integer(combiner))
  if(lines$rhipe_combiner=="1")
    lines$rhipe_reduce_justcollect <- "FALSE"

  if(lines$rhipe_map_output_keyclass != c("org.godhuli.rhipe.RHBytesWritable")
     && is.null(reduce)){
    stop("If using ordered keys, provide a reduce e.g.

reduce = expression(
  reduce={ lapply(reduce.values,function(r) rhcollect(reduce.key,r)) }
)
")
  }
  ## parttype = c("string","integer","numeric","complex","logical","raw")

  lines <- lapply(lines,as.character);
  conf <- tempfile(pattern='rhipe')

  h <- list(lines,temp=conf)
  if(!is.null(mapred$class))
    class(h)=mapred$class
  else
    class(h)="rhmr"
  h
}


rhlapply <- function(ll=NULL,fun,ifolder="",ofolder="",setup=NULL,
                    inout=c("lapply","sequence"),readIn=T,mapred=list(),jobname="rhlapply",
                     doLocal=F,N,aggr=NULL,...){
  del.o.file <- F
  del.i.file <- F
  
  ok <- F
  if(is.list(ll)) ok <- T
  if(is.numeric(ll) && length(ll)==1) ok <- T
  if(is.null(ll) && ifolder!="") ok <- T
  if(!is.null(ll) && ifolder!="")
    stop("cannot provide ll and ifolder at the same time, either or")
  
  if(!ok) stop("Must provide either number of iterations OR a list OR an input folder")
  if(is.numeric(ll) && ll<=0)
    stop("if ll is numeric, must be >0")

  ##We create an expression that unserializes
  ##the user fun and installs it under the name userFUN...
  ##the map expression is
  ##      ...r... <- userFUN...(map.value)
  ##      rhcollect(map.key,...result...)
  ##If the user provides a setup
  ##Insert the deserialization as the first instruction
  ## usercode <- rawToChar(serialize(fun,NULL,ascii=TRUE))
  usecodeText <-
    parse(text=paste("userFUN...=",paste(deparse(fun),collapse="\n")))
  aggrText <-
    parse(text=paste("aggr...=",paste(deparse(aggr),collapse="\n")))

  userFUN... <- fun
  if(!is.null(setup))
    setup <- append(usecodeText,setup)
  else
    setup <- usecodeText

  setup <- append(aggrText,setup)

  map.exp <- expression({
    w <- lapply(seq_along(map.values),function(.id.){
      r <- userFUN...(map.values[[.id.]])
      if(is.null(aggr...))
        rhcollect(map.keys[[.id.]],r)
      else
        r
    })
    if(!is.null(aggr...)){
      rhcollect(1,aggr...(w))
    }
  })

  if(ofolder==""){
    ##One hopes this is unique
    tempo.file <- paste("/tmp/",tail(strsplit(tempfile(patter=paste("rhipelapply.output.",
                                                         paste(sample(letters,4),sep="",collapse=""),sep="",collapse="")),
                                              "/+")[[1]],1),"/",sep="")
##     .jcheck();.jcall(rhoptions()$fsshell,"V","makeFolderToDelete",tempo.file,check=T);
##     doDeletableFile(tempo.file,rhoptions()$socket)
    del.o.file <- T}
  else{
    tempo.file <- ofolder
  }
  ## sok <-  socketConnection('127.0.0.1',rhoptions()$port,open='wb',blocking=T)
  
  if(is.list(ll)){
    tempi.file <-  paste("/tmp/",tail(strsplit(paste(tempfile(patter="rhipelapply.input."),
                                                     paste(sample(letters,4),sep="",collapse=""),
                                                     sep="",collapse="")
                                               ,"/+")[[1]],1),"/",sep="")
##1
    message("Creating temporary input folder for list")
    if(missing(N)){
      aa <- Rhipe:::optmerge( rhoptions()$mropts,mapred)
      if(!is.null(aa$mapred.map.tasks) ){
        a <- as.numeric(aa$mapred.map.tasks)
        rhwrite(ll,f=tempi.file,N)
      }else rhwrite(ll,f=tempi.file)#
    }
    else
      rhwrite(ll,f=tempi.file,N=N)#

    ifolder=tempi.file
    del.i.file <- T
  }
  if(is.numeric(ll)){
      mapred$rhipe_lapply_lengthofinput <- as.integer(ll)
    }
  mapred$rhipe_reduce_justcollect <- "TRUE"
    
  if(ifolder!="") inout[1] <- "sequence"
  if(inout[2]=='lapply')
    stop('inout[2] cannot be lapply')

  if(is.null(mapred$mapred.reduce.tasks))
    mapred$mapred.reduce.tasks <- 0
  mapred$class="rhlapply"
  
  z <- rhmr(map=map.exp,reduce=NULL,ifolder=ifolder,combiner=F,
            setup=list(map=setup,reduce=expression()),ofolder=tempo.file,copyFiles=T,inout=inout,mapred=mapred,...)

  h=list(z,function(){
    retdata <- NULL
      on.exit({
        #2
            if(del.o.file) rhdel(tempo.file)
            if(del.i.file) rhdel(tempi.file)
            return(retdata)
          })
      if(readIn){
        message("---------------")
        message("Reading in Data")
        message("---------------")
        #3
        retdata <- rhread(paste(tempo.file,"/p*",sep=""),type='sequence')
        if(!is.null(aggr)) retdata <- aggr(lapply(retdata,"[[",2))
      }
    })
  class(h)="rhlapply"
  h
}


rhex <- function (conf,async=FALSE,mapred,...) 
{
  exitf <- NULL
  ## browser()
  if(class(conf)=="rhlapply") {
    zonf <- conf[[1]]$temp
    exitf <- conf[[2]]
    lines <- conf[[1]][[1]]
  }else if(class(conf)=='rhmr'){
    zonf <- conf$temp
    lines <- conf[[1]]
  }else
  stop("Wrong class of list given")

  if(!missing(mapred)){
    for(i in names(mapred)){
      lines[[i]] <- mapred[[i]]
    }
  }
  lines$rhipe_job_async <- as.character(as.logical(async))

  conffile <- file(zonf,open='wb')
  writeBin(as.integer(length(lines)),conffile,size=4,endian='big')
  for(x in names(lines)){
    writeBin(as.integer(nchar(x)),conffile,size=4,endian='big')
    writeBin(charToRaw(x), conffile,endian='big')
    writeBin(as.integer(nchar(lines[[x]])),conffile,size=4,endian='big')
    writeBin(charToRaw(as.character(lines[[x]])),conffile,endian='big')
  }
  close(conffile)
  
  cmd <- paste("$HADOOP/bin/hadoop jar ", rhoptions()$jarloc, 
               " org.godhuli.rhipe.RHMR ", zonf, sep = "")
  x. <- paste("Running: ", cmd)
  y. <- paste(rep("-",min(nchar(x.),40)))
  message(y.);message(x.);message(y.)
  result <- system(cmd,...)
  f3=NULL
  if(result==256){
    f1=file(zonf,"rb")
    f2=readBin(f1,"integer",1,endian='network')
    f3=rhuz(readBin(f1,'raw',f2))
    close(f1)
  }
  unlink(zonf)
  if(result==256 && !is.null(exitf) && !async){
    return(exitf())
  }
  if(async==TRUE){
    y <- if(!is.null(exitf)) list(f3,exitf) else list(f3)
    names(y[[1]]) <- c("job.url","job.name","job.id","job.start.time")
    class(y) <- "jobtoken"
    return(y)
  }
  if(result == 256){
    if(!is.null(f3) && !is.null(f3$R_ERRORS)){
      rr <- FALSE
      stop(sprintf("ERROR\n%s",paste(names(f3$R_ERRORS),collapse="\n")))
    }else rr <- TRUE
  }else rr <- FALSE
  return(list(rr, counters=f3))
}

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

rhstatus <- function(x){
  if(class(x)!="jobtoken" && class(x)!="character" ) stop("Must give a jobtoken object(as obtained from rhex)")
  if(class(x)=="character") id <- x else {
    x <- x[[1]]
    id <- x[['job.id']]
  }
  result <- Rhipe:::doCMD(rhoptions()$cmd['status'],jobid =id,
                          needoutput=TRUE,ignore.stderr=TRUE,verbose=FALSE)
  d <- data.frame("pct"=result[[3]],"numtasks"=c(result[[4]][1],result[[5]][[1]]),
                  "pending"=c(result[[4]][2],result[[5]][[2]]),
                  "running" = c(result[[4]][3],result[[5]][[3]]),
                  "complete" = c(result[[4]][4],result[[5]][[4]])
                  ,"failed" = c(result[[4]][5],result[[5]][[5]]))

  rownames(d) <- c("map","reduce")
  duration = result[[2]]
  state = result[[1]]
  return(list(state=state,duration=duration,progress=d, counters=result[[6]]));
}

  
rhjoin <- function(x,verbose=TRUE,ignore.stderr=TRUE){
  if(class(x)!="jobtoken" && class(x)!="character") stop("Must give a jobtoken object(as obtained from rhex) or the Job id")
  if(class(x) == "jobtoken") job.id <-  x[[1]]['job.id'] else job.id = x
  result <- Rhipe:::doCMD(rhoptions()$cmd['join'], jobid =job.id,needoutput=TRUE,
                          joinwordy = as.character(as.logical(verbose))
                          ,ignore.stderr=ignore.stderr)
                         
  if(class(x) == "jobtoken" && length(x)==2){
    ## from rhlapply
    return(x[[2]]())
  }
  return(    list(result=result[[1]], counters=result[[2]]))
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
