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

       
rhmr <- function(map,reduce=NULL,
                 combiner=F,
                 setup=NULL,
                 cleanup=NULL,
                 ofolder='',
                 ifolder='',
                 inout=c("text","text"),
                 mapred=NULL,
                 shared=c(),
                 jarfiles=c(),
                 copyFiles=F,
                 opts=rhoptions(),
                 jobname=""){
  lines <- list();
  if(!is.expression(map))
    stop("'map' must be an expression")
  reduces <- T
  lines$rhipe_reduce_justcollect <- "FALSE"
  ## lines$rhipe_reduce <- rawToChar(serialize(expression(),NULL,ascii=T))
  ## lines$rhipe_reduce_prekey <- rawToChar(serialize(expression(),NULL,ascii=T))
  ## lines$rhipe_reduce_postkey <- rawToChar(serialize(expression(),NULL,ascii=T))
  
  if(is.null(reduce)){
    reduces <- FALSE
  }

  reduce$reduce <- eval(substitute(reduce$reduce))
  reduce$pre <- eval(substitute(reduce$pre))
  reduce$post <- eval(substitute(reduce$post))
  
  lines$rhipe_reduce <- rawToChar(serialize(reduce$reduce,NULL,ascii=T))
  lines$rhipe_reduce_prekey <- rawToChar(serialize(reduce$pre,NULL,ascii=T))
  lines$rhipe_reduce_postkey <- rawToChar(serialize(reduce$post,NULL,ascii=T))

  lines$rhipe_jobname=jobname
  if(combiner && is.null(reduce))
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


  map.s <- eval(substitute(map.s))
  setup.m <- eval(substitute(setup.m))
  setup.r <- eval(substitute(setup.r))
  cleanup.m <- eval(substitute(cleanup.m))
  cleanup.r <- eval(substitute(cleanup.r))
  
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
  lines$map_output_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
  lines$map_output_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
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
         },
         'binary'={
           stop("'binary' cannot be used as input format")
         })
         
  switch(inout[2],
         'text' = {
           lines$rhipe_outputformat_class <-
             ## 'org.godhuli.rhipe.RXTextOutputFormat'
              'org.apache.hadoop.mapreduce.lib.output.TextOutputFormat'
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
         })
  
  lines$rhipe_reduce_buff_size <- 10000
  lines$rhipe_map_buff_size <- 10000
  lines$rhipe_job_verbose <- "TRUE"
  lines$rhipe_stream_buffer <- 10*1024
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
  
  lines$rhipe_combiner <- paste(combiner)
  if(lines$rhipe_combiner=="TRUE")
    lines$rhipe_reduce_justcollect <- "FALSE"
  
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
                     doLocal=F,...){
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
  
  if(!is.null(setup))
    setup <- append(usecodeText,setup)
  else
    setup <- usecodeText

  map.exp <- expression({ for(.id. in 1:length(map.values)) { ..r..<-userFUN...(map.values[[.id.]]); rhcollect(map.keys[[.id.]],..r..) }})

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
    rhwrite(ll,f=tempi.file)#
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
        retdata <- rhread(paste(tempo.file,"/p*",sep=""),doLocal)
      }
    })
  class(h)="rhlapply"
  h
}


rhex <- function (conf) 
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
  result <- system(cmd)
  unlink(zonf)
  if(result==256 && !is.null(exitf)){
    return(exitf())
  }
  return(if(result==256) 1 else 0)
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
