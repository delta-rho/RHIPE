rhoptions <- function(){
  get("rhipeOptions",envir=.rhipeEnv)
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
                 opts=rhoptions()){
  lines <- list();
  if(!is.expression(map))
    stop("'map' must be an expression")
  reduces <- T
  lines$rhipe_reduce_justcollect <- "FALSE"
  lines$rhipe_reduce <- rawToChar(serialize(expression(),NULL,ascii=T))
  lines$rhipe_reduce_prekey <- rawToChar(serialize(expression(),NULL,ascii=T))
  lines$rhipe_reduce_postkey <- rawToChar(serialize(expression(),NULL,ascii=T))
  
  if(is.null(reduce)){
    reduces <- FALSE
  }
  if(is.expression(reduce)){
    lines$rhipe_reduce <- rawToChar(serialize(reduce,NULL,ascii=T))
  }
  if(is.list(reduce)){
    if(!is.null(reduce$pre) && is.expression(reduce$pre)){
      lines$rhipe_reduce_prekey <- rawToChar(serialize(reduce$pre,NULL,ascii=T))
    }else{
      lines$rhipe_reduce_prekey <- rawToChar(serialize(expression(),NULL,ascii=T))
    }
    if(!is.null(reduce$post) && is.expression(reduce$post)){
      lines$rhipe_reduce_postkey <- rawToChar(serialize(reduce$post,NULL,ascii=T))
    }else{
      lines$rhipe_reduce_postkey <- rawToChar(serialize(expression(),NULL,ascii=T))
    }
    if(!is.null(reduce$reduce) && is.expression(reduce$reduce)){
      lines$rhipe_reduce <- rawToChar(serialize(reduce$reduce,NULL,ascii=T))
    }else{
      lines$rhipe_reduce <- rawToChar(serialize(expression(),NULL,ascii=T))
    }
  }

  
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
                     R_HOME=Sys.getenv("R_HOME")
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
  switch(inout[1],
         'text' = {
           lines$rhipe_inputformat_class <- 'org.godhuli.rhipe.RXTextInputFormat'
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
         })

         
  switch(inout[2],
         'text' = {
           lines$rhipe_outputformat_class <- 'org.godhuli.rhipe.RXTextOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         },
         'sequence' = {
           lines$rhipe_outputformat_class <-
             'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
         })

  lines$rhipe_job_verbose <- "TRUE"
  lines$rhipe_stream_buffer <- 10*1024
  ##If user does not provide
  ##a reduce function,set reduce to NULL
  ##however can be over ridden by
  ##mared.reduce.tasks
  if(!reduces) lines$mapred.reduce.tasks <- '0'
  
  lines$rhipe_copy_file <- paste(copyFiles)
  if(!is.null(mapred$mapred.job.tracker) &&
     mapred$mapred.job.tracker=='local')
    lines$rhipe_copy_file <- 'FALSE'
  for(n in names(mapred)) lines[[n]] <- mapred[[n]]
  
  lines$rhipe_combiner <- paste(combiner)
  if(lines$rhipe_combiner=="TRUE")
    lines$rhipe_reduce_justcollect <- "FALSE"
  
  lines <- lapply(lines,as.character);
  conf <- tempfile(pattern='rhipe')
  conffile <- file(conf,open='wb')
  writeBin(as.integer(length(lines)),conffile,size=4,endian='big')
  for(x in names(lines)){
    writeBin(as.integer(nchar(x)),conffile,size=4,endian='big')
    writeBin(charToRaw(x), conffile,endian='big')
    writeBin(as.integer(nchar(lines[[x]])),conffile,size=4,endian='big')
    writeBin(charToRaw(as.character(lines[[x]])),conffile,endian='big')
  }
    
  close(conffile)
  h <- list(lines,temp=conf)
  class(h)="rhmr"
  h
}


rhlapply <- function(ll=NULL,fun,ifolder="",ofolder="",setup=NULL,
                    inout=c("lapply","sequence"),readIn=T,mapred=list(),...){
  del.o.file <- F
  del.i.file <- F
  
  ok <- F
  if(is.list(ll)) ok <- T
  if(is.numeric(ll) && length(ll)==1) ok <- T
  if(is.null(ll) && ifolder!="") ok <- T
  if(!is.null(ll) && ifolder!="")
    stop("cannot provide ll and ifolder at the same time, either or")
  
  if(!ok) stop("Must provide either number of iterations OR a list OR an input folder")
  

  ##We create an expression that unserializes
  ##the user fun and installs it under the name userFUN...
  ##the map expression is
  ##      ...r... <- userFUN...(map.value)
  ##      rhcollect(map.key,...result...)
  ##If the user provides a setup
  ##Insert the deserialization as the first instruction
  usercode <- rawToChar(serialize(fun,NULL,ascii=TRUE))
  usecodeText <-
    parse(text=paste("userFUN... <-","unserialize(charToRaw('",usercode,"'))",sep=""))
  
  if(!is.null(setup))
    setup <- append(usecodeText,setup)
  else
    setup <- usecodeText
  map.exp <- expression({ ..r..<-userFUN...(map.value); rhcollect(map.key,..r..)})

  if(ofolder==""){
    ##One hopes this is unique
    tempo.file <- paste("/tmp/",tail(strsplit(tempfile(patter="rhipelapply.output."),
                                              "/+")[[1]],1),"/",sep="")
    .jcheck();.jcall(rhoptions()$fsshell,"V","makeFolderToDelete",tempo.file,check=T);
    del.o.file <- T}
  else{
    tempo.file <- ofolder
  }
  
  if(is.list(ll)){
    tempi.file <-  paste("/tmp/",tail(strsplit(tempfile(patter="rhipelapply.input.")
                                               ,"/+")[[1]],1),"/",sep="")
    rhwrite(ll,f=paste(tempi.file,"p",sep=""),...)
    ifolder=tempi.file
    del.i.file <- T
  }
  if(is.numeric(ll)){
      mapred$rhipe_lapply_lengthofinput <- ll
    }
  mapred$rhipe_reduce_justcollect <- "TRUE"
    
  if(ifolder!="") inout[1] <- "sequence"
  z <- rhmr(map=map.exp,reduce=NULL,ifolder=ifolder,combiner=F,
            setup=setup,ofolder=tempo.file,inout=inout,mapred=mapred,...)

  h=list(z,function(){
    retdata <- NULL
      on.exit({
            if(del.o.file) rhdel(tempo.file)
            if(del.i.file) rhdel(tempi.file)
            return(retdata)
          })
      if(readIn)
        retdata <- rhread(paste(tempo.file,"/p*",sep=""))
    })
  class(h)="rhlapply"
  h
}


rhex <- function (conf) 
{
  exitf <- NULL
  ## browser()
  if(class(conf)=="rhlapply") {
    zonf <- conf[[1]]
    exitf <- conf[[2]]
  }else if(class(conf)=='rhmr'){
    zonf <- conf$temp
  }else
  stop("Wrong class of list given")
  cmd <- paste("$HADOOP/bin/hadoop jar ", rhoptions()$jarloc, 
               " org.godhuli.rhipe.RHMR ", zonf, sep = "")
  cat("Running: ", cmd, "\n")
  result <- system(cmd)
  if(result==256 && !is.null(exitf)){
    return(exitf())
  }
  return(if(result==256) 1 else 0)
}

