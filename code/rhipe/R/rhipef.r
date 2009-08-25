

##  RHIPE - software that integrates Hadoop mapreduce with R

##   This program is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as published by
##   the Free Software Foundation; either version 2 of the License, or
##   (at your option) any later version.

##   This program is distributed in the hope that it will be useful,
##   but WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.

##   You should have received a copy of the GNU General Public License
##   along with this program; if not, write to the Free Software
##   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

##  Saptarshi Guha sguha@purdue.edu

  
rhsetoptions <- function(l){

  assign("rhipeOptions",l,envir=.rhipeEnv)
}
rhoptions <- function(){
  return(get("rhipeOptions",envir=.rhipeEnv))
}

rhsave<- function(file,...){
  f=tempfile()
  save(...,file=path.expand(f))
  system(paste("$HADOOP_BIN/hadoop dfs -rmr",file," 1>2",sep=" ",collapse=""),ignore.stderr = TRUE)
  cm <- paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-put",f,file,collapse=" ")
  system(cm)
  on.exit(unlink(f))
}
rhsave.image <- function(file,...){
  rhsave(list = ls(envir = .GlobalEnv),file=file,...)
}
rhload<- function(file,envir=NULL,...){
  f=tempfile()
  cm <- paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-get",file,f,collapse=" ")
  system(cm)
  if(is.null(envir))
    load(file=f,parent.frame(2))
  else
    load(file=f,envir)
  on.exit(unlink(f))
}
rhget<- function(from,to,...){
  cm <- paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-get",from,path.expand(to),collapse=" ")
  system(command=cm,...)
}
rhput<- function(from,to,...){
  cm <- paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-put",path.expand(from),to,collapse=" ")
  system(command=cm,...)
}
rhls <- function(w="/",...){
  system(command=paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-ls",w,collapse=" "),...)
}
rhkill <- function(w,...){
  #w is job
  if(length(grep("^job_",w))==0) w=paste("job_",w,sep="",collapse="")
  system(command=paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"job","-kill",w,collapse=" "),...)
}
rhrm <- function(w,gp=NA,...){
  if(is.na(gp)){
    system(command=paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-rmr",w,collapse=" "),...)
  }else{
    d <- as.character(rhlsd(w,...)[,'path'])
    dd <- d[grep(gp,d)]
    for(xx in dd) {
      system(command=paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-rmr",xx,collapse=" "),...)
    }
##   invisible(.Call("rdeleteFonDFS",f))
  }
}

rdeleteFonDFS <- function(f,l){
  .jcall("org/saptarshiguha/rhipe/utils/Utils","V","deleteFonDFS",f,as.integer(l))
}
rhlapply <- function(list.object=NULL,func,configure=expression(),
                     output.folder='',
                     shared.files=c(),
                     hadoop.mapreduce=list(),verbose=T,takeAll=T,usecache=NULL){
  payload <- list()
  local="notlocal"
  if("mapred.job.tracker" %in% names(hadoop.mapreduce)){
    if(hadoop.mapreduce$mapred.job.tracker=="local") local="local"
  } 
  if(is.list(list.object)){
    payload$list.length<- as.integer(length(list.object))
    payload$list.object <- serialize(NULL,NULL) #list.object,connection=NULL,ascii=F)
    payload$list.object.needed <-as.integer(0)
  }else if(is.null(list.object)){
    if(is.null(usecache)) stop("If not list object,
specify usecache as a valid folder containing sequence files")
    payload$list.length <- 1
    payload$list.object <- serialize(NULL,NULL)
    payload$list.object.needed <-as.integer(0)
  }else{
    if(is.numeric(list.object) & length(list.object)==1){
      payload$list.length <- as.integer(list.object[1]) ##CHANGE CHANGE to numeric
      payload$list.object <- serialize(as.list(list.object),connection=NULL,ascii=F)
      payload$list.object.needed <- as.integer(1)
    }else stop("Invalid list object/replicate number provided")
  } 
  if(is.na(payload$list.length)) stop("Length of list is weird")
  ## Serialized Function
  if(!is.function(func)) stop("func: must be a function")
  payload$func.serialized <- serialize(func,connection=NULL,ascii=F)
  ## Configure
  if(!is.expression(configure))
      stop("configure must be an expression")
  payload$configure <- serialize(configure,connection=NULL)
  ##Output Folder
  if(missing(output.folder)) {
    removetmp=T
    takeAll=T
  } else{
    removetmp <- F
    if(missing(takeAll)) takeAll <- F
  }
  if(!is.character(output.folder)) stop("output.folder must be a character")
  payload$output.folder <- output.folder[1]
  ##Shared files
  payload$shared.files <- unlist(as.character(shared.files))
  if(! all(sapply(payload$shared.files,is.character))) stop("shared.files must be all characters")
  payload$shared.files <- unlist(sapply(payload$shared.files,function(r){
    r1 <- strsplit(r,"/",extended=F)[[1]]
    return(paste(r,r1[length(r1)],sep="#",collapse=''))
  },simplify=T))
  ##Hadoop Mapreduce Options
  if( !"mapred.reduce.tasks" %in% names(hadoop.mapreduce)){
    hadoop.mapreduce$mapred.reduce.tasks=0
  }
  props <- list( c("mapred.job.reuse.jvm.num.tasks",-1))
  for(xx in props){
    if( !xx[1] %in% names(hadoop.mapreduce)) hadoop.mapreduce[[xx[1]]] <- xx[2]
  }

##   props=list(c("mapred.compress.map.output","true"),c("mapred.output.compress","false"),c("mapred.output.compression.type","NONE"))
##   for(xx in props){
##     if( !xx[1] %in% names(hadoop.mapreduce)) hadoop.mapreduce[[xx[1]]] <- xx[2]
##   }

  payload$hadoop.mapreduce <- lapply(hadoop.mapreduce,paste)
  el <- prod(unlist(lapply(payload$hadoop.mapreduce,is.character)))
  if(el==0) stop("invalid entries in hadoop.mapreduce.list")
  if(!is.null(payload$hadoop.mapreduce$mapred.job.tracker) &&
     payload$hadoop.mapreduce$mapred.job.tracker == 'local')
    payload$hadoop.mapreduce$rhipejob.copy.to.dfs='0'
  
  opts <- rhoptions()
  payload$rport = as.integer(opts$rport)
  uuid <- lapplyMap(payload)
  if(is.null(uuid)) stop("Error making mapfile for rhlapply");
  names(uuid) <- "UUID"
  print(uuid)
  mapfile <- paste("/tmp/",uuid,".mapfile",sep="",collapse="")
  workfile="0"
  if(payload$list.object.needed == 0) {
    if(is.null(usecache)){
      workfile=paste("/tmp/",uuid,".tmpinput",sep="",collapse="")
      cfg_ <- rhoptions()$hadoop.cfg
      dfs <- .jcall(cfg_,"S","get","fs.default.name")
      .jcall(cfg_,"V","set","fs.default.name","file:///")
      filesystem <- .jcall("org/apache/hadoop/fs/FileSystem",
                           "Lorg/apache/hadoop/fs/FileSystem;","get",cfg_)
      .jcall(filesystem,"Z","mkdirs", .jnew("org/apache/hadoop/fs/Path",workfile))

      print("Writing temporary sequence files")
      rwk <- .jnew("org.saptarshiguha.rhipe.hadoop.RXWritableRAW")
      rwv <- .jnew("org.saptarshiguha.rhipe.hadoop.RXWritableRAW")
      if(!is.null(hadoop.mapreduce$mapred.map.tasks))
        numtosplit <- as.numeric(hadoop.mapreduce$mapred.map.tasks)
      else
        numtosplit <- length(list.object)
      splits.id <- split(1:length(list.object), sapply(1:length(list.object), "%%",numtosplit))
      inds <- as.integer(quantile(1:length(splits.id),pr=1:10/10))
      lapply(1:length(splits.id),function(idx){
        jvectorKeys <- .jcall("org/saptarshiguha/rhipe/utils/Utils","Ljava/util/Vector;","givemeVec");
        jvectorValues <- .jcall("org/saptarshiguha/rhipe/utils/Utils","Ljava/util/Vector;","givemeVec");
        if( length(yuy <- which(idx==inds))>0)
          print(paste(yuy*10,"%",sep=""))
        sapply(splits.id[[idx]],function(ix){
          ##           browser()
          if(is.null(kk <- names(list.object[ ix ])[1])) kk <- ix
          .jcall("org/saptarshiguha/rhipe/utils/Utils","V","addKVToAVec",
                 jvectorKeys,jvectorValues,
                 rhsz(kk ),rhsz(list.object[[ix]]))
        })
        .jcall("org/saptarshiguha/rhipe/utils/Utils","V","writeAndCloseSeq",
               filesystem,cfg_,paste(workfile,idx,sep="/"),jvectorKeys,jvectorValues,
               rwk,rwv)
        })
      .jcall(cfg_,"V","set","fs.default.name",dfs)
      filesystem <- .jcall("org/apache/hadoop/fs/FileSystem",
                           "Lorg/apache/hadoop/fs/FileSystem;","get",cfg_)
      .jcall(filesystem,"V","moveFromLocalFile",
             .jnew("org.apache.hadoop.fs.Path",workfile),
             .jnew("org.apache.hadoop.fs.Path","/tmp"))
    }else workfile = usecache
  }
  successdownload=F
  on.exit({
    if(local=="local") l=as.integer(1) else l=as.integer(0);
    if(removetmp && successdownload) rdeleteFonDFS(paste("/tmp/",uuid,".work",sep="",collapse=""),l)
    if(successdownload) rdeleteFonDFS(paste("/tmp/",uuid,".mapfile",sep="",collapse=""),l)
  })
  jar=paste(Sys.getenv("RHIPE"),"rhipe.jar",sep=.Platform$file.sep)
  redir <- "true"
  if(!verbose) redir <- "false"
  cmd=paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"jar",jar,"org.saptarshiguha.rhipe.hadoop.RHLApply",mapfile,redir,workfile,local,collapse=" ")
  ret=system(command=cmd,wait=T)
  ret=as.numeric(ret)/256

  if(ret == 115 && takeAll==T) {
    message("downloading results...")
    if(removetmp) j= paste("/tmp/",uuid,".work/part*",sep="",collapse="")
    else j=paste(payload$output.folder,"/part*",sep="",collapse="")
    if(local=="local") l <- T else l <- F
    if(payload$list.object.needed == 0) ignn <- F else ignn <- T
    u <- rhsqallKV(j,payload$list.length,ignore.key=ignn,local=F)
    successdownload=T
    return(u)
  }
  if(ret == 115 && takeAll==F){
    return(T)
  }
  if(ret!=115) return(NULL)

}

lapplyMap <- function(payload){
  cfg <- rhoptions()$hadoop.cfg
  filesystem <- .jcall("org/apache/hadoop/fs/FileSystem","Lorg/apache/hadoop/fs/FileSystem;","get",cfg)
  hash <- .jnew("java/util/Hashtable")
  for(u in names(payload$hadoop.mapreduce)){
    key <- .jnew("java/lang/String", u)
    value <- .jnew("java/lang/String", as.character(payload$hadoop.mapreduce[u]))
    .jcall(hash,"Ljava/lang/Object;","put",.jcast(key,"java/lang/Object"),.jcast(value,"java/lang/Object"))
  }
  uuid <- .jcall("org/saptarshiguha/rhipe/hadoop/RHLApply","S","makeMapFile",
                 cfg,filesystem,as.integer(payload$list.length),as.integer(payload$list.object.needed),
                 payload$list.object,payload$func.serialized,payload$configure,payload$output.folder,
                 c(payload$shared.files,"",""),hash,as.integer(payload$rport))
  return(uuid)
}


rhmr <- function (map, reduce = NA,combiner = F, input.folder, output.folder = "",
                  configure = list(map=expression(),reduce=expression()),
                  close = list(map=expression(), reduce=expression()),
                  shared.files = c(), 
                  inputformat = "TextInputFormat", outputformat = "TextOutputFormat", 
                  hadoop.mapreduce = list(), verbose = T, step=F,libjars = "") 
{
    payload <- list()
    local = "notlocal"
    if ("mapred.job.tracker" %in% names(hadoop.mapreduce)) {
        if (hadoop.mapreduce$mapred.job.tracker == "local") 
            local = "local"
    }
    if (!is.function(map)) 
        stop("map: must be a function")
    payload$mapper <- serialize(map, connection = NULL, ascii = F)
    if ((("mapred.reduce.tasks" %in% names(hadoop.mapreduce) && 
        as.character(hadoop.mapreduce$mapred.reduce.tasks) == 
            "0"))) 
        nmt = T
    else nmt = F
    if (!is.function(reduce)  && !nmt) 
        stop("reduce: must be a function or expression")
    if(step && combiner) 
        stop("Cannot use a combiner when reducer is to be run for every value")
    payload$reducer <- serialize(reduce, connection = NULL, ascii = F)
    payload$hadoop.mapreduce = list()
    payload$hadoop.mapreduce$rhipejob.combinerspill = as.integer(1e+05)
    payload$hadoop.mapreduce$rhipejob.needcombiner = as.integer(combiner[1])
    if(nmt) payload$hadoop.mapreduce$rhipejob.needcombiner = as.integer(0)
    payload$hadoop.mapreduce$rhipejob.cutoff = 1
    payload$hadoop.mapreduce$rhipejob.input.folder = as.character(paste(input.folder,collapse=","))
    payload$hadoop.mapreduce$rhipejob.reduce.stepper = as.integer(step)

    ##Configure
    configure1 <- list()
    if(length(configure)==1 && is.null(names(configure))){
      if(!is.expression(configure[[1]])) stop("The configure must be equal to an expression")
      configure1$map <- configure[[1]];
      configure1$reduce <- configure[[1]]
    }else{
      if("map" %in% names(configure)){
        if(!is.expression(configure$map))  stop("The configure$map must be equal to an expression")
        configure1$map <- configure$map
      }else configure1$map <- expression()
      if("reduce" %in% names(configure)){
        if(!is.expression(configure$reduce))  stop("The configure$reduce must be equal to an expression")
        configure1$reduce <- configure$reduce
      }else configure1$reduce <- expression()
    }
    payload$configure <- serialize(configure1, connection = NULL)


    ##Close
    cloze1 <- list()
    if(length(close)==1 && is.null(names(close))){
      if(!is.expression(close[[1]])) stop("The close must be equal to an expression")
      cloze1$map <- close[[1]];
      cloze1$reduce <- close[[1]]
    }else{
      if("map" %in% names(close)){
        if(!is.expression(close$map))  stop("The close$map must be equal to an expression")
        cloze1$map <- close$map
      }else cloze1$map <- expression()
      if("reduce" %in% names(close)){
        if(!is.expression(close$reduce))  stop("The close$reduce must be equal to an expression")
        cloze1$reduce <- close$reduce
      }else cloze1$reduce <- expression()
    }
    payload$cloze <- serialize(cloze1, connection = NULL)
    
    if (!is.character(output.folder)) 
        stop("output.folder must be a character")
    payload$hadoop.mapreduce$rhipejob.output.folder <- output.folder[1]
    payload$shared.files <- unlist(as.character(shared.files))
    if (!all(sapply(payload$shared.files, is.character))) 
        stop("shared.files must be all characters")
    payload$shared.files <- unlist(sapply(payload$shared.files, function(r) {
        r1 <- strsplit(r, "/", extended = F)[[1]]
        return(paste(r, r1[length(r1)], sep = "#", collapse = ""))
    }, simplify = T))
    payload$hadoop.mapreduce$rhipejob.lowmem <- 0
    props <- list(c("mapred.job.reuse.jvm.num.tasks", -1), c("mapred.textoutputformat.separator", 
        " "), c("rhipejob.textoutput.fieldsep", " "), c("rhipejob.logical.true", 
        "TRUE"), c("rhipejob.logical.false", "FALSE"), c("rhipejob.int.logical.na", 
        "NA"), c("rhipejob.textinput.comment", "#"))
    for (xx in props) {
        if (!xx[1] %in% names(hadoop.mapreduce)) 
            hadoop.mapreduce[[xx[1]]] <- xx[2]
    }
    for (x in names(hadoop.mapreduce)) payload$hadoop.mapreduce[[x]] = hadoop.mapreduce[[x]]
    opts <- rhoptions()
    payload$hadoop.mapreduce$rhipejob.rport = opts$rport
    inpformat <- as.character(inputformat[1])
    payload$hadoop.mapreduce$rhipejob.outfmt.is.text = "0"
    if (inpformat == "TextInputFormat") {
        payload$hadoop.mapreduce$rhipejob.inputformat.keyclass = "org.saptarshiguha.rhipe.hadoop.RXWritableLong"
        payload$hadoop.mapreduce$rhipejob.inputformat.valueclass = "org.saptarshiguha.rhipe.hadoop.RXWritableText"
        payload$hadoop.mapreduce$rhipejob.input.format.class = "org.saptarshiguha.rhipe.hadoop.RXTextInputFormat"
    }
    else if (inpformat == "SequenceFileInputFormat") {
        payload$hadoop.mapreduce$rhipejob.inputformat.keyclass = "org.saptarshiguha.rhipe.hadoop.RXWritableRAW"
        payload$hadoop.mapreduce$rhipejob.inputformat.valueclass = "org.saptarshiguha.rhipe.hadoop.RXWritableRAW"
        payload$hadoop.mapreduce$rhipejob.input.format.class = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    }else{
      payload$hadoop.mapreduce$rhipejob.input.format.class=inpformat
      if(is.null(payload$hadoop.mapreduce$rhipejob.inputformat.keyclass) || is.null(payload$hadoop.mapreduce$rhipejob.inputformat.valueclass)){
        print("If you specify a different inputformat, also specify the rhipejob.inputformat.keyclass and rhipejob.inputformat.valueclass")
      }
     } 
    opformat <- as.character(outputformat[1])
    if (opformat == "TextOutputFormat") {
        if(is.null(hadoop.mapreduce$rhipejob.outputformat.keyclass))
          payload$hadoop.mapreduce$rhipejob.outputformat.keyclass = "org.saptarshiguha.rhipe.hadoop.RXWritableText"
        else
          payload$hadoop.mapreduce$rhipejob.outputformat.keyclass = hadoop.mapreduce$rhipejob.outputformat.keyclass
        payload$hadoop.mapreduce$rhipejob.outputformat.valueclass = "org.saptarshiguha.rhipe.hadoop.RXWritableText"
        payload$hadoop.mapreduce$rhipejob.output.format.class = "org.saptarshiguha.rhipe.hadoop.RXTextOutputFormat" # "org.apache.hadoop.mapred.TextOutputFormat"
        payload$hadoop.mapreduce$rhipejob.outfmt.is.text = "1"
    }
    else if (opformat == "SequenceFileOutputFormat") {
      payload$hadoop.mapreduce$rhipejob.outputformat.keyclass = "org.saptarshiguha.rhipe.hadoop.RXWritableRAW"
      payload$hadoop.mapreduce$rhipejob.outputformat.valueclass = "org.saptarshiguha.rhipe.hadoop.RXWritableRAW"
      payload$hadoop.mapreduce$rhipejob.output.format.class = "org.apache.hadoop.mapred.SequenceFileOutputFormat"
    }else{
      payload$hadoop.mapreduce$rhipejob.output.format.class=opformat
      if(is.null(payload$hadoop.mapreduce$rhipejob.outputformat.keyclass) || is.null(payload$hadoop.mapreduce$rhipejob.outputformat.valueclass)){
        print("If you specify a different outputformat, also specify the rhipejob.outputformat.keyclass and rhipejob.outputformat.valueclass")
      }
    }
    payload$hadoop.mapreduce <- lapply(payload$hadoop.mapreduce, 
        as.character)
    uuid <- makeMR(payload)
    if (is.null(uuid)) 
        stop("Error making mapfile for rhmr")
    names(uuid) <- "UUID"
    print(uuid)
    successis = F
    mapfile <- paste("/tmp/", uuid, ".mapfile", sep = "", collapse = "")
    on.exit({
        if (successis) rdeleteFonDFS( paste("/tmp/", 
            uuid, ".mapfile", sep = "", collapse = ""), as.integer(0))
    })
    jar = paste(Sys.getenv("RHIPE"),  "rhipe.jar", sep = .Platform$file.sep)
    redir <- "true"
    if (!verbose) 
        redir <- "false"
    libjars <- c(libjars)
    if (libjars != "") 
        libjars = paste("-libjars", paste(libjars, sep = "", 
            collapse = ","))
    else libjars = ""
    cmd = paste(paste(Sys.getenv("HADOOP_BIN"), "hadoop", sep = .Platform$file.sep, 
        collapse = ""), "jar", jar, "org.saptarshiguha.rhipe.hadoop.RHMR", 
        libjars, mapfile, redir, local, collapse = " ")
    print(cmd)
    ret = system(command = cmd, wait = T)
    ret = as.numeric(ret)/256
    ret
    if (ret == 115) 
        successis = T
}
## ([B[B[B[B[Ljava/lang/String;Ljava/util/Hashtable;)Ljava/lang/String;
## ([I[I[B[B[B[B[Ljava/lang/String;Ljava/util/Hashtable;)
makeMR <- function(payload){
  cfg <- rhoptions()$hadoop.cfg
  filesystem <- .jcall("org/apache/hadoop/fs/FileSystem","Lorg/apache/hadoop/fs/FileSystem;","get",cfg)
  hash <- .jnew("java/util/Hashtable")
  for(u in names(payload$hadoop.mapreduce)){
    key <- .jnew("java/lang/String", u)
    value <- .jnew("java/lang/String", as.character(payload$hadoop.mapreduce[u]))
    .jcall(hash,"Ljava/lang/Object;","put",.jcast(key,"java/lang/Object"),.jcast(value,"java/lang/Object"))
  }
  uuid <- .jcall("org/saptarshiguha/rhipe/hadoop/RHMR","S","makeMapFile",cfg,filesystem,
                 payload$mapper,payload$reducer,
                 payload$configure,payload$cloze, 
                 c(payload$shared.files,"",""),hash)
  return(uuid)
}
