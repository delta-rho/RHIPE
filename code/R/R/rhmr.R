
#' Prepares a MapReduce Job For Execution
#'
#' Creates the R object that contains all the information required by RHIPE to
#' run a MapReduce job via a call to \code{\link{rhex}} (see details).
#' 
#' @param map \code{map} is an R expression (created using the R command
#'   \code{expression}) that is evaluated by RHIPE during the map stage. For
#'   each task, RHIPE will call this expression multiple times (see details).
#' @param reduce \code{reduce} is an R expression (created using the R command
#'   \code{expression}) that is evaluated by RHIPE during the reduce stage, or
#'   it is a vector of expressions with names pre, reduce, and post.  For
#'   example \code{reduce = expression(pre={...}, reduce={...}, post={...})}.
#'   \code{reduce} is optional, and if not specified the map output keys will
#'   be sorted and shuffled and saved to disk.
#' @param combiner
#' 
#' If set to TRUE, RHIPE will run the \code{reduce} expression on the output of
#'   the \code{map} expression locally i.e. on the same computer that is
#'   running the associated map after \emph{io.sort.mb} megabytes of key,value
#'   pairs.
#' 
#' See details.
#' 
#' WARNING: setup/cleanup may not run when you think when used with a combiner.
#'   We recommend only advanced users try to use both a combiner and
#'   setup/cleanup expressions.
#' @param setup An expression of R code to be run before map and reduce.
#'   Alternatively a list with elements map and reduce e.g
#'   \code{setup=list(map=,reduce=)} and each of those is, ran respectively,
#'   before the map and reduce phases. See details.
#' @param cleanup As in setup except cleanup runs after the map and reduce
#'   phases.
#' @param ofolder The destination of the output. If the destination already
#'   exists, it will be overwritten. This is not needed if there is not output.
#' @param ifolder This is a path to a folder on the HDFS containing the input
#'   data. This folder may contain sub folders in which case RHIPE use the all
#'   the files in the subfolders as input. This argument is optional: if not
#'   provided, the user must provide a value for \code{N} and set the first
#'   value of \code{inout} to \code{lapply}.
#' @param inout A character vector of one or two components which specify the
#'   formats of the input and output destinations. If \code{inout} is of length
#'   one this specifies the input format, the output being NULL (nothing is
#'   written) Vector element values must be from c("sequence", "text", "map",
#'   "lapply").  See details. Also, see argument \code{N} for information about
#'   the "lapply" value.
#' @param orderby This is one of \emph{bytes}, \emph{integer} , \emph{numeric}
#'   and \emph{character}. The intermediate keys will be ordered assuming the
#'   output key in the map is of that type. If not of the type an exception
#'   will be thrown. Tuples can be sorted too, see \emph{Tuple Sorting} in the
#'   online documentation pdf.
#' @param mapred Specify Hadoop and RHIPE options in this parameter (a list).
#'   See details and for Hadoop options go
#'   \href{http://hadoop.apache.org/common/docs/current/mapred-default.html}{here}.
#' @param shared This is a character vector of files located on the HDFS. At
#'   the beginning of the MapReduce job, these files will be copied to the
#'   local hard disks of the Tasktrackers (cluster computers on which the
#'   compute nodes/cores are located). See details.
#' @param jarfiles Optional JARs that need to be used during Hadoop MapReduce.
#'   This is used in the case when a user provides a custom InputFormat.
#'   Specify the JAR file to handle this InputFormat using this argument and
#'   specify the name of the InputFormat in the \code{mapred} argument.
#' @param zips Distributed cache file on the HDFS to unzip and distribute to each MapReduce task.  See 
#'   \href{http://hadoop.apache.org/common/docs/r0.20.1/mapred_tutorial.html\#DistributedCache}{Distributed Cache}.
#' @param partitioner A list of two names elements: \code{lims} and
#'   \code{type}.  See details.
#' @param copyFiles Will the files created in the R code e.g. PDF output, be
#'   copied to the destination folder, \code{ofolder}?
#' @param N To apply a computation to the numbers 1, 2, \ldots{}, \emph{N}, set
#'   argument \code{inout[[1]]} to \code{"lapply"} and specify the value of
#'   \emph{N} in this parameter. Set the number of map tasks in
#'   \code{mapred.map.tasks} (hence each task will run approximately
#'   floor(\emph{N}/\code{mapred.map.tasks}) computations sequentially).
#' @param opts RHIPE launches the C engine on the remote computers using the
#'   value found in \code{rhoptions()$opts$runner}. This is created from the
#'   local R installation which is possibly different from the Tasktrackers. If
#'   this is the case, specify the command that launches the R session via this
#'   parameter.
#' @param jobname The name of the job, which is visible on the Jobtracker
#'   website. If not provided, Hadoop MapReduce uses the default name
#'   \emph{job_date_time_number} e.g. \code{job_201007281701_0274}.
#' @param parameters A list argument.  Each element of the list must have a name.  Each element of the list will be placed in the global environment in MapReduce.  For example \code{parameters = list(arg1 = 1, arg2 = 2)} will place in the global environment for maps and reduces arg1 and arg2 with integer values 1 and 2 respectively. 
#' @return Returns an object of class \code{rhmr} suitable for beginning
#'   executing a Hadoop job with \code{rhex}.
#' @author Saptarshi Guha
#' @details \itemize{ 
#'   \item{Buffer Size:}{
#' If a task consists of \emph{W} key,value pairs, the expression \code{map}
#' will be called 
#' ceil(\emph{W} / \emph{rhipe_map_buffsize}) times. The default
#' value of \emph{rhipe_map_buffsize} is 10,000 and is user configurable. Each
#' time \code{map} is called, the vectors \code{map.keys} and \code{map.values}
#' contain \emph{rhipe_map_buffsize} keys and values respectively. If the
#' objects are large it advisable to reduce the size of
#' \emph{rhipe_map_buffsize}, so that the total amount of memory used by a task
#' is well controlled.  For particularly large map.values, the authors have
#' used rhipe_map_buffsize as low as 10.}
#' \item{Setup:}{
#' In RHIPE, each task is a sequence of many thousands of key, value pairs.
#' Before running the \code{map} and \code{reduce} expression (and before any
#' key, value pairs have been read), RHIPE will evaluate expressions in
#' \code{setup} and \code{cleanup}. Each of these may contain the names
#' \code{map} and \code{reduce} e.g \code{setup=list(map=,reduce=)} specific to
#' the \code{map} and \code{reduce} expressions. If just an expressions is
#' provided, it will be evaluated before both the Map phase and Reduce phase.
#' The same is true for \code{cleanup}. Variables created, packages loaded in
#' the \code{setup} expression will be visible in the \code{map} and the
#' \code{reduce} expression but not both since both are evaluated in different
#' R sessions (except when using a combiner).}
#' \item{Sorting and Shuffling:}{
#' To turn off sorting and shuffling and instead write the map output to disk
#' directly, set \code{mapred.reduce.tasks} to zero in \code{mapred}. In this
#' case, the output keys are not sorted and the output format should not be
#' \emph{map} (since a map file expects sorted keys).}
#' \item{Using a Combiner:}{
#' If \code{combiner} is TRUE, the \code{reduce} expression will be invoked
#' during the local combine, in which case the output is intermediate and not
#' saved as final output. The \code{reduce} expression also be invoked during
#' the final reduce phase, in which case it will receive all the values
#' associated with the key (note, these are values outputted when \code{reduce}
#' is invoked as a combiner) and the output will be committed to the
#' destination folder.
#' To determine in which state \code{reduce} is running read the environment
#' variable \code{rhipe_iscombining} which is `1' (also the R symbol
#' \code{rhipe_iscombining} is equal TRUE) or `0' for the former and latter
#' states respectively.
#' WARNING: setup and cleanup may not run when you think when used with a
#' combiner.  We recommend only advanced users try to use both a combiner and
#' setup/cleanup expressions.}
#' \item{Using Shared Files:}{
#' \code{shared} is a character vector of files located on the HDFS. At the
#' beginning of the MapReduce job, these files will be copied to the local hard
#' disks of the Tasktrackers (cluster computers on which the compute
#' nodes/cores are located). User provided R code can read theses files from
#' the current directory (which is located on the local hard disk). For
#' example, if \emph{/path/to/file.Rdata} is located on the HDFS and shared, it
#' is possible to read it in the R expressions as \code{load('file.Rdata')}.
#' Note, there is no need for the full path, the file is copied to the current
#' directory of the R process.}
#' \item{inout:}{File Types}
#' \itemize{
#' \item{"sequence"}{
#' The keys and values can be arbitrary R objects. All the information of the
#' object will be preserved. To extract a single key,value pair from a sequence
#' file, either the user has to read the entire file or compose a MapReduce job
#' to subset that key,value pair.}
#' \item{"text"}{
#' The keys, and values are stored as lines of text. If the input is of text
#' format, the keys will be byte offsets from beginning of the file and the
#' value is a line of text without the trailing newline. R objects written to a
#' text output format are written as one line. Characters are quoted and
#' vectors are separated by \code{mapred.field.separator} (default is space).
#' The character used to separate the key from the value is specified in the
#' \code{mapred} argument by setting \code{mapred.textoutputformat.separator}
#' (default is tab). To not output the key, set
#' \code{mapred.textoutputformat.usekey} to FALSE.}
#' \item{"map"}{
#' A map file is actually a folder consisting of sequence file and an index
#' file. A small percentage of the keys in the sequence file are stored in the
#' index file. Using the index file, Hadoop can very quickly return a value
#' corresponding to a key (using \code{rhgetkey}). To create such an output
#' format, use \emph{map}. Note, the keys have to be saved in sorted order. The
#' keys are sent to the \code{reduce} expression in sorted order, hence if the
#' user does not modify \code{reduce.key} a query-able map file will be
#' created. If \code{reduce.key} is modified, the sorted guarantee does not
#' hold and RHIPE will either throw an error or querying the output for a key
#' might return with empty results. MapFiles cannot be created if
#' \code{orderby} is not \emph{bytes}.}
#' }
#' \item{Custom Partitioning:}{
#' A list of two names elements: \code{lims} and \code{type}. A partitioner
#' forces all keys sharing the same property to be processed by one reducer.
#' Thus, for these keys, the output of the reduce phase will be saved in one
#' file. For example, if the keys were IP addresses e.g. \code{c(A,B,C,D)}
#' where the components are integers, with the default partitioner, the space
#' of keys will be uniformly distributed across the number of reduce tasks. If
#' it is desired to store all IP addresses with the same first three ordinates
#' in one file (and processed by one R process), use a partitioner as
#' \code{list(lims=c(1:3), type='integer')}. RHIPE implements partitioners when
#' the key is an atomic vector of the following type: integer, string, and
#' real. The value of \code{lims} specifies the ordinates (beginning and end)
#' of the key to partition on. The numbers must be positive. \code{lims} can be
#' a single number.}
#' \item {Avoid Time Outs:}{
#' To avoid time outs during long map or reduce expressions, your MapReduce
#' expressions should report status messages via calls to rhstatus. In the
#' absence of \code{rhstatus} and if \code{mapred.task.timeout} is non zero (by
#' default it is 10 minutes) Hadoop will eventually kill a lengthy R process.}
#' \item{List of Important Options for the mapred argument:}{
#' These are all set with mapred = list( name=value, name=value,...).}
#' \itemize{ 
#'   \item{rhipe_map_buffsize:}{
#' Number of elements in the map buffer. (not size in bytes!)  Control the
#' amount of memory your map task employ using this.}
#' \item{rhipe_reduce_buffsize:}{
#' Number of elements in the reduce.values buffer. (not size in bytes!)
#' Control the amount of memory your reduce task employ using this.}
#' \item{rhipe_stream_buffer:}{.}
#' \item{mapred.task.timeout}{
#' If non-zero the number of milliseconds before a task times out.}
#' \item{mapred.reduce.tasks}{
#' If zero then no reducer is run and map output is placed directly on disk
#' without shuffling or sorting.  If non-zero, the number of simultaneous
#' reduce task to launch.}
#' \item{mapred.map.tasks}{
#' The number of simultaneous map task to launch.}
#' }
#' }
#' @seealso \code{\link{rhex}}, \code{\link{rhstatus}}, \code{\link{rhkill}}
#' @keywords Hadoop MapReduce
#' @examples
#' 
#' \dontrun{
#'    #RUNNABLE BUT ARTIFICIAL EXAMPLE
#'    #We will create a data set with three columns: 
#'    #the level of a categorical variable A, a time variable B and a value C. 
#'    #For each level of A, we want the sum of differences of C ordered by B within A.
#'    #Creating the Data set The column A is the key, but this is not important. 
#'    #There are 5000 levels of A, each level has 10,000 observations. 
#'    #By design the values of B are randomly written (sample), 
#'    #also for simplicity C is equal to B, though this need not be.
#' 
#' library(Rhipe)
#' rhinit()
#' 
#' #might need a call here to rhoptions for runner option depending on user
#' 
#' map <- expression({
#'    N <- 10000
#'    for( first.col in map.values ){
#'       w <- sample(N,N,replace=FALSE)
#'       for( i in w)
#'          rhcollect(first.col,c(i,i))
#'    
#'    }
#' })
#' mapred <- list(mapred.reduce.tasks=0)
#' z=rhmr(map=map, N=5000, inout=c("lapply","sequence"),ofolder="/tmp/sort",mapred=mapred)
#' ex = rhex(z, async=TRUE)
#' rhstatus(ex)    #Wait for job to finish; Ctrl+C to quit
#' 
#' 
#' 
#' #Sum of Differences The key is the value of A and B, the value is C.
#' 
#' map <- expression({
#'    for(r in seq_along(map.values)){
#'       f <- map.values[[r]]
#'       rhcollect(as.integer(c(map.keys[[r]],f[1])),f[2])
#'    }
#' })
#' 
#' 
#' reduce.setup <- expression({
#'    newp <- -Inf
#'    diffsum <- NULL
#' })
#' reduce <- expression(
#'    pre={
#'       if(reduce.key[[1]][1] != newp) {
#'          if(newp>-Inf) 
#'             rhcollect(newp, diffsum) #prevents -Inf from being collected
#'          diffsum <- 0
#'          lastval <- 0
#'          newp <- reduce.key[[1]][1]
#'          skip <- TRUE
#'       }
#'     }, 
#'     reduce={
#'       current <- unlist(reduce.values) #only one value!
#'       if(!skip) 
#'          diffsum <- diffsum + (current-lastval) 
#'       else 
#'          skip <- FALSE
#'       lastval <- current
#'    }
#' )
#' reduce.cleanup <- expression({
#'    if(newp>-Inf) rhcollect(newp,diffsum) #for the last key
#' })
#' 
#' #To turn on the partitioning and ordering of keys,
#' z <- rhmr(map=map,reduce=reduce, inout=c("sequence","sequence"),
#'       ifolder="/tmp/sort",ofolder="/tmp/sort2", part=list(lims=1,type="integer"),
#'       orderby="integer",cleanup=list(reduce=reduce.cleanup),
#'       setup=list(reduce=reduce.setup))
#' ex = rhex(z, async=TRUE)
#' rhstatus(ex)  #Wait for job to finish; ctrl + C to quit
#' }
#' @export

rhmr <- function(map=NULL,reduce=NULL,
                 combiner=FALSE,
                 setup=NULL,
                 cleanup=NULL,
                 ofolder='',
                 ifolder='',
                 inout=c("sequence","sequence"),
                 orderby='bytes',
                 mapred=NULL,
                 shared=c(),
                 jarfiles=c(),
                 zips=c(),
                 partitioner=NULL,
                 copyFiles=F,
                 N=NA,
                 opts=rhoptions(),
                 jobname="",
                 parameters=NULL){
  lines <- list();
  is.Expression <- function(r) is.expression(r) || class(r)=="{"
  parent.deserial.code <- expression({
    param.names <- Sys.getenv("rhipe.params.names")
    if(!is.null(param.names)){
      nam <- strsplit(param.names,";")[[1]]
      for(x in nam) assign(x=x,value=unserialize(charToRaw(Sys.getenv(x))),envir=.GlobalEnv)
    }
  })
  if(!is.Expression(map))
    stop("'map' must be an expression")
  reduces <- T
  lines$rhipe_reduce_justcollect <- "FALSE"
  
  if(is.null(reduce)){
    reduces <- FALSE
  }
  
  lines$rhipe_reduce <- rawToChar(serialize(reduce$reduce,NULL,ascii=T))
  lines$rhipe_reduce_prekey <- rawToChar(serialize(reduce$pre ,NULL,ascii=T))
  lines$rhipe_reduce_postkey <- rawToChar(serialize(reduce$post,NULL,ascii=T))

  lines$rhipe_jobname=jobname
  if(combiner & is.null(reduce))
    combiner <- FALSE
  if(is.null(setup)){
    setup$map <- expression()
    setup$reduce <- expression()
  }

  if(!is.Expression(setup) && !is.list(setup))
    stop("'setup' is either a list of expressions (map=,reduce=) or expression")

  if(is.list(setup)){
    if(! all(unlist(lapply(setup,is.Expression))))
      stop("elements of 'setup' must be expressions")
    if(is.null(setup$reduce)) setup$reduce <- expression()
    if(is.null(setup$map)) setup$map <- expression()
  }
  if(is.null(setup))
    setup <- expression()
  if(is.Expression(setup)){
    setup <- list(map=setup,reduce=setup)
  }
  setup$map <- c(parent.deserial.code,setup$map)
  setup$reduce <- c(parent.deserial.code,setup$reduce)
  
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
  
  map.s <- rawToChar(serialize(map,NULL,ascii=T))
  
  setup.m <- rawToChar(serialize(setup$map,NULL,ascii=T))
  setup.r <- rawToChar(serialize(setup$reduce,NULL,ascii=T))
  cleanup.m <- rawToChar(serialize(cleanup$map,NULL,ascii=T))
  cleanup.r <- rawToChar(serialize(cleanup$reduce,NULL,ascii=T))

  if(ofolder == ""){
    if(!is.null(rhoptions()$HADOOP.TMP.FOLDER)){
      fnames <- rhls(rhoptions()$HADOOP.TMP.FOLDER)$files
      library(digest)
      w. <- if(grepl("/$",rhoptions()$HADOOP.TMP.FOLDER)) "" else "/"
      ofolder <- sprintf("rhipe-temp-%s%s%s",rhoptions()$HADOOP.TMP.FOLDER, w., digest(fnames, "md5"))
      read.and.delete.ofolder <- TRUE
    }else{
      stop("parameter ofolder is default '' and RHIPE could not find a value for HADOOP.TMP.FOLDER in rhoptions().\n Set this: rhoptions(HADOOP.TMP.FOLDER=path)")
    }
  }else{
    read.and.delete.ofolder <- FALSE
  }

  ofolder <- sapply(ofolder,function(r) {
    x <- if(substr(r,nchar(r),nchar(r))!="/" && r!=""){
     paste(r,"/",sep="")
   } else r
  })
  names(ofolder) <- NULL
  
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
                            ,rhipe.read.and.delete.ofolder=read.and.delete.ofolder
                            ,rhipe_map=(map.s)
                            ,rhipe_setup_map=(setup.m)
                            ,rhipe_cleanup_map= (cleanup.m)
                            ,rhipe_cleanup_reduce= (cleanup.r)
                            ,rhipe_setup_reduce= (setup.r)
                            ,rhipe_command=paste(opts$runner,collapse=" ")
                            ,rhipe_input_folder=paste(ifolder,collapse=",")
                            ,rhipe_output_folder=paste(ofolder,collapse=",")))

  shared.files <- unlist(as.character(shared))
  if(! all(sapply(shared.files,is.character)))
    stop("shared  must be all characters")
  shared.files <- unlist(sapply(shared.files,function(r){
    r1 <- strsplit(r,"/")[[1]]
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
           lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'#'org.apache.hadoop.io.NullWritable'
           lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'#'org.apache.hadoop.io.NullWritable'
           lines$rhipe_map_output_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'#'org.apache.hadoop.io.NullWritable'
           lines$rhipe_map_output_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'#'org.apache.hadoop.io.NullWritable'
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
  lines$rhipe_send_keys_to_map <- 1L
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
  lines$rhipe_reduce_buff_size <- 6000
  lines$rhipe_map_buff_size <- 3000
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
  lines$RHIPE_DEBUG <- 0
  lines$rhipe_map_input_type <- "default"
  lines$mapred.job.reuse.jvm.num.tasks <- -1
  for(n in names(mapred)) lines[[n]] <- mapred[[n]]
  
  lines$rhipe_combiner <- paste(as.integer(combiner))
  if(lines$rhipe_combiner=="1")
    lines$rhipe_reduce_justcollect <- "FALSE"
  if(length(jarfiles)>0) {
    lines$rhipe_jarfiles <- paste(path.expand(jarfiles),collapse=",")
    ## make a temp folder containing jar files
    p <- tempdir()
    invisible(sapply(jarfiles, function(r) rhget(r, p)))
    lines$rhipe_cp_tempdir <- p
    lines$rhipe_classpaths <- paste(list.files(p,full.names=TRUE),collapse=",")
  }else {
    lines$rhipe_jarfiles=""
    lines$rhipe_classpaths <- ""
  }

  zips <- c(zips,rhoptions()$zip.files)
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

  if(lines$rhipe_map_output_keyclass != c("org.godhuli.rhipe.RHBytesWritable")
     && is.null(reduce)){
    stop("If using ordered keys, provide a reduce even a dummy reduce e.g.

reduce = expression(
  reduce={ lapply(reduce.values,function(r) rhcollect(reduce.key,r)) }
)
")
  }
  .s <- 0
  for(i in names(parameters)){
    lines[[i]] = rawToChar(serialize(parameters[[i]],NULL,ascii=TRUE))
    .s <- .s+nchar(lines[[i]])
  }
  if(length(parameters)>0) {
    message(sprintf("Serialized %s Kb from %s parameters", round(.s/1024,1), length(parameters)))
    lines[["rhipe.params.names"]] <- paste(names(parameters),collapse=";")
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
       


