#' A function that returns a function to specify input/output formats
#'
#' Returns a function to spec out the input output formats
#' @param ... arguments passed to the function
#' @param type the name of the function handler
#' @param envir the environment that calls your function
#' @details the function returned must take 3 arguments 'lines',direction(input or output), call signature
#' see \code{rhoptions()$ioformats} for examples on how to write your own.
#' @export
rhfmt <- function(...,type){
  if(!type %in% names(rhoptions()$ioformats))
    stop(sprintf("%s type is not present",type))
  rhoptions()$ioformats[[type]](...)
}

folder.handler <- function(ifolder){
  if(!is.null(ifolder)) ifolder <- rhofolder(ifolder)
  if(all(sapply(ifolder, function(r) nchar(r)>0)))
    ifolder = rhabsolute.hdfs.path(ifolder)
  ifolder
}

lapplyio <- function(args){
  args <- eval(args)
  function(lines,direction,caller){
    if(direction!="input") stop("Cannot use this for anything but input")
    lines$rhipe_inputformat_class <- 'org.godhuli.rhipe.LApplyInputFormat'
    lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHNumeric'
    lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHNumeric'
    seeding <- NULL
    if(length(args)>=2){
      lines$mapred.map.tasks <- as.integer(args[2])
      if(length(args)>2)
        seeding <- as.integer(args[-c(1:2)])
    }
    if(is.null(lines$param.temp.file)){
      lines$param.temp.file <- Rhipe:::makeParamTempFile(file="rhipe-temp-params",list(initPRNG=Rhipe:::initPRNG(seeding)))
    }else{
      lines$param.temp.file$envir$initPRNG <- Rhipe:::initPRNG(seeding)
    }
    expr <- expression({
      initPRNG()
    })
    lines$rhipe_setup_reduce <- c(lines$rhipe_setup_reduce,expr)
    lines$rhipe_setup_map <- c(lines$rhipe_setup_map,expr)
    lines$rhipe_lapply_lengthofinput <- as.integer(args[1])
    lines
  }
}


nullo <- function(){
  function(lines, direction,callers){
    if(direction!="output") stop("Cannot use null for anything but output")
    lines$rhipe_outputformat_class <-'org.apache.hadoop.mapreduce.lib.output.NullOutputFormat'
    lines$rhipe_outputformat_keyclass <- 'org.apache.hadoop.io.NullWritable'
    lines$rhipe_outputformat_valueclass <- 'org.apache.hadoop.io.NullWritablee'
    lines$rhipe_map_output_keyclass <- 'org.apache.hadoop.io.NullWritable'
    lines$rhipe_map_output_valueclass <- 'org.apache.hadoop.io.NullWritable'
    lines$rhipe.use.null <- "TRUE"
    lines
  }
}

mapio <- function(folders,interval=1, compression="BLOCK"){
  folders <- eval(folders); interval <- eval(interval); compression <- eval(compression)
  function(lines,direction,callers){
    if(direction=="input"){
      folders <- Rhipe:::folder.handler(folders)
      uu <- unclass(rhls(folders,rec=TRUE)['file'])$file
      folders <- uu[grep("data$",uu)]
      remr <- c(grep(rhoptions()$file.types.remove.regex,folders))
      interval <- eval(interval);compression <- eval(compression)
      if(length(remr)>0)
        folders <- folders[-remr]
      lines$rhipe_input_folder <- paste(folders,collapse=",")
      lines$rhipe_inputformat_class <- 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
      lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
    }else{
      if(!is.null(lines$mapred.reduce.tasks) && lines$mapred.reduce.tasks==0)
        stop("if you're using map output, use a non zero reducer")
      folders <- Rhipe:::folder.handler(folders)
      lines$rhipe_output_folder <- paste(folders, collapse = ",")
      lines$io.map.index.interval <- interval
      lines$mapred.output.compression.type <- compression
      if(compression == "NONE"){
        lines$mapred.output.compress <- "false"
        lines$mapred.compress.map.output <- "false"
      }
      lines$rhipe_outputformat_class <- "org.godhuli.rhipe.RHMapFileOutputFormat"
      lines$rhipe_outputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
      lines$rhipe_outputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
    }
    lines
  }
}

sequenceio <- function(folders){
  folders <- eval(folders)
  function(lines,direction,callers){
    if(direction=="input"){
      folders <- Rhipe:::folder.handler(folders)
      folders <- rhls(folders,rec=TRUE)$file
      remr <- c(grep(rhoptions()$file.types.remove.regex,folders))
      if(length(remr)>0)
        folders <- folders[-remr]
      lines$rhipe_input_folder <- paste(folders,collapse=",")
      lines$rhipe_inputformat_class <- 'org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat'
      lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
      lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
    }else{ ##output
      folders <- Rhipe:::folder.handler(folders)
      lines$rhipe_output_folder <- paste(folders,collapse=",")
      lines$rhipe_outputformat_class <- 'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat'
      lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
      lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
    }
    lines
  }
}

textio <- function(folders,nline=NULL,writeKey=TRUE, field.sep=" ",kv.sep="\t",eol="\r\n",stringquote=""){
  folders <- eval(folders)
  writeKey <- eval(writeKey); field.sep=eval(field.sep);kv.sep=eval(kv.sep);eol=eval(eol);stringquote=eval(stringquote);
  function(lines,direction,caller){
    if(direction=="input"){
      folders <- Rhipe:::folder.handler(folders)
      folders <- rhls(folders,rec=TRUE)$file
      remr <- c(grep(rhoptions()$file.types.remove.regex,folders))
      if(length(remr)>0)
        folders <- folders[-remr]
      lines$rhipe_input_folder <- paste(folders,collapse=",")
      if(!is.null(nline) && is.integer(nline)){
        lines$rhipe_inputformat_class <- "org.godhuli.rhipe.RNLineInputFormat"
        lines$mapreduce.input.lineinputformat.linespermap <- as.integer(nline)
      }else{
        lines$rhipe_inputformat_class <- 'org.godhuli.rhipe.RXTextInputFormat'
      }
      lines$rhipe_inputformat_keyclass <- 'org.godhuli.rhipe.RHNumeric'
      lines$rhipe_inputformat_valueclass <- 'org.godhuli.rhipe.RHText'
      
      if(is.null(lines$param.temp.file)){
        linesToTable <- Rhipe:::linesToTable
        environment(linesToTable) <- .BaseNamespaceEnv
        lines$param.temp.file <- Rhipe:::makeParamTempFile(file="rhipe-temp-params",list(linesToTable=linesToTable))
      }else{
        linesToTable <- Rhipe:::linesToTable
        environment(linesToTable) <- .BaseNamespaceEnv
        lines$param.temp.file$envir$linesToTable <- linesToTable
      }
    }else{
      folders <- Rhipe:::folder.handler(folders)
      lines$rhipe_output_folder <- paste(folders,collapse=",")
      lines$mapred.textoutputformat.separator <-  kv.sep
      lines$mapred.field.separator <- field.sep
      lines$mapred.textoutputformat.usekey <- writeKey
      lines$rhipe.eol.sequence <- eol
      lines$rhipe_string_quote <- stringquote
      lines$rhipe_outputformat_class <- 'org.godhuli.rhipe.RXTextOutputFormat'
      lines$rhipe_outputformat_keyclass <- 'org.godhuli.rhipe.RHBytesWritable'
      lines$rhipe_outputformat_valueclass <- 'org.godhuli.rhipe.RHBytesWritable'
    }
    lines
  }
}

handleIOFormats <- function(opts){
  opts$ioformats <- list(
                      text=textio,
                      seq=sequenceio,
                      sequence=sequenceio,
                      map=mapio,
                      N=lapplyio,
                      null=nullo)
  opts
}

    
