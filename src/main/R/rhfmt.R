#' A function that returns a function to specify input/output formats
#'
#' Returns a function to spec out the input output formats
#' @param type the name of the function handler
#' @param ... arguments passed to the function
#' @details the function returned must take 3 arguments 'lines',direction(input or output), call signature
#' see \code{rhoptions()$ioformats} for examples on how to write your own.
#' @export
rhfmt <- function(type, ...) {
   if (!type %in% names(rhoptions()$ioformats)) 
      stop(sprintf("%s type is not present", type))
   rhoptions()$ioformats[[type]](...)
}

folder.handler <- function(ifolder) {
   if (!is.null(ifolder)) 
      ifolder <- rhofolder(ifolder)
   if (all(sapply(ifolder, function(r) nchar(r) > 0))) 
      ifolder <- rhabsolute.hdfs.path(ifolder)
   ifolder
}

lapplyio <- function(args) {
   args <- eval(args)
   function(lines, direction, caller) {
      if (direction != "input") 
         stop("Cannot use this for anything but input")
      lines$rhipe_inputformat_class <- "org.godhuli.rhipe.LApplyInputFormat"
      lines$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHNumeric"
      lines$rhipe_inputformat_valueclass <- "org.godhuli.rhipe.RHNumeric"
      seeding <- NULL
      if (length(args) >= 2) {
         lines$mapred.map.tasks <- as.integer(args[2])
         if (length(args) > 2) 
            seeding <- as.integer(args[-c(1:2)])
      }
      if (is.null(lines$param.temp.file)) {
         lines$param.temp.file <- Rhipe:::makeParamTempFile(file = "rhipe-temp-params", 
            list(initPRNG = Rhipe:::initPRNG(seeding)))
      } else {
         lines$param.temp.file$envir$initPRNG <- Rhipe:::initPRNG(seeding)
      }
      expr <- expression({
         initPRNG()
      })
      lines$rhipe_setup_reduce <- c(lines$rhipe_setup_reduce, expr)
      lines$rhipe_setup_map <- c(lines$rhipe_setup_map, expr)
      lines$rhipe_lapply_lengthofinput <- as.integer(args[1])
      lines
   }
}


nullo <- function() {
   function(lines, direction, callers) {
      if (direction != "output") 
         stop("Cannot use null for anything but output")
      lines$rhipe_outputformat_class <- "org.apache.hadoop.mapreduce.lib.output.NullOutputFormat"
      lines$rhipe_outputformat_keyclass <- "org.apache.hadoop.io.NullWritable"
      lines$rhipe_outputformat_valueclass <- "org.apache.hadoop.io.NullWritablee"
      lines$rhipe_map_output_keyclass <- "org.apache.hadoop.io.NullWritable"
      lines$rhipe_map_output_valueclass <- "org.apache.hadoop.io.NullWritable"
      lines$rhipe.use.null <- "TRUE"
      lines
   }
}

mapio <- function(folders, interval = 1, compression = "BLOCK") {
   folders <- eval(folders)
   interval <- eval(interval)
   compression <- eval(compression)
   function(lines, direction, callers) {
      if (direction == "input") {
         folders <- Rhipe:::folder.handler(folders)
         uu <- unclass(rhls(folders, recurse = TRUE)["file"])$file
         folders <- uu[grep("data$", uu)]
         remr <- c(grep(rhoptions()$file.types.remove.regex, folders))
         interval <- eval(interval)
         compression <- eval(compression)
         if (length(remr) > 0) 
            folders <- folders[-remr]
         lines$rhipe_input_folder <- paste(folders, collapse = ",")
         lines$rhipe_inputformat_class <- "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
         lines$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         lines$rhipe_inputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
      } else {
         if (!is.null(lines$mapred.reduce.tasks) && lines$mapred.reduce.tasks == 
            0) 
            stop("if you're using map output, use a non zero reducer")
         folders <- Rhipe:::folder.handler(folders)
         lines$rhipe_output_folder <- paste(folders, collapse = ",")
         lines$io.map.index.interval <- interval
         lines$mapred.output.compression.type <- compression
         if (compression == "NONE") {
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

sequenceio <- function(folders, recordsAsText = FALSE) {
   folders <- eval(folders)
   recordsAsText <- eval(recordsAsText)
   function(lines, direction, callers) {
      if (direction == "input") {
         folders <- Rhipe:::folder.handler(folders)
         folders <- rhls(folders, recurse = TRUE)$file
         remr <- c(grep(rhoptions()$file.types.remove.regex, folders))
         if (length(remr) > 0) 
            folders <- folders[-remr]
         lines$rhipe_input_folder <- paste(folders, collapse = ",")
         if (recordsAsText) 
            lines$rhipe_inputformat_class <- "org.godhuli.rhipe.RXSQTextAndTextIF" else lines$rhipe_inputformat_class <- "org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat"
         lines$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         lines$rhipe_inputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
      } else {
         ## output
         folders <- Rhipe:::folder.handler(folders)
         lines$rhipe_output_folder <- paste(folders, collapse = ",")
         if (recordsAsText) {
            lines$rhipe_string_quote <- ""
            lines$rhipe_outputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
            lines$rhipe_outputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
            lines$rhipe_outputformat_class <- "org.godhuli.rhipe.RHSequenceAsTextOutputFormat"
         } else {
            lines$rhipe_outputformat_class <- "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
            lines$rhipe_outputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
            lines$rhipe_outputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
         }
      }
      lines
   }
}

textio <- function(folders, nline = NULL, writeKey = TRUE, field.sep = " ", kv.sep = "\t", 
   eol = "\r\n", stringquote = "",encode.null=TRUE) {
   folders <- eval(folders)
   writeKey <- eval(writeKey)
   field.sep <- eval(field.sep)
   kv.sep <- eval(kv.sep)
   eol <- eval(eol)
   stringquote <- eval(stringquote)
   encode.null <-eval(encode.null)
   function(lines, direction, caller) {
      if (direction == "input") {
         folders <- Rhipe:::folder.handler(folders)
         folders <- rhls(folders, recurse = TRUE)$file
         remr <- c(grep(rhoptions()$file.types.remove.regex, folders))
         if (length(remr) > 0) 
            folders <- folders[-remr]
         lines$rhipe_input_folder <- paste(folders, collapse = ",")
         if (!is.null(nline) && is.integer(nline)) {
            lines$rhipe_inputformat_class <- "org.godhuli.rhipe.RNLineInputFormat"
            lines$mapreduce.input.lineinputformat.linespermap <- as.integer(nline)
         } else {
            lines$rhipe_inputformat_class <- "org.godhuli.rhipe.RXTextInputFormat"
         }
         lines$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHNumeric"
         lines$rhipe_inputformat_valueclass <- "org.godhuli.rhipe.RHText"
         lines$rhipe_textinputformat_encode_null <- as.character(as.logical(encode.null))
         if (is.null(lines$param.temp.file)) {
            linesToTable <- Rhipe:::linesToTable
            environment(linesToTable) <- .BaseNamespaceEnv
            lines$param.temp.file <- Rhipe:::makeParamTempFile(file = "rhipe-temp-params", 
              list(linesToTable = linesToTable))
         } else {
            linesToTable <- Rhipe:::linesToTable
            environment(linesToTable) <- .BaseNamespaceEnv
            lines$param.temp.file$envir$linesToTable <- linesToTable
         }
      } else {
         folders <- Rhipe:::folder.handler(folders)
         lines$rhipe_output_folder <- paste(folders, collapse = ",")
         lines$mapred.textoutputformat.separator <- kv.sep
         lines$mapred.field.separator <- field.sep
         lines$mapred.textoutputformat.usekey <- writeKey
         lines$rhipe.eol.sequence <- eol
         lines$rhipe_string_quote <- stringquote
         lines$rhipe_outputformat_class <- "org.godhuli.rhipe.RXTextOutputFormat"
         lines$rhipe_outputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         lines$rhipe_outputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
      }
      lines
   }
}
hbase <- function(table, colspec = NULL, rows = NULL, caching = 3L, cacheBlocks = TRUE, 
   autoReduceDetect = FALSE, valueTypes = "raw", zooinfo = NULL, classpaths = NULL, 
   jars) {
   makeRaw <- function(a) {
      if (is.null(a)) 
         return(NULL)
      a <- if (is.character(a)) 
         charToRaw(a)
      if (!is.raw(a)) 
         stop("rows must be raw")
      J("org.apache.commons.codec.binary.Base64")$encodeBase64String(.jbyte(a))
   }
   valueTypes <- eval(valueTypes)
   table <- eval(table)
   colspec <- eval(colspec)
   rows <- eval(rows)
   cacheBlocks <- eval(cacheBlocks)
   autoReduceDetect <- eval(autoReduceDetect)
   caching <- eval(caching)
   zooinfo <- eval(zooinfo)
   hbaseJars <- list.files(Sys.getenv("HBASE_HOME"), pattern = "jar$", full.names = TRUE, 
      recursive = TRUE)
   hbaseConf <- Sys.getenv("HBASE_CONF_DIR")
   if (is.null(classpaths)) 
      .jaddClassPath(c(hbaseConf, hbaseJars)) else .jaddClassPath(classpaths)
   classes <- rhoptions()$clz
   classes$config <- J("org/apache/hadoop/hbase/HBaseConfiguration")$create(classes$config)
   rhoptions(clz = classes)
   function(mapred, direction, callers) {
      if (direction == "output") {
         if (is.null(table)) 
            stop("Please provide a pre-made table")
         if (autoReduceDetect) {
            hb <- rb.init()
            tba <- rb.table.connect(hb, table)
            mapred$mapred.reduce.tasks <- min(mapred$mapred.reduce.tasks, tba$table$getRegionsInfo()$size(), 
              na.rm = TRUE)
         }
         mapred$hbase.mapred.outputtable <- as.character(table[1])
         if (!is.null(zooinfo)) {
            mapred$zookeeper.znode.parent <- zooinfo$zookeeper.znode.parent
            mapred$hbase.zookeeper.quorum <- zooinfo$hbase.zookeeper.quorum
         } else {
            mapred$hbase.zookeeper.quorum <- rhoptions()$clz$config$get("hbase.zookeeper.quorum")
            mapred$zookeeper.znode.parent <- rhoptions()$clz$config$get("zookeeper.znode.parent")
         }
         mapred$rhipe_outputformat_class <- "org.godhuli.rhipe.hbase.RHTableOutputFormat"
         mapred$rhipe_outputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         mapred$rhipe_outputformat_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
         mapred$rhipe_map_output_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         mapred$rhipe_map_output_valueclass <- "org.godhuli.rhipe.RHBytesWritable"
         mapred$jarfiles <- jars
         mapred
      } else {
         if (is.null(table)) 
            stop("Please provide table type e.g. crash_reports or telemetry")
         mapred$rhipe.hbase.tablename <- as.character(table[1])
         mapred$rhipe.hbase.colspec <- paste(colspec, collapse = ",")
         if (!is.null(rows)) {
            mapred$rhipe.hbase.rowlim.start <- makeRaw(rows[[1]])
            mapred$rhipe.hbase.rowlim.end <- makeRaw(rows[[2]])
         }
         mapred$rhipe.hbase.mozilla.cacheblocks <- sprintf("%s:%s", as.integer(caching), 
            as.integer(cacheBlocks))
         if (!is.null(zooinfo)) {
            mapred$zookeeper.znode.parent <- zooinfo$zookeeper.znode.parent
            mapred$hbase.zookeeper.quorum <- zooinfo$hbase.zookeeper.quorum
         } else {
            mapred$hbase.zookeeper.quorum <- rhoptions()$clz$config$get("hbase.zookeeper.quorum")
            mapred$zookeeper.znode.parent <- rhoptions()$clz$config$get("zookeeper.znode.parent")
         }
         mapred$rhipe.hbase.value.type <- valueTypes
         message(sprintf("Using %s table", table))
         if (!table %in% c("telemetry", "crash_reports")) {
            mapred$rhipe_inputformat_class <- "org.godhuli.rhipe.hbase.RHHBaseGeneral"
         } else {
            if (table == "crash_reports") {
              mapred$rhipe.hbase.dateformat <- "yyMMdd"
            } else if (table == "telemetry") {
              mapred$rhipe.hbase.dateformat <- "yyyyMMdd"
            }
            mapred$rhipe.hbase.mozilla.prefix <- if (table == "telemetry") 
              "byteprefix" else "hexprefix"
            mapred$rhipe_inputformat_class <- "org.godhuli.rhipe.hbase.RHCrashReportTableInputFormat"
         }
         ## mapred$hbase.client.scanner.caching <- 100L
         mapred$rhipe_inputformat_keyclass <- "org.godhuli.rhipe.RHBytesWritable"
         mapred$rhipe_inputformat_valueclass <- "org.godhuli.rhipe.hbase.RHResult"
         mapred$jarfiles <- jars
         mapred
      }
   }
}

handleIOFormats <- function(opts) {
   opts$ioformats <- list(text = textio, seq = sequenceio, sequence = sequenceio, 
      map = mapio, N = lapplyio, null = nullo, hbase = hbase)
   opts
} 
