#' Prepares,Submits and Monitors  MapReduce Jobs
#' 
#' Creates the R object that contains all the information required by RHIPE to
#' run a MapReduce job via a call to \code{\link{rhex}} (see details).
#' 
#' @param map \code{map} is an R expression (created using the R command
#'   \code{expression}) that is evaluated by RHIPE during the map stage. For
#'   each task, RHIPE will call this expression multiple times (see details).
#' This can also be a function of two arguments,the key and the value. Using an expression provides one more opportunity for vector operations.
#' @param reduce \code{reduce} is an R expression (created using the R command
#'   \code{expression}) that is evaluated by RHIPE during the reduce stage, or
#'   it is a vector of expressions with names pre, reduce, and post.  For
#'   example \code{reduce = expression(pre={...}, reduce={...}, post={...})}.
#'   \code{reduce} is optional, and if not specified the map output keys will
#'   be sorted and shuffled and saved to disk. If it is not specified, then a default identity reduce is performed. Setting  it to 0 or another integer is equivalent to mapred.reduce.tasks=reduce
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
#' @param input Specifies the type of input. If a character vector then Sequence file input.
#' If a numeric(N), the lapply input e.g the key will be from 1 to N. If a pair of numbers, then the key ranges from 1 ..N[1]
#' and mapred.map.tasks is set to N[2] (thus each split processes approximately N[1]/N[2] key values). See \code{rhfmt} for more examples and \code{rhoptions()$ioformats}. To get text input, specify \code{rhfmt(path, 'text')}.
#' @param output Similar to \code{input}.  To get a map output format (after
#' after which the user can call \code{rhmapfile} to query using \code{rhgetkey}), specify \code{rhfmt(path, 'map')}.
#' @param setup An expression of R code to be run before map and reduce.
#'   Alternatively an expression with elements map and reduce e.g
#'   \code{setup=expression(map=,reduce=)} and each of those is, ran respectively,
#'   before the map and reduce phases. See details.
#' @param cleanup As in setup except cleanup runs after the map and reduce
#'   phases.
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
#' @param parameters A named list  with parameters to be passed to a mapreduce job.It can also be the string 'all', and all variables (whose size <= rhoptions()$copyObjects$maxsize (bytes) and the name of the variable not in rhoptions()$copyObjects$exclude) will be automatically copied. If rhoptions()$copyObjects$auto is TRUE (default)', RHIPE will make an attempt (via codetools) to determine the called variables/functions and copy them automatically. 
#' @param copyFiles Will the files created in the R code e.g. PDF output, be
#'   copied to the destination folder, \code{ofolder}?
#' @param jobname The name of the job, which is visible on the Jobtracker
#'   website. If not provided, Hadoop MapReduce uses the default name
#'   \emph{job_date_time_number} e.g. \code{job_201007281701_0274}.
#' @param job The parameter \code{job} can either be a string with the format
#'   \emph{job_datetime_id} (e.g. \emph{job_201007281701_0274})
#' @param mon.sec If \code{mon.sec} is greater than 0, a small data frame
#'   indicating the progress will be returned every \code{mon.sec} seconds.
#' @param readback if FALSE, results will not be read back and insteat results from rhstatus is returned
#' @param noeval If TRUE do not run, just return the job object
#' @param debug can be one of 'count' which counts the number of errors but does not stop the job, 'stop' which
#' stops the job (setting debug to NULL (default) is the same and is much faster)
#' and 'collect' which collects 20 errors per split, saves them in
#' files (inside the output folder) prefixed with rhipe.debug and does not kill the job.
#' The maximum number of errors to collect can be set in the \code{param} argument to rhwatch, i.e rh.max.errors=10 collects a maximum of 10 errors
#' @param ... Extra parameters passed to \code{rhstatus}.
#' @return If the state is SUCCEEDED and total output size (in MB) is less than \code{rhoptions()$max.read.in.size} the data is read with a warning if the number of records is more than \code{rhoptions()$reduce.output.records.warn}. If \code{rhoptions()$rhmr.max.records.to.read.in} is not NA, that many records is read. This only works for Sequence output. Otherwise an object of length two. The first element is the data returned from rhstatus and the second is the data returned by the internal \code{rhmr} function.
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
#' \item{'sequence'}{
#' The keys and values can be arbitrary R objects. All the information of the
#' object will be preserved. To extract a single key,value pair from a sequence
#' file, either the user has to read the entire file or compose a MapReduce job
#' to subset that key,value pair.}
#' \item{'text'}{
#' The keys, and values are stored as lines of text. If the input is of text
#' format, the keys will be byte offsets from beginning of the file and the
#' value is a line of text without the trailing newline. R objects written to a
#' text output format are written as one line. Characters are quoted and
#' vectors are separated by \code{mapred.field.separator} (default is space).
#' The character used to separate the key from the value is specified in the
#' \code{mapred} argument by setting \code{mapred.textoutputformat.separator}
#' (default is tab). To not output the key, set
#' \code{mapred.textoutputformat.usekey} to FALSE.}
#' \item{'map'}{
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
#' # RUNNABLE BUT ARTIFICIAL EXAMPLE
#' # We will create a data set with three columns: 
#' # the level of a categorical variable A, a time variable B and a value C. 
#' # For each level of A, we want the sum of differences of C ordered by B within A.
#' # Creating the Data set The column A is the key, but this is not important. 
#' # There are 5000 levels of A, each level has 10,000 observations. 
#' # By design the values of B are randomly written (sample), 
#' # also for simplicity C is equal to B, though this need not be.
#' 
#' library(Rhipe)
#' rhinit()
#' 
#' # might need a call here to rhoptions for runner option depending on user
#' 
#' map <- expression({
#'    N <- 10000
#'    for( first.col in map.values ){
#'       w <- sample(N,N,replace=FALSE)
#'       for( i in w){
#'          rhcollect(first.col,c(i,i))
#'       }
#'    }
#' })
#' mapred <- list(
#'  rhipe_map_buffsize=3000,
#'  mapred.reduce.tasks = 1
#' )
#' z <- rhwatch(
#'     map      = map, 
#'     reduce   = NULL, 
#'     input    = 5000, 
#'     output   = rhfmt("/tmp/sort", type = "sequence"), 
#'     mapred   = mapred, 
#'     readback = FALSE
#' )
#' 
#' #Sum of Differences The key is the value of A and B, the value is C.
#' 
#' map <- expression({
#'    for(r in seq_along(map.values)){
#'       f <- map.values[[r]]
#'       rhcollect(as.integer(c(map.keys[[r]], f[1])), f[2])
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
#'    if(newp > -Inf){
#'      rhcollect(newp, diffsum)
#'    } #for the last key
#' })
#' 
#' #To turn on the partitioning and ordering of keys,
#' z <- rhwatch(
#'     map         = map,
#'     reduce      = reduce, 
#'     input       = rhfmt('/tmp/sort', type = "sequence"),
#'     output      = rhfmt('/tmp/sort2', type = "sequence"),
#'     partitioner = list(lims = 1, type = 'integer'),
#'     orderby     = 'integer',
#'     cleanup     = list(reduce = reduce.cleanup),
#'     setup       = list(reduce = reduce.setup),
#'     readback    = FALSE
#' )
#' }
#' @export
rhwatch <- function(map = NULL, reduce = NULL, combiner = FALSE, setup = NULL, 
   cleanup = NULL, input = NULL, output = NULL, orderby = "bytes", 
   mapred = NULL, shared = c(), jarfiles = c(), zips = c(), partitioner = NULL, 
   copyFiles = FALSE, jobname = "", parameters = NULL, job = NULL, mon.sec = 5, 
   readback = rhoptions()$readback, debug = NULL, noeval = FALSE, ...) {

   ## ########################################################### 
   ## handle "..."
   ## ########################################################### 

   envir <- sys.frame(-1)
   if (is.null(job))
      job <- Rhipe:::.rhmr(map = map, reduce = reduce, combiner = combiner, setup = setup, 
         cleanup = cleanup, input = input, output = output, orderby = orderby, 
         mapred = mapred, shared = shared, jarfiles = jarfiles, zips = zips, partitioner = partitioner, 
         copyFiles = copyFiles, jobname = jobname, parameters = parameters, envir = envir) 
   else if (is.character(job)) 
      return(Rhipe:::rhwatch.runner(job = job, mon.sec = mon.sec, readback = readback, ...))
   
   if (!is.null(job$lines$mapred.job.tracker) && job$lines$mapred.job.tracker == TRUE) {
      z <- Rhipe:::rhwatch.runner(job = job, mon.sec = mon.sec, readback = readback, ...)
      if (readback == FALSE) {
         class(z) <- append(class(z), "rhwatch")
      }
      return(z)
   }
   
   if (!is.null(debug)) {
      m <- unserialize(charToRaw(job[[1]]$rhipe_map))
      if (!(is(m, "rhmr-map") || is(m, "rhmr-map2"))) 
         stop("RHIPE: for debugging purposes, must use a map expression returned by ewrap")
      
      ## Replace the map expression
      if (is(m, "rhmr-map")) {
         j <- m[[1]][[3]]  ##the mapply
         jj <- j[[3]][[2]]  ## the function passed to mapply
         l <- list()
         l$replace <- jj[[3]][[2]]  ## body of jj when rhmap is fixed it's body(jj)
         l$before <- m[[1]][[2]]
         l$after <- m[[1]][[4]]
         FIX <- function(x) if (is.null(x)) 
            NULL else x
         newm <- as.expression(bquote({
            .(BEFORE)
            result <- mapply(function(.index, k, r) {
              tryCatch(.(REPLACE), error = function(e) {
               rhipe.trap(e, k, r)
               NULL
              })
            }, seq_along(map.values), map.keys, map.values, SIMPLIFY = FALSE)
            .(AFTER)
             }, list(BEFORE = FIX(l$before), AFTER = FIX(l$after), REPLACE = FIX(l$replace))))
         
         environment(newm) <- .BaseNamespaceEnv
         job[[1]]$rhipe_map <- rawToChar(serialize(newm, NULL, ascii = TRUE))
      } else if (is(m, "rhmr-map2")) {
         jj <- m[[1]][[2]][[3]][[2]]  ## the function passed to mapply
         newm <- expression({
            result <- mapply(function(k, r) {
              tryCatch(rhipe_inner_runner(k, r), error = function(e) {
               rhipe.trap(e, k, r)
               NULL
              })
            }, map.keys, map.values, SIMPLIFY = FALSE)
         })
         
         environment(newm) <- .BaseNamespaceEnv
         job[[1]]$rhipe_map <- rawToChar(serialize(newm, NULL, ascii = TRUE))
      }
      ## Has the user given one?
      if (is.list(debug) && !is.null(debug$map)) {
         if (!is.null(debug$map$setup)) 
            setup <- debug$map$setup
         if (!is.null(debug$map$cleanup)) 
            cleanup <- debug$map$cleanup
         if (!is.null(debug$map$handler)) 
            handler <- debug$map$handler
      } else if (is.character(debug)) {
         handler <- rhoptions()$debug$map[[debug]]$handler
         setup <- rhoptions()$debug$map[[debug]]$setup
         cleanup <- rhoptions()$debug$map[[debug]]$cleanup
         if (is.null(handler)) 
            stop("Rhipe(rhwatch): invalid debug character string provided")
      }
      if (is.null(job$parameters)) {
         environment(handler) <- .BaseNamespaceEnv
         job$parameters <- Rhipe:::makeParamTempFile(file = "rhipe-temp-params", 
            parameters = list(rhipe.trap = handler))
         
         ## need the code to load temporary files!
         x <- unserialize(charToRaw(job[[1]]$rhipe_setup_map))
         y <- job$parameters$setup
         environment(y) <- .BaseNamespacEenv
         job[[1]]$rhipe_setup_map <- rawToChar(serialize(c(y, x), NULL, ascii = TRUE))
         
         x <- unserialize(charToRaw(job[[1]]$rhipe_setup_reduce))
         job[[1]]$rhipe_setup_reduce <- rawToChar(serialize(c(y, x), NULL, ascii = TRUE))
         ## this is becoming quite the HACK of all lines magic and this hit should be in rhex ...
         job[[1]]$rhipe_shared <- sprintf("%s,%s#%s", job[[1]]$rhipe_shared, job$parameters$file, 
            basename(job$parameters$file))
      } else {
         environment(handler) <- .BaseNamespaceEnv
         job$parameters$envir$rhipe.trap <- handler
      }
      if (is.expression(setup)) {
         environment(setup) <- .BaseNamespaceEnv
         x <- unserialize(charToRaw(job[[1]]$rhipe_setup_map))
         job[[1]]$rhipe_setup_map <- rawToChar(serialize(c(x, setup), NULL, ascii = TRUE))
      }
      if (is.expression(cleanup)) {
         environment(cleanup) <- .BaseNamespaceEnv
         cleanupmap <- unserialize(charToRaw(job[[1]]$rhipe_cleanup_map))
         job[[1]]$rhipe_cleanup_map <- rawToChar(serialize(c(cleanupmap, cleanup), 
            NULL, ascii = TRUE))
      }
      environment(handler) <- .BaseNamespaceEnv
      job[[1]]$rhipe_copy_file <- "TRUE"  ##logic for local runner is wrong here
      job[[1]]$rhipe_copy_excludes <- rhoptions()$rhipe_copy_excludes
      job[[1]]$rhipe_copyfile_folder <- rhoptions()$rhipe_copyfile_folder
   }
   if (noeval) 
      return(job)
   z <- Rhipe:::rhwatch.runner(job = job, mon.sec = mon.sec, readback = readback, 
      ...)
   if (readback == FALSE) {
      class(z) <- append(class(z), "rhwatch")
   }
   z
}

rhwatch.runner <- function(job, mon.sec = 5, readback = TRUE, debug = NULL, ...) {
   if (class(job) == "rhmr") {
      results <- if (is(job, "rhmr")) 
         rhstatus(rhex(job, async = TRUE), mon.sec = mon.sec, ...) else rhstatus(job, mon.sec = mon.sec, ...)
      ofolder <- job$lines$rhipe_output_folder
      
      # if rhoption write.job.info is TRUE, then write it to _rh_meta
      if (results$state == "SUCCEEDED" && rhoptions()$write.job.info) {
         # get job id
	 id = parseJobIDFromTracking(results)
	          
         jobData <- list(results = results, jobConf = job, jobInfo = rhJobInfo(id))
         rhsave(jobData, file = paste(ofolder, "/_rh_meta/jobData.Rdata", sep = ""))
      }
      
      if (readback == TRUE && results$state == "SUCCEEDED" && sum(rhls(ofolder)$size)/(1024^2) < 
         rhoptions()$max.read.in.size) {
         W <- "Reduce output records"
         if (!is.null(job$lines$mapred.reduce.tasks) && as.numeric(job$lines$mapred.reduce.tasks) == 
            0) 
            W <- "Map output records"
         num.records <- as.numeric(results$counters$"Map-Reduce Framework"[W, 
            ])
         if (num.records > rhoptions()$reduce.output.records.warn) 
            warning(sprintf("Number of output records is %s which is greater than rhoptions()$reduce.output.records.warn\n. Consider running a mapreduce to make this smaller, since reading so many key-value pairs is slow in R", 
              num.records))
         oclass <- job$lines$rhipe_outputformat_class
         textual <- FALSE
         type <- "sequence"
         # should have logic to make type="map" if it really is
         if (grepl("RHSequenceAsTextOutputFormat", oclass)) {
            textual <- TRUE
         }
         if (grepl("RXTextOutputFormat", oclass)) {
            type <- "text"
         }
         
         if (!is.na(rhoptions()$rhmr.max.records.to.read.in)) 
            return(rhread(ofolder, max = rhoptions()$rhmr.max.records.to.read.in, 
              type = type, textual = textual)) else return(rhread(ofolder, type = type, textual = textual))
      }
      if (grepl("(FAILED|KILLED)", results$state)) {
         if (is.null(debug) || (!is.null(debug) && debug != "collect")) {
            warning(sprintf("Job failure, deleting output: %s:", ofolder))
            rhdel(ofolder)
         } else warning("debug is 'collect', so not deleting output folder")
      }
      return(list(results, job))
   } else 
      ## Ideally even with a job.id i can still get the all the job info by looking
      ## somewhere in the output folder.  job is now a job_identifier string
      rhstatus(job, mon.sec = mon.sec, ...)
} 
