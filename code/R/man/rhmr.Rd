\name{rhmr}
\alias{rhmr}
\title{Prepares a MapReduce Job For Execution}
\usage{
  rhmr(map = NULL, reduce = NULL, combiner = FALSE,
    setup = NULL, cleanup = NULL, ofolder = "",
    ifolder = "", inout = c("sequence", "sequence"),
    orderby = "bytes", mapred = NULL, shared = c(),
    jarfiles = c(), zips = c(), partitioner = NULL,
    copyFiles = F, N = NA, jobname = "", paramaters = NULL)
}
\arguments{
  \item{map}{\code{map} is an R expression (created using
  the R command \code{expression}) that is evaluated by
  RHIPE during the map stage. For each task, RHIPE will
  call this expression multiple times (see details).}

  \item{reduce}{\code{reduce} is an R expression (created
  using the R command \code{expression}) that is evaluated
  by RHIPE during the reduce stage, or it is a vector of
  expressions with names pre, reduce, and post.  For
  example \code{reduce = expression(pre={...},
  reduce={...}, post={...})}.  \code{reduce} is optional,
  and if not specified the map output keys will be sorted
  and shuffled and saved to disk. If it is not specified,
  then a default identity reduce is performed. Setting it
  to 0 or another integer is equivalent to
  mapred.reduce.tasks=reduce}

  \item{combiner}{If set to TRUE, RHIPE will run the
  \code{reduce} expression on the output of the \code{map}
  expression locally i.e. on the same computer that is
  running the associated map after \emph{io.sort.mb}
  megabytes of key,value pairs.

  See details.

  WARNING: setup/cleanup may not run when you think when
  used with a combiner.  We recommend only advanced users
  try to use both a combiner and setup/cleanup
  expressions.}

  \item{setup}{An expression of R code to be run before map
  and reduce.  Alternatively an expression with elements
  map and reduce e.g \code{setup=expression(map=,reduce=)}
  and each of those is, ran respectively, before the map
  and reduce phases. See details.}

  \item{cleanup}{As in setup except cleanup runs after the
  map and reduce phases.}

  \item{ofolder}{The destination of the output. If the
  destination already exists, it will be overwritten. This
  is not needed if there is not output.}

  \item{ifolder}{This is a path to a folder on the HDFS
  containing the input data. This folder may contain sub
  folders in which case RHIPE use the all the files in the
  subfolders as input. This argument is optional: if not
  provided, the user must provide a value for \code{N} and
  set the first value of \code{inout} to \code{lapply}.}

  \item{inout}{A character vector of one or two components
  which specify the formats of the input and output
  destinations. If \code{inout} is of length one this
  specifies the input format, the output being NULL
  (nothing is written) Vector element values must be from
  c("sequence", "text", "map", "lapply").  \code{inout[1]}
  can be a function, which should modify the first argument
  it gets.  See details. Also, see argument \code{N} for
  information about the "lapply" value.}

  \item{orderby}{This is one of \emph{bytes},
  \emph{integer} , \emph{numeric} and \emph{character}. The
  intermediate keys will be ordered assuming the output key
  in the map is of that type. If not of the type an
  exception will be thrown. Tuples can be sorted too, see
  \emph{Tuple Sorting} in the online documentation pdf.}

  \item{mapred}{Specify Hadoop and RHIPE options in this
  parameter (a list).  See details and for Hadoop options
  go
  \href{http://hadoop.apache.org/common/docs/current/mapred-default.html}{here}.}

  \item{shared}{This is a character vector of files located
  on the HDFS. At the beginning of the MapReduce job, these
  files will be copied to the local hard disks of the
  Tasktrackers (cluster computers on which the compute
  nodes/cores are located). See details.}

  \item{jarfiles}{Optional JARs that need to be used during
  Hadoop MapReduce.  This is used in the case when a user
  provides a custom InputFormat.  Specify the JAR file to
  handle this InputFormat using this argument and specify
  the name of the InputFormat in the \code{mapred}
  argument.}

  \item{zips}{Distributed cache file on the HDFS to unzip
  and distribute to each MapReduce task.  See
  \href{http://hadoop.apache.org/common/docs/r0.20.1/mapred_tutorial.html\#DistributedCache}{Distributed
  Cache}.}

  \item{partitioner}{A list of two names elements:
  \code{lims} and \code{type}.  See details.}

  \item{paramater}{A named list (or an alist) with
  paramaters to be passed to a mapreduce job.}

  \item{copyFiles}{Will the files created in the R code
  e.g. PDF output, be copied to the destination folder,
  \code{ofolder}?}

  \item{N}{To apply a computation to the numbers 1, 2,
  \ldots{}, \emph{N},specify the value of \emph{N} in this
  parameter. To change the number of map tasks, set N to a
  pair, where first element is N and second element is
  mapred.map.tasks (hence each task will run approximately
  floor(\emph{N}/\code{mapred.map.tasks}) computations
  sequentially). Note that \code{rhmr} automatically sets
  \code{inout[1]} to 'lapply' is \emph{N} is not \emph{NA}}

  \item{jobname}{The name of the job, which is visible on
  the Jobtracker website. If not provided, Hadoop MapReduce
  uses the default name \emph{job_date_time_number} e.g.
  \code{job_201007281701_0274}.}

  \item{parameters}{A list argument.  Each element of the
  list must have a name.  Each element of the list will be
  placed in the global environment in MapReduce.  For
  example \code{parameters = list(arg1 = 1, arg2 = 2)} will
  place in the global environment for maps and reduces arg1
  and arg2 with integer values 1 and 2 respectively.}
}
\value{
  Returns an object of class \code{rhmr} suitable for
  beginning executing a Hadoop job with \code{rhex}.
}
\description{
  Creates the R object that contains all the information
  required by RHIPE to run a MapReduce job via a call to
  \code{\link{rhex}} (see details).
}
\details{
  \itemize{ \item{Buffer Size:}{ If a task consists of
  \emph{W} key,value pairs, the expression \code{map} will
  be called ceil(\emph{W} / \emph{rhipe_map_buffsize})
  times. The default value of \emph{rhipe_map_buffsize} is
  10,000 and is user configurable. Each time \code{map} is
  called, the vectors \code{map.keys} and \code{map.values}
  contain \emph{rhipe_map_buffsize} keys and values
  respectively. If the objects are large it advisable to
  reduce the size of \emph{rhipe_map_buffsize}, so that the
  total amount of memory used by a task is well controlled.
  For particularly large map.values, the authors have used
  rhipe_map_buffsize as low as 10.} \item{Setup:}{ In
  RHIPE, each task is a sequence of many thousands of key,
  value pairs. Before running the \code{map} and
  \code{reduce} expression (and before any key, value pairs
  have been read), RHIPE will evaluate expressions in
  \code{setup} and \code{cleanup}. Each of these may
  contain the names \code{map} and \code{reduce} e.g
  \code{setup=list(map=,reduce=)} specific to the
  \code{map} and \code{reduce} expressions. If just an
  expressions is provided, it will be evaluated before both
  the Map phase and Reduce phase. The same is true for
  \code{cleanup}. Variables created, packages loaded in the
  \code{setup} expression will be visible in the \code{map}
  and the \code{reduce} expression but not both since both
  are evaluated in different R sessions (except when using
  a combiner).} \item{Sorting and Shuffling:}{ To turn off
  sorting and shuffling and instead write the map output to
  disk directly, set \code{mapred.reduce.tasks} to zero in
  \code{mapred}. In this case, the output keys are not
  sorted and the output format should not be \emph{map}
  (since a map file expects sorted keys).} \item{Using a
  Combiner:}{ If \code{combiner} is TRUE, the \code{reduce}
  expression will be invoked during the local combine, in
  which case the output is intermediate and not saved as
  final output. The \code{reduce} expression also be
  invoked during the final reduce phase, in which case it
  will receive all the values associated with the key
  (note, these are values outputted when \code{reduce} is
  invoked as a combiner) and the output will be committed
  to the destination folder. To determine in which state
  \code{reduce} is running read the environment variable
  \code{rhipe_iscombining} which is `1' (also the R symbol
  \code{rhipe_iscombining} is equal TRUE) or `0' for the
  former and latter states respectively. WARNING: setup and
  cleanup may not run when you think when used with a
  combiner.  We recommend only advanced users try to use
  both a combiner and setup/cleanup expressions.}
  \item{Using Shared Files:}{ \code{shared} is a character
  vector of files located on the HDFS. At the beginning of
  the MapReduce job, these files will be copied to the
  local hard disks of the Tasktrackers (cluster computers
  on which the compute nodes/cores are located). User
  provided R code can read theses files from the current
  directory (which is located on the local hard disk). For
  example, if \emph{/path/to/file.Rdata} is located on the
  HDFS and shared, it is possible to read it in the R
  expressions as \code{load('file.Rdata')}. Note, there is
  no need for the full path, the file is copied to the
  current directory of the R process.} \item{inout:}{File
  Types} \itemize{ \item{"sequence"}{ The keys and values
  can be arbitrary R objects. All the information of the
  object will be preserved. To extract a single key,value
  pair from a sequence file, either the user has to read
  the entire file or compose a MapReduce job to subset that
  key,value pair.} \item{"text"}{ The keys, and values are
  stored as lines of text. If the input is of text format,
  the keys will be byte offsets from beginning of the file
  and the value is a line of text without the trailing
  newline. R objects written to a text output format are
  written as one line. Characters are quoted and vectors
  are separated by \code{mapred.field.separator} (default
  is space). The character used to separate the key from
  the value is specified in the \code{mapred} argument by
  setting \code{mapred.textoutputformat.separator} (default
  is tab). To not output the key, set
  \code{mapred.textoutputformat.usekey} to FALSE.}
  \item{"map"}{ A map file is actually a folder consisting
  of sequence file and an index file. A small percentage of
  the keys in the sequence file are stored in the index
  file. Using the index file, Hadoop can very quickly
  return a value corresponding to a key (using
  \code{rhgetkey}). To create such an output format, use
  \emph{map}. Note, the keys have to be saved in sorted
  order. The keys are sent to the \code{reduce} expression
  in sorted order, hence if the user does not modify
  \code{reduce.key} a query-able map file will be created.
  If \code{reduce.key} is modified, the sorted guarantee
  does not hold and RHIPE will either throw an error or
  querying the output for a key might return with empty
  results. MapFiles cannot be created if \code{orderby} is
  not \emph{bytes}.} } \item{Custom Partitioning:}{ A list
  of two names elements: \code{lims} and \code{type}. A
  partitioner forces all keys sharing the same property to
  be processed by one reducer. Thus, for these keys, the
  output of the reduce phase will be saved in one file. For
  example, if the keys were IP addresses e.g.
  \code{c(A,B,C,D)} where the components are integers, with
  the default partitioner, the space of keys will be
  uniformly distributed across the number of reduce tasks.
  If it is desired to store all IP addresses with the same
  first three ordinates in one file (and processed by one R
  process), use a partitioner as \code{list(lims=c(1:3),
  type='integer')}. RHIPE implements partitioners when the
  key is an atomic vector of the following type: integer,
  string, and real. The value of \code{lims} specifies the
  ordinates (beginning and end) of the key to partition on.
  The numbers must be positive. \code{lims} can be a single
  number.} \item {Avoid Time Outs:}{ To avoid time outs
  during long map or reduce expressions, your MapReduce
  expressions should report status messages via calls to
  rhstatus. In the absence of \code{rhstatus} and if
  \code{mapred.task.timeout} is non zero (by default it is
  10 minutes) Hadoop will eventually kill a lengthy R
  process.} \item{List of Important Options for the mapred
  argument:}{ These are all set with mapred = list(
  name=value, name=value,...).} \itemize{
  \item{rhipe_map_buffsize:}{ Number of elements in the map
  buffer. (not size in bytes!)  Control the amount of
  memory your map task employ using this.}
  \item{rhipe_reduce_buffsize:}{ Number of elements in the
  reduce.values buffer. (not size in bytes!) Control the
  amount of memory your reduce task employ using this.}
  \item{rhipe_stream_buffer:}{.}
  \item{mapred.task.timeout}{ If non-zero the number of
  milliseconds before a task times out.}
  \item{mapred.reduce.tasks}{ If zero then no reducer is
  run and map output is placed directly on disk without
  shuffling or sorting.  If non-zero, the number of
  simultaneous reduce task to launch.}
  \item{mapred.map.tasks}{ The number of simultaneous map
  task to launch.} } }
}
\examples{
\dontrun{
   #RUNNABLE BUT ARTIFICIAL EXAMPLE
   #We will create a data set with three columns:
   #the level of a categorical variable A, a time variable B and a value C.
   #For each level of A, we want the sum of differences of C ordered by B within A.
   #Creating the Data set The column A is the key, but this is not important.
   #There are 5000 levels of A, each level has 10,000 observations.
   #By design the values of B are randomly written (sample),
   #also for simplicity C is equal to B, though this need not be.

library(Rhipe)
rhinit()

#might need a call here to rhoptions for runner option depending on user

map <- expression({
   N <- 10000
   for( first.col in map.values ){
      w <- sample(N,N,replace=FALSE)
      for( i in w)
         rhcollect(first.col,c(i,i))

   }
})
mapred <- list(mapred.reduce.tasks=0)
z=rhmr(map=map, N=5000, inout=c("lapply","sequence"),ofolder="/tmp/sort",mapred=mapred)
ex = rhex(z, async=TRUE)
rhstatus(ex)    #Wait for job to finish; Ctrl+C to quit



#Sum of Differences The key is the value of A and B, the value is C.

map <- expression({
   for(r in seq_along(map.values)){
      f <- map.values[[r]]
      rhcollect(as.integer(c(map.keys[[r]],f[1])),f[2])
   }
})


reduce.setup <- expression({
   newp <- -Inf
   diffsum <- NULL
})
reduce <- expression(
   pre={
      if(reduce.key[[1]][1] != newp) {
         if(newp>-Inf)
            rhcollect(newp, diffsum) #prevents -Inf from being collected
         diffsum <- 0
         lastval <- 0
         newp <- reduce.key[[1]][1]
         skip <- TRUE
      }
    },
    reduce={
      current <- unlist(reduce.values) #only one value!
      if(!skip)
         diffsum <- diffsum + (current-lastval)
      else
         skip <- FALSE
      lastval <- current
   }
)
reduce.cleanup <- expression({
   if(newp>-Inf) rhcollect(newp,diffsum) #for the last key
})

#To turn on the partitioning and ordering of keys,
z <- rhmr(map=map,reduce=reduce, inout=c("sequence","sequence"),
      ifolder="/tmp/sort",ofolder="/tmp/sort2", part=list(lims=1,type="integer"),
      orderby="integer",cleanup=list(reduce=reduce.cleanup),
      setup=list(reduce=reduce.setup))
ex = rhex(z, async=TRUE)
rhstatus(ex)  #Wait for job to finish; ctrl + C to quit
}
}
\author{
  Saptarshi Guha
}
\seealso{
  \code{\link{rhex}}, \code{\link{rhstatus}},
  \code{\link{rhkill}}
}
\keyword{Hadoop}
\keyword{MapReduce}

