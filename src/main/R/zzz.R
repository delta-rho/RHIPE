.rhipeEnv <- new.env()
vvvv <- tryCatch(installed.packages()["Rhipe","Version"], error=function(e) "unknown")
#attr(vvvv, "minor") <- "0"
attr(vvvv, "date") <- tryCatch(installed.packages(fields="Date")["Rhipe","Date"], error=function(e) "unknown")

class(vvvv) <- "rhversion"

assign("rhipeOptions", list(version = vvvv), envir = .rhipeEnv)

.onLoad <- function(libname, pkgname) {
   library.dynam("Rhipe", pkgname, libname,local=FALSE)
   opts <- get("rhipeOptions", envir = .rhipeEnv)
   
   ## #################################################################
   ## JAVA AND HADOOP
   ## #################################################################

   opts$jarloc <- list.files(file.path(system.file(package = "Rhipe"), "java"), 
      pattern = "Rhipe.jar$", full.names = TRUE)
   
   opts$mycp <- list.files(file.path(system.file(package = "Rhipe"), "java"), pattern = "jar$", 
      full.names = TRUE)
   
   # need to exclude all Rhipe jars as they are already taken care of
   mycp_exclude <- list.files(file.path(system.file(package = "Rhipe"), "java"), 
      pattern = "Rhipe", full.names = TRUE)
   opts$mycp <- setdiff(opts$mycp, mycp_exclude)
   
   if (Sys.getenv("HADOOP") == "" && Sys.getenv("HADOOP_HOME") == "" && Sys.getenv("HADOOP_BIN") == 
      "") 
      packageStartupMessage("Rhipe requires HADOOP_HOME or HADOOP or HADOOP_BIN environment variable to be present\n $HADOOP/bin/hadoop or $HADOOP_BIN/hadoop should exist")
   if (Sys.getenv("HADOOP_BIN") == "") {
      packageStartupMessage("Rhipe: HADOOP_BIN is missing, using $HADOOP/bin")
      Sys.setenv(HADOOP_BIN = sprintf("%s/bin", Sys.getenv("HADOOP")))
   }
   
   if (Sys.getenv("HADOOP_HOME") == "") 
      packageStartupMessage("HADOOP_HOME missing")
   if (Sys.getenv("HADOOP_CONF_DIR") == "") 
      packageStartupMessage("HADOOP_CONF_DIR missing, you are probably going to have a problem running RHIPE.\nHADOOP_CONF_DIR should be the location of the directory that contains the configuration files")
      
   ## #################################################################
   ## RhipeMapReduce, runner, and checks
   ## #################################################################

   opts$RhipeMapReduce <- list.files(paste(system.file(package = "Rhipe"), 
      "bin", sep = .Platform$file.sep), pattern = "^RhipeMapReduce$", 
      full.names = TRUE)
   if (is.null(opts$RhipeMapReduce) || length(opts$RhipeMapReduce) != 1) {
      packageStartupMessage("RhipeMapReduce executable not found in package bin folder as expected")
   }
   ## RhipeMapReduce is the executable, but the simpliest way to run it is via R CMD
   ## which sets up environment variables.
   runner_env <- Sys.getenv("RHIPE_RUNNER")
   if (runner_env != "") {
      opts$runner <- runner_env
   } else {
      opts$runner <- paste("R", "CMD", opts$RhipeMapReduce, "--slave", "--silent", 
         "--vanilla")  #,'--max-ppsize=100000','--max-nsize=1G')     
   }
   
   #######################################################################
   ## OTHER DEFAULTS
   #######################################################################

   opts$job.status.overprint <- FALSE
   opts$write.job.info <- FALSE
   opts$file.types.remove.regex <- "(/_meta|/_rh_meta|/_outputs|/_SUCCESS|/_LOG|/_log|rhipe_debug|rhipe_merged_index_db)"
   opts$max.read.in.size <- 200 * 1024 * 1024  ## 100MB
   opts$reduce.output.records.warn <- 200 * 1000
   opts$rhmr.max.records.to.read.in <- NA
   opts$rhipe_copy_excludes <- "(.*/Rtmp.*)"
   opts$rhipe_copyfile_folder <- "_outputs"
   opts$HADOOP.TMP.FOLDER <- Sys.getenv("RHIPE_HADOOP_TMP_FOLDER")
   if (opts$HADOOP.TMP.FOLDER == "") { opts$HADOOP.TMP.FOLDER <- "/tmp" }
   opts$readback <- TRUE
   opts$zips <- c()
   opts$hdfs.working.dir <- "/"
   ## other defaults
   opts$copyObjects <- list(auto = TRUE, maxsize = 100 * 1024 * 1024, exclude = c(".Random.seed", 
      "map.values", "map.keys", "reduce.values", "reduce.key", "rhcollect", "rng"))
   opts$templates <- list()
   opts$templates$scalarsummer <- expression(pre = {
      .sum <- 0
   }, reduce = {
      .sum <- .sum + sum(unlist(reduce.values), na.rm = TRUE)
   }, post = {
      {
         rhcollect(reduce.key, .sum)
      }
   })
   opts$templates$scalarsummer <- structure(opts$templates$scalarsummer, combine = TRUE)
   opts$templates$colsummer <- expression(pre = {
      .sum <- 0
   }, reduce = {
      .sum <- .sum + apply(do.call("rbind", reduce.values), 2, sum)
   }, post = {
      {
         rhcollect(reduce.key, .sum)
      }
   })
   opts$templates$colsummer <- structure(opts$templates$colsummer, combine = TRUE)
   
   opts$templates$rbinder <- function(r = NULL, combine = FALSE, dfname = "adata") {
      ..r <- substitute(r)
      r <- if (is(..r, "name")) 
         get(as.character(..r)) else ..r
      def <- if (is.null(r)) 
         TRUE else FALSE
      r <- if (is.null(r)) 
         substitute({
            rhcollect(reduce.key, adata)
         }) else r
      y <- bquote(expression(pre = {
         adata <- list()
      }, reduce = {
         adata[[length(adata) + 1]] <- reduce.values
      }, post = {
         adata <- do.call("rbind", unlist(adata, recursive = FALSE))
         .(P)
      }), list(P = r))
      y <- if (combine || def) 
         structure(y, combine = TRUE) else y
      environment(y) <- .BaseNamespaceEnv
      y
   }
   opts$templates$raggregate <- function(r = NULL, combine = FALSE, dfname = "adata") {
      ..r <- substitute(r)
      ..r <- if (is(..r, "name")) 
         get(as.character(..r)) else ..r
      def <- if (is.null(..r)) 
         TRUE else FALSE
      r <- if (is.null(..r)) 
         substitute({
            adata <- unlist(adata, recursive = FALSE)
            rhcollect(reduce.key, adata)
         }) else ..r
      y <- bquote(expression(pre = {
         adata <- list()
      }, reduce = {
         adata[[length(adata) + 1]] <- reduce.values
      }, post = {
         .(P)
      }), list(P = r))
      y <- if (combine || def) 
         structure(y, combine = TRUE) else y
      environment(y) <- .BaseNamespaceEnv  ## Using GlobalEnv screws thing sup ...
      
      y
   }
   opts$templates$identity <- expression(reduce = {
      lapply(reduce.values, function(r) rhcollect(reduce.key, r))
   })
   opts$templates$range <- expression(pre = {
      rng <- c(Inf, -Inf)
   }, reduce = {
      rx <- unlist(reduce.values)
      rng <- c(min(rng[1], rx, na.rm = TRUE), max(rng[2], rx, na.rm = TRUE))
   }, post = {
      rhcollect(reduce.key, rng)
   })
   opts$templates$range <- structure(opts$templates$range, combine = TRUE)
   opts$debug <- list()
   opts$debug$map <- list()
   opts$debug$map$collect <- list(setup = expression({
      
      rhAccumulateError <- local({
         maxm <- tryCatch(rh.max.errors, error = function(e) 20)
         x <- function(maximum.errors = maxm) {
            errors <- list()
            maximum.errors <- maximum.errors
            counter <- 0
            return(function(X, retrieve = FALSE) {
              if (retrieve) return(errors)
              if (counter < maximum.errors) {
               counter <<- counter + 1
               errors[[counter]] <<- X
              }
            })
         }
         x()
      })
   }), cleanup = expression({
      rhipe.errors <- rhAccumulateError(ret = TRUE)
      if (length(rhipe.errors) > 0) {
         dir.create("./tmp/_debug", showWarnings = FALSE)
         save(rhipe.errors, file = sprintf("./tmp/_debug/rhipe_debug_%s", Sys.getenv("mapred.task.id")))
         rhcounter("@R_DebugFile", "saved.files", 1)
      }
   }), handler = function(e, k, r) {
      rhcounter("R_UNTRAPPED_ERRORS", as.character(e), 1)
      rhAccumulateError(list(as.character(e), k, r))
   })
   opts$debug$map$count <- list(setup = NA, cleanup = NA, handler = function(e, 
      k, r) rhcounter("R_UNTRAPPED_ERRORS", as.character(e), 1))
   opts$debug$map[["stop"]] <- list(setup = NA, cleanup = NA, handler = function(e, 
      k, r) rhcounter("R_ERRORS", as.character(e), 1))
   
   ## ##################### Handle IO Formats ######################
   opts <- handleIOFormats(opts)
   
   ########################################################################
   ## FINSHING
   ########################################################################
   
   assign("rhipeOptions", opts, envir = .rhipeEnv)
   ## initialize()
   a <- "| Please call rhinit() else RHIPE will not run |"
   a <- sprintf("%s\n%s\n%s", paste(rep("-", nchar(a)), collapse = ""), a, paste(rep("-", 
      nchar(a)), collapse = ""))
   packageStartupMessage(a)
}

#' Initializes the RHIPE subsystem
#' 
#' @export 
rhinit <- function() {
   opts <- rhoptions()
   hadoop <- Sys.getenv(c("HADOOP_HOME", "HADOOP_CONF_DIR", "HADOOP_LIBS"))
   
   if (hadoop["HADOOP_HOME"] != "") {
      ## print(any(c(grepl('(yes|true)',tolower(Sys.getenv('RHIPE_USE_CDH4'))),grepl('cdh4',
      ## c(list.files(hadoop['HADOOP_HOME'])))) ,na.rm=TRUE))
#      if (any(c(grepl("(yes|true)", tolower(Sys.getenv("RHIPE_USE_CDH4"))), grepl("cdh4",
#         c(list.files(hadoop["HADOOP_HOME"])))), na.rm = TRUE)) {
#         packageStartupMessage("Rhipe: Using RhipeCDH4.jar")
#         opts$jarloc <- list.files(file.path(system.file(package = "Rhipe"), "java"),
#            pattern = "RhipeCDH4.jar$", full.names = TRUE)
#      } else {
         packageStartupMessage("Rhipe: Using Rhipe.jar file")
#      }
   }
   library(rJava)
   c1 <- list.files(hadoop["HADOOP_HOME"], pattern = "jar$", full.names = TRUE, recursive = FALSE)
   c15 <- tryCatch(unlist(sapply(strsplit(hadoop["HADOOP_LIBS"], ":")[[1]], function(r) {
      list.files(r, pattern = "jar$", full.names = TRUE, recursive = FALSE)
   })), error = function(e) NULL)
   
   c2 <- hadoop["HADOOP_CONF_DIR"]
   .jinit(parameters = c(getOption("java.parameters"), "-Xrs"))
   ## mycp needs to come first as hadoop distros such as cdh4 have an older version
   ## jar for guava
   .jaddClassPath(c(opts$jarloc, opts$mycp, c2, c15, c1))
# work in progress - dynamic cp construction from hadoop
#hcp <- system("$HADOOP_HOME/bin/hadoop classpath | tr -d '*' | tr ':' '\n'",intern = TRUE)
#hcpFiles <- list.files(hcp,full=TRUE)
#.jaddClassPath(c(opts$jarloc, opts$mycp,hcpFiles))
   packageStartupMessage(sprintf("Initializing Rhipe v%s", vvvv))
   server <- .jnew("org/godhuli/rhipe/PersonalServer")
   dbg <- as.integer(Sys.getenv("RHIPE_DEBUG_LEVEL"))
   tryCatch(server$run(if (is.na(dbg)) 
      0L else dbg), Exception = function(e) e$printStackTrace())
   rhoptions(jarloc = opts$jarloc, server = server, clz = list(fileutils = server$getFU(), 
      config = server$getConf()))
   server$getConf()$setClassLoader(.jclassLoader())
   rhoptions(mropts = Rhipe:::rhmropts(), hadoop.env = hadoop)
   packageStartupMessage("Initializing mapfile caches")
   rh.init.cache()
}

