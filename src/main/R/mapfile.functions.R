is.mapfolder <- function(apath) {
   f <- rhls(apath)$file
   idx <- any(sapply(f, function(r) grepl("index$", r)))
   dta <- any(sapply(f, function(r) grepl("data$", r)))
   if (idx && dta) 
      TRUE else FALSE
}
dir.contain.mapfolders <- function(dir) {
   paths <- rhabsolute.hdfs.path(dir)
   paths <- rhls(paths)$file
   paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths)]
   all(sapply(paths, is.mapfolder))
}


#' Initializes the Value and Mapfile caches
#' @param mbsize is the number of bytes (i.e. key/values) read from a MapFile cached
#' @param openhandles is the number of file handles cached
#' @details
#' Keep \code{openhandles} below the maximum number of open sockets
#' @export
rh.init.cache <- function(mbsize = 1024 * 1024 * 100, openhandles = 100) {
   rhoptions()$server$initializeCaches(as.integer(mbsize), as.integer(openhandles))
}


#' Creates a Handle to a Mapfile
#' @param paths Absolute path to map file on HDFS or the output from \code{\link{rhwatch}}.
#' @export
rhmapfile <- function(paths) {
   if (is(paths, "rhwatch") || is(paths, "rhmr")) 
      paths <- rhofolder(paths)
   paths <- rhabsolute.hdfs.path(paths)
   paths <- rhls(paths)
   paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths$file), , drop = FALSE]$file
   if (length(paths) == 0) 
      stop("paths must be a character vector of mapfiles( a directory containing them or a single one)")
   akey <- paste(head(strsplit(paths[1], "/")[[1]], -1), sep = "", collapse = "/")
   a <- rhoptions()$server$initializeMapFile(paths, akey)
   obj <- new.env()
   obj$filename <- akey
   obj$paths <- paths
   class(obj) <- "mapfile"
   obj
}

#' Prints A MapFile Object
#' @method print mapfile
#' @param x mapfile object
#' @param ... further arguments passed to or from other methods
#' @export
print.mapfile <- function(x, ...) {
   cat(sprintf("%s is a MapFile with %s index files\n", x$filename, length(x$paths)))
}

getkey <- function(v, keys, mc = lapply) {
   a <- rhuz(rhoptions()$server$rhgetkeys2(v$filename, .jarray(rhsz(as.list(keys)))))
   mc(a, rhuz)
}

#' @export
"[[.mapfile" <- function(a, i, ...) {
   getkey(a, i, ...)[[1]]
   ## if(length(a)>=1) a[[1]] else NULL
}

#' @export
"[.mapfile" <- function(a, i, ...) {
   getkey(a, i, ...)
}

#' @export
"[[<-.mapfile" <- function(x, value, i, ...) {
   stop("Assignment to MapFile keys is not supported")
}

#' @export
"[<-.mapfile" <- function(x, value, i, ...) {
   stop("Assignment to MapFile keys is not supported")
}



#' Returns Cache Information
#' @param which is one of 'filehandles' or 'valuebytes'
#' @details
#' This functions returns a data frame containg the cache statistics for the value cache and filehandle cache
#' set in \code{rh.init.cache}. See \url{http://docs.guava-libraries.googlecode.com/git-history/release/javadoc/com/google/common/cache/CacheStats.html} for an explanation
#' @export
rhcacheStats <- function(which = c("filehandles", "valuebytes")) {
   which <- list(filehandles = 0L, valuebytes = 1L)[[which]]
   v <- rhuz(rhoptions()$server$cacheStatistics(as.integer(which)))
   v <- data.frame(measure = c("averageLoadPenalty", "evictionCount", "hitCount", 
      "hitRate", "loadCount", "loadExceptionCount", "loadExceptionRate", "loadSuccessCount", 
      "missCount", "missRate", "requestCount", "totalLoadTime"), stat = v)
   v
} 
