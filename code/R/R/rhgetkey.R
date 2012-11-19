is.mapfolder <- function(apath){
  f <- rhls(apath)$file
  idx <- any(sapply(f,function(r) grepl("index$",r)))
  dta <- any(sapply(f,function(r) grepl("data$",r)))
  if(idx && dta) TRUE else FALSE
}
dir.contain.mapfolders <- function(dir){
  paths = rhabsolute.hdfs.path(dir)
  paths <- rhls(paths)$file
  paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths)]
  all(sapply(paths, is.mapfolder))
}
    

#' Get Value Associated With a Key In A Map File
#'
#' Returns the values associated with a key in a map file on the HDFS.
#' 
#' @param keys Keys to return values for.
#' @param paths Absolute path to map file on HDFS or the output from \code{rhwatch}.
#' @param mc This is set to \code{lapply}, the user can set this to \code{mclapply} for parallel \code{lapply}
#' @param size Number of Key,Value pairs to increase the buffer size by at a time.
#' @author Saptarshi Guha
#' @return Returns the values from the map files contained in \code{path} corresponding
#' to the keys in \code{keys}. \code{path} will contain folders which is
#' MapFiles are stored. Thus the \code{path} must have been created as the
#' output of a RHIPE job with \code{inout[2]} (the output format) set to
#' \emph{map}. Also, the saved keys must be in sorted order. This is always the
#' case if \emph{mapred.reduce.tasks} is not zero. The variable
#' \emph{reduce.key} is not modified.
#' @note A simple way to convert any RHIPE SequenceFile data set to MapFile is to run
#' an identity MapReduce.
#' @seealso \code{\link{rhget}}, \code{\link{rhput}},
#'   \code{\link{rhdel}}, \code{\link{rhwrite}}, \code{\link{rhsave}}
#' @keywords keys HDFS file
#' @export
rhgetkey <- function (keys, paths, mc = lapply, size = 3000) 
{
    sequence = ""
    if (is(paths, "rhwatch")) 
      paths <- rhofolder(paths)
    paths = rhabsolute.hdfs.path(paths)
    paths <- rhls(paths)$file
    paths <- paths[!grepl(rhoptions()$file.types.remove.regex, paths)]
    if (length(paths)==0 || !all(is.character(paths)) || !all(sapply(paths, is.mapfolder)) )
        stop("paths must be a character vector of mapfiles( a directory containing them or a single one)")
    keys <- lapply(keys, rhsz)
    akey <- paste(head(strsplit(paths[1],"/")[[1]],-1),sep="",collapse="/")
    p <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhgetkeys", 
        list(keys, paths, sequence, if (sequence == "") FALSE else TRUE
             ,akey)), getresponse = 0L, conti = function() {
        return(Rhipe:::rbstream(rhoptions()$child$handle, size, 
            mc))
    })
    p
}

#' Creates a Handle to a Mapfile
#' @param Absolute path to map file on HDFS or the output from \code{rhwatch}.
#' @export
rhmapfile <- function(paths){
  if(is(paths, "rhwatch") || is(paths,"rhmr"))
    paths <- rhofolder(paths)
  akey <- paste(head(strsplit(paths[1],"/")[[1]],-1),sep="",collapse="/")
  invisible(rhgetkey(NULL,paths))
  obj <- new.env()
  obj$filename <- akey
  obj$paths <- paths
  class(obj) = "mapfile"
  obj
}
#' Prints A MapFile Object
#' @export
print.mapfile <- function(a,...){
  cat(sprintf("%s is a MapFile with %s index files\n", a$filename, nrow(rhls(a$paths))))
}

#' Single Index into MapFile Object(calls \code{rhgetkey}
#' @export
"[[.mapfile" <- function(a,i,...){
  a <- rhgetkey(i,a$paths)
  if(length(a)==1) a[[1]] else NULL
}

#' Array Indexing
#' @export
"[.mapfile" <- function(a,i,...){
  rhgetkey(i,a$paths)
}

#' Cannot Assign to Object
#' @export
"[[<-.mapfile" <- function(a,i,...){
  stop("Assignment to MapFile keys is not supported")
}

#' Cannot Assign to Object
#' @export
"[<-.mapfile" <- function(a,i,...){
  stop("Assignment to MapFile keys is not supported")
}



# rhgetkey <- function(keys,paths,sequence=NULL,skip=0,ignore.stderr=T,verbose=F,...){
#   on.exit({
#     if(dodel) unlink(tmf)
#   })
#   if(is.null(sequence)){
#     dodel=T
#     tmf <- tempfile()
#   }else{
#     tmf=sequence;dodel=F
#   }
#   pat <- rhls(paths)
#   if(substr(pat[1,'permission'],1,1)!='-'){
#     paths <- pat$file
#   } ## if == "-", user gave a single map file folder, dont expand it
#   
#   if(!all(is.character(paths)))
#     stop('paths must be a character vector of mapfiles( a directory containing them or a single one)')
#   keys <- lapply(keys,rhsz)
#   paths=unlist(paths)
#   Rhipe:::doCMD(rhoptions()$cmd['getkey'], keys=keys,src=paths,dest=tmf,skip=as.integer(skip),sequence=!is.null(sequence),
#                 ignore.stderr=ignore.stderr,verbose=verbose)
#   if(is.null(sequence)) rhreadBin(tmf,...,verb=verbose)
# }
## rhgetkey(list(c("f405ad5006b8dda3a7a1a819e4d13abfdbf8a","1"),c("f2b6a5390e9521397031f81c1a756e204fb18","1")) ,"/tmp/small.seq")


