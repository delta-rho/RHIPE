#' Get Value Associated With a Key In A Map File
#'
#' Returns the values associated with a key in a map file on the HDFS.
#' 
#' @param keys Keys to return values for.
#' @param paths Absolute path to map file on HDFS.
#' @param mc This is set to \code{lapply}, the user can set this to \code{mclapply} for parallel \code{lapply}
#' @param skip Corresponds to io.map.index.skip
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
rhgetkey <- function(keys,paths,mc=lapply,size=3000,skip=0L){
	#TODO: Add these back to the functional arguments when someone is ready to comment on what they do.
	sequence=""
	paths = rhabsolute.hdfs.path(paths)
	pat <- rhls(paths)
	if (substr(pat[1, "permission"], 1, 1) != "-")  paths <- pat$file
	if (!all(is.character(paths))) 
	stop("paths must be a character vector of mapfiles( a directory containing them or a single one)")
	keys <- lapply(keys, rhsz)
	paths <- unlist(paths)
	p <- Rhipe:::send.cmd(rhoptions()$child$handle, list("rhgetkeys", list(keys,paths,sequence,
		       if(sequence=="") FALSE else TRUE,
		       as.integer(skip)))
	   ,getresponse=0L,
	   conti = function(){
		 return(Rhipe:::rbstream(rhoptions()$child$handle,size,mc))
	   })
	p
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
