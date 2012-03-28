#' Set HDFS Working Dir
#'
#' Sets a working directory for all Rhipe commands that use the HDFS.
#' By default hdfs.working.dir is set to "/".  All input directories to
#' all Rhipe commands may omit a leading "/" and indicate they are off of
#' the current hdfs.working.dir. 
#' 
#' This is anagolous to how file operations
#' in R work off the current working directory.
#
#' This function will be slow in a loop because it list the files in
#' the HDFS directory to make sure it exist.
#' 
#' @param path Path to set working directory.  May be relative the current 
#' HDFS working directory or absolute.
#' @author Jeremiah Rounds
#' @seealso \code{\link{hdfs.getwd}}
#' @export
hdfs.setwd = function(path){
	path = rhabsolute.hdfs.path(path)
	#safety first.  See if the directory exist reusing a list.  
	#Is there a better command that doesn't have the overhead of returning files?
	x =  Rhipe:::send.cmd(rhoptions()$child$handle, list("rhls", 
        path, 0L))
	rhoptions(hdfs.working.dir = path)
}
