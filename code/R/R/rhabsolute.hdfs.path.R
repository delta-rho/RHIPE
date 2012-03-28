#' Get Absolute HDFS Path
#'
#' Changes a relative path (a path without a leading /) into an absolute path
#' based on the value of hdfs.getwd().  If the path is already absolute this 
#' function does nothing.
#' 
#' @param path Path to examine and change to absolute.
#' @return Absolute HDFS path corresponding to relative path in the input.
#' @author Jeremiah Rounds
rhabsolute.hdfs.path = function(path){
	path = as.character(path)
	if(length(path) > 1)
		stop("Path is not a single string")
	if(length(path) == 0 || nchar(path) == 0)
		stop("Path is zero length.")
	lead.char = substr(path,1,1)
	if(lead.char == "/")
		return(path)
	wd = hdfs.getwd()
	return(paste(wd,path,sep="/"))	
}



