#' Get Absolute HDFS Path
#'
#' Changes a relative path (a path without a leading /) into an absolute path
#' based on the value of hdfs.getwd().  If it is already an absolute path there 
#' is no change to the returned path.
#'
#' For all returns, any trailing "/" are removed from the path (if path nchar > 1).
#' 
#' @param path Path to examine and change to absolute.
#' @return Absolute HDFS path corresponding to relative path in the input.
#' @author Jeremiah Rounds
rhabsolute.hdfs.path = function(path){
	path = as.character(path)
	if(length(path) > 1)
		stop("Path is not a single string.")
	if(length(path) == 0 || nchar(path) == 0)
		stop("Path is zero length.")
	n = nchar(path)
	if(n > 1 && substr(path,n,n) == "/")
		path = substr(path,1,n-1)
	lead.char = substr(path,1,1)
	#already absolute?
	if(lead.char == "/")
		return(path)
	#this is considered relative..
	wd = hdfs.getwd()
	if(nchar(wd) == 1)
		return(paste(wd,path,sep=""))
	else
		return(paste(wd,path,sep="/"))	
}



