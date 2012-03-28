#' Get Absolute HDFS Path
#'
#' Changes a relative path (a path without a leading /) into an absolute path
#' based on the value of hdfs.getwd().  If it is already an absolute path there 
#' is no change to the returned path.
#'
#' For all returns, any trailing "/" are removed from the path (if path nchar > 1).
#' 
#' @param path Path to examine and change to absolute.  Input may be a list or vector.
#' @return Absolute HDFS path corresponding to relative path in the input.  If input is a vector or list returns a vector or list of paths.
#' @author Jeremiah Rounds
rhabsolute.hdfs.path = function(paths){
	inpaths = as.character(paths)
	ret = list()
	wd = hdfs.getwd()
	for(i in seq_along(inpaths)){
		p = inpaths[i]
		if(length(p) == 0 || nchar(p) == 0)
			stop("Path is zero length.")
		n = nchar(p)
		#we take a trailing "/" if it exist.
		if(n > 1 && substr(path,n,n) == "/")
			p = substr(p,1,n-1)
		lead.char = substr(p,1,1)
		#already absolute?
		if(lead.char == "/"){
			retp = p
		}else{
			#this is considered relative..
			if(nchar(wd) == 1)
				retp = paste(wd,path,sep="")
			else
				retp = paste(wd,path,sep="/")	
		}
		ret[[i]] = retp
	}
	
	#now match the class of the input
	#wouldn't surprise me to learn there is a more elegant way to do this.
	if("character" %in% class(paths))
		return(as.character(ret))  #should work for everything else.
	if("list" %in% class(paths))
		return(ret)  #already a list
}



