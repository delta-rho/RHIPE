
#' Test rhabsolute.hdfs.path.R
#'
#' rhabsolute.hdfs.path() a great variety of inputs. 
#' So here we are checking to see if it does what we expect.
#' ASSUMES Rhipe is setup for arbitrary jobs.
#' @author Jeremiah Rounds
unit_test = function(){
	is.good = FALSE
	check.equal = function(input, error.message){
		ret = rhabsolute.hdfs.path(input)
		if(length(ret) != length(input))
			stop(error.message)
		if(any(ret != input))
			stop(error.message)
		return(TRUE)
	}
	try({
		x = NA
		ret = rhabsolute.hdfs.path(x)
		if(length(ret) != 1 || !is.na(ret))
			stop("Expected NA.")
			
		x = rep(NA,10)
		ret = rhabsolute.hdfs.path(x)
		if(length(ret) != 10 || any(!is.na(ret)))
			stop("Length 10 vector of NA expected.")
			
		x = as.list(x)
		ret = rhabsolute.hdfs.path(x)
		if(class(x) != "list" || length(ret) != 10 || any(!is.na(ret)))
			stop("Length 10 list of NA expected.")
		
		x = c("","/garbage","/garbage2/")
		ret = rhabsolute.hdfs.path(x)
		if(length(ret) != 3 || ret[1] != "" || ret[2] != "/garbage" || ret[3] != "/garbage2")
			stop("Test fail.")
			
		is.good = TRUE
	})
	if(is.good) {
		result = "GOOD"
	} else { 
		result = "BAD"
	}
	return(as.list(environment()))
}
