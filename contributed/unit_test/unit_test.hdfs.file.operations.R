


#' Test HDFS File Operations
#'
#' Using relative file locations
#' ASSUMES Rhipe is set to run arbitrary jobs at a relative file location from current hdfs.getwd().
#' Also it writes to getwd()
#' @author Jeremiah Rounds


unit_test = function(){
	is.good = FALSE
	try({
		try({rhdel("tmp1")},silent=TRUE)
		try({rhdel("tmp2")},silent=TRUE)
		try({rhdel("tmp3")},silent=TRUE)
		try({rhdel("tmp4")},silent=TRUE)
		try({rhdel("tmp5")},silent=TRUE)
		set.seed(1)
		NCOEF = 50
		b = rnorm(NCOEF)
		b.copy = b
		
		
		#RHSAVE, RHCP, RHLS, and then RHLOAD
		rhsave("b", file = "tmp1/export.example.Rdata",envir=environment() )
		b = NULL
		rhcp("tmp1/export.example.Rdata", "tmp2/export.example.Rdata")
		if(!rhdel("tmp1/export.example.Rdata")) NULL = NULL #force error
		x = rhls("tmp1")
		if(nrow(x) != 0)
			NULL = NULL #force error
		rhload(file = "tmp2/export.example.Rdata", envir=environment())
		if(all(b != b.copy))
			NULL = NULL #force error
		b=NULL	
			
		#RHGET FOLLOWED BY LOAD
		LOCAL = paste(getwd(),"export.example.Rdata",sep=.Platform$file.sep)
		try({unlink(LOCAL)},silent=TRUE)
		rhget("tmp2/export.example.Rdata", getwd())
		load(LOCAL)
		if(all(b != b.copy))
			NULL = NULL #force error
		b=NULL
		
		#POSSIBLE BUG
		#sometimes we get a .crc file left at this point so detecting and deleting it
		#if it is left rhput chokes
		LOCAL.CRC = paste(getwd(),".export.example.Rdata.crc", sep=.Platform$file.sep)
		try({unlink(LOCAL.CRC)},silent=TRUE)
		
		#RHPUT FOLLOWED BY RHLOAD
		rhput(LOCAL, "tmp3/export.example.Rdata")
		rhload(file="tmp3/export.example.Rdata",envir=environment())
		if(all(b != b.copy))
			NULL = NULL #force error
		b=NULL
		

		#RHSAVE.IMAGE FOLLOWED BY RHLOAD
		b=b.copy
		rhsave.image(file="tmp4/export.example.Rdata")
		b=NULL
		rhload(file="tmp4/export.example.Rdata",envir=environment())
		if(all(b != b.copy))
			NULL = NULL #force error
		b=NULL
	
		#RHWRITE FOLLOWED BY RHREAD
		value = as.list(1:100)
		rhwrite(value, "tmp5/write.data")
		copy.keyvalue = rhread("tmp5/write.data")
		test.equal = function(x,y) if(x[[2]] != y) NULL = NULL #force error
		mapply(test.equal,copy.keyvalue, value)
		
		################################################################################################
		#clean up
		################################################################################################
		rhdel("tmp1")
		rhdel("tmp2")
		rhdel("tmp3")
		rhdel("tmp4")
		rhdel("tmp5")
		
		is.good = TRUE
	}) #end try
	
	
	if(is.good){ 
		result = "GOOD"
	} else {
		result = "BAD"
	}
	return(as.list(environment()))
}

