################################################################################################
# In this example we demonstrate how to upload an R object to the HDFS for use in MapReduce.
# We simulate an n x p matrix divided into chunks of m rows as observed covariates 
# and a response of length n divided up itself.
################################################################################################


#' Matrix Simulation With Exported RDATA
#'
#' ASSUMES Rhipe library is loaded, rhinit has been called, and rhoptions()$runner is set.
#' Param default values are appropriate for local Hadoop job.
#' Otherwise change them to Hadoop appropriate values.  
#' NOTE: the "testing" aspect of this is little more then "did it run?"
#' @param base.ofolder Folder to place the output directory into (Not the actual ofolder).
#' @param zips Argument to pass to rhmr so that this runs on your Hadoop.
#' @param mapred Argument to pass to rhmr so that this runs on your Hadoop.
#' @author Jeremiah Rounds
unit_test = function(base.ofolder = getwd(), zips=NULL, mapred=list(mapred.job.tracker='local')){
	is.good = FALSE
	try({
		################################################################################################
		# Task 1 define some beta coefficients that will form the model.
		################################################################################################
		set.seed(1)
		NCOEF = 50
		b = rnorm(NCOEF)
		#placing some structure in the coefficients to see if we can see it later
		b= b + 4*sin((1:NCOEF)/NCOEF*2*pi)   
		NROW=100
		#DEMO PUTTING OBJECTS ON HDFS
		EXPORT_EXAMPLE_RDATA = paste(base.ofolder,"export.example.Rdata",sep="/")
		rhsave("b","NCOEF","NROW", file = EXPORT_EXAMPLE_RDATA,envir=environment() )


		################################################################################################
		# Task 2 simulate the matrix in chunks and demo loading the exported data.
		################################################################################################


		param = list()
		param$setup = expression({
			#DEMO GETTING YOUR OBJECTS FROM THE HDFS.
			#environment variable comes from the mapred object of the job.
			load("export.example.Rdata")
			#loaded NCOEF and b
		})
		param$map = expression({
			for (r in map.values){
				set.seed(r)
				X = runif(NROW*NCOEF)
				X = matrix(X,NROW,NCOEF)
				y = X %*% b + rnorm(NROW)
				y = as.numeric(y)
				rhcollect(r, list(X = X, y = y,files=system("ls",int=TRUE)))
			}
		})

		param$inout = c("lapply","sequence")
		param$N = 100
		param$jobname = "export_rdata_sim_data"
		param$shared = EXPORT_EXAMPLE_RDATA


		#where do we put this output?
		param$ofolder = paste(base.ofolder, param$jobname, sep="/")

		#do you want to run local with mapred=list(mapred.job.tracker='local')?
		if(exists("mapred"))
			param$mapred = mapred


		#do you need an archive for your runner
		if(exists("zips"))
			param$zips = zips
	
		mr = do.call("rhmr", param)
		ex = rhex(mr,async=FALSE)

		#what do we got?
		output = rhread(param$ofolder,type="sequence", max=1)	
		value = output[[1]][[2]]
		X = value$X
		Y = value$Y
		files = value$files
		is.good = TRUE
		#does output look sane?
		if(output[[1]][[1]] > param$N)
			is.good = FALSE
		if(nrow(X) != NROW)
			is.good = FALSE
		if(ncol(X) != NCOEF)
			is.good = FALSE
	}) # end try
	
	
	if(is.good) 
		result = "GOOD"
	else 
		result = "BAD"
	return(as.list(environment()))
}	

