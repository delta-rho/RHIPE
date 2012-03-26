
#' Simple Hello World Test
#'
#' Outputs 10 Hello World! Key, Value Pairs.
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
		param = list()
		param$map = expression({
		  for (r in map.values){
			rhcollect(r, "Hello World!")
		  }
		})
		param$reduce = expression(
			pre={},
			reduce={rhcollect(reduce.key, reduce.values)},
			post={}
		)
		param$inout = c("lapply","sequence")
		param$N = 10
		param$jobname = "simple_hello_world"

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
		output = rhread(param$ofolder,type="sequence")
		
		#little more then does it look sane?
		if(length(output) == 10)
			is.good = TRUE
		if(output[[1]][[2]] != "Hello World!")
			is.good = FALSE
	})
	if(is.good) 
		result = "GOOD"
	else 
		result = "BAD"
	return(as.list(environment()))
}
