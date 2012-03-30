
#' Simple Hello World Test
#'
#' Outputs 10 Hello World! Key, Value Pairs.
#' ASSUMES Rhipe is setup for arbitrary jobs.
#' @author Jeremiah Rounds
unit_test = function(){
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
		param$ofolder = param$jobname
	
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
