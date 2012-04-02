################################################################################################
# A NON-RIGOROUS TEST OF RHIPE'S COMBINER ON A 'LOCAL' HADOOP INSTALL
################################################################################################



#' Simple Summary Combiner Test
#'
#' In this example we are imagining we have chunks of a matrix with 100 columns stored in 10 row
#' key, value pairs.
#
#' To further complicate the issue we are saying the chunks of matrix actually get reduced to two
#' different keys.
#
#' We seek to calculate the min, max, mean, sum, and nrecords in each column of the matrix.
#' We use a combiner to speed up calculations.

#' ASSUMES Rhipe is set to run arbitrary jobs at a relative file location from current hdfs.getwd().
#' @author Jeremiah Rounds


unit_test = function(){
	is.good = FALSE
	try({
	param = list()
	param$map = expression({
		emit.summary = function(data){
			emit = list()
			emit$min = apply(x,2,min)
			emit$max = apply(x,2,max)
			emit$mean = apply(x,2,mean)
			emit$sum = apply(x,2,sum)
			emit$sumsq = apply(x,2, function(c) sum(c^2))
			emit$nrecords = nrow(x)
			#shape of emit is a list of 4 things
			#min, max, mean are a vector of length ncol(x)
			#nrecords is a scalar.
			return(emit)
		}
		for (r in map.values){
			set.seed(r)
			NCOL = 10
			NROW = 10
			x = runif(NROW*NCOL)
			x = matrix(x,NROW,NCOL)
			offset = matrix(1:NCOL,NCOL,NROW)
			offset = t(offset)   #now has the index of the column in each element of a column
			x = x + offset 
			# r%% 2 is only some non-sense to make a bunch of keys reshape to two keys for testing.
			rhcollect(r %% 2, emit.summary(x))
		}
	})
	param$reduce = expression(
		pre={
			#helper function to extract name, rbind, and apply to columns func.
			extract.reapply = function(list.emitted, name, func){
				ret = lapply(list.emitted, function(e) e[[name]])
				ret = do.call("rbind",ret)
				ret  = apply(ret,2,func)
				return(ret)
			}
			emit.summary = function(list.emitted){
				emit = list()
				#we have many vectors of mins and need to find the min for each column if they are rbind.
				emit$min = extract.reapply(list.emitted, "min", min)
				emit$max = extract.reapply(list.emitted, "max", max)
				emit$sum = extract.reapply(list.emitted, "sum", sum)
				emit$sumsq = extract.reapply(list.emitted, "sumsq", sum)
				emit$nrecords = as.numeric(extract.reapply(list.emitted, "nrecords", sum))
				#mean needs to be handled seperately
				emit$mean = emit$sum/emit$nrecords
				#note how this is the same shape that comes out of the map
				return(emit)			
			}
			#storage
			list.summary.data = list()
			index = 1
		},
		reduce={
			list.summary.data[[index]] = emit.summary(reduce.values)
			index = index + 1
		},
		post={
			rhcollect(reduce.key, emit.summary(list.summary.data))
		}
	)
	param$inout = c("lapply","sequence")
	param$N = 1000
	param$jobname = "simple_summary_combiner_out"
	param$combiner = TRUE
	param$ofolder =  param$jobname
	
	mr = do.call("rhmr", param)
	ex = rhex(mr,async=FALSE)

	output = rhread(param$ofolder,type="sequence")
	#do the results look sane?

	mins = output[[1]][[2]]$min
	if(! all(order(mins) == 1:10))
		stop("Order mins in [[1]] wrong."
	mins = output[[2]][[2]]$min
	if(! all(order(mins) == 1:10))
		stop("Order mins in [[2]] wrong.")
	if(length(output) == 2)
		stop("Length output wrong.")
	is.good = TRUE
	}) #end try
	
	
	if(is.good) {
		result = "GOOD"
	} else { 
		result = "BAD"
	}
	return(as.list(environment()))
}

