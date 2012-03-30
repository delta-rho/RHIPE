source("my.hadoop.settings.R")



################################################################################################
# IF ABOVE IS APPROPRIATELY SET NOTHING NEED BE CHANGED BELOW THIS LINE
################################################################################################

check_unit = function(filename){
	r = NULL
	try({
	unit_test = NULL
	source(filename)
	r = unit_test()
	r$filename = filename
	cat("TEST ", filename, " " , r$result,"\n")
	})	
	return(r)
}
report.all = function(test.out){
	is.good = sapply(test.out, function(o) {
		is.good = FALSE
		try({
			cat("[",o$result,"]", o$filename ,"\n")	
			is.good = o$is.good
		})
		return(is.good)
	})
	if(all(is.good))
		cat("[ GOOD ] ALL TEST\n")
	else
		cat("[ BAD ] ATLEAST ONE TEST FAIL\n")
		
}
tests = list.files(pattern="unit_test.*.R")
test.out = lapply(tests, check_unit)
report.all(test.out)




