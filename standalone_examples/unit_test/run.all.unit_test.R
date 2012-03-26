################################################################################################
# BEFORE RUNNING THIS RUN SOMETHING LIKE IN settings.R
################################################################################################
################################################################################################
# CHANGE THESE TO SOMETHING GOOD FOR YOUR HADOOP
# THESE WORK FOR ME ON LOCAL HADOOP FOR EXAMPLE
################################################################################################

library(Rhipe)
rhinit()
#rhoptions(runner="R CMD /home/jrounds/R/x86_64-pc-linux-gnu-library/2.14/Rhipe/libs/imperious.so --slave --silent --vanilla") #my .66 runner.
#YOU MAY NEED TO CHANGE THESE
rhoptions()$runner                     #MapReduce runner script
base.ofolder = getwd()                 #Base HDFS folder to put ouputs into. getwd() is good for local runs.
mapred = list(mapred.job.tracker='local') #Do you need mapred options to run jobs? Make NULL if not.
zips = NULL				               #Do you need zips to run jobs? Otherwise leave NULL.

check_unit = function(filename){
	r = NULL
	try({
	source(filename)
	r = unit_test(base.ofolder, zips, mapred)
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




