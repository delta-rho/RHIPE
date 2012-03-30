Author: Jeremiah Rounds

This folder contains standalone examples in the form of unit_test.  The unit_test don't conform to a known standard because I felt like the issue of running jobs on Hadoop would be difficult to manage.  If anyone has any suggestions besides the system I cooked up, I am all ears.





## RUNNING EXAMPLES ##

run.all.unit_test.R will need modification before you can run it.  You will need to library(Rhipe) and rhinit().  You will then need to set rhoptions(runner= ...). You will need to set the following things in the global R environment:


base.ofolder -- where on the HDFS to place directories and files during unit test?
zips -- argument to rhmr appropriate for your runner.  Assign NULL to it if you don't need a zips argument.
mapred -- argument to rhmr appopriate for your Hadoop. For example, do we need a 'local' value?  If you don't need mapred in rhmr to run jobs assign NULL to this.

After modification source run.all.unit_test.R.











## TO ADD NEW UNIT TEST EXAMPLES ##

Each stand alone example needs to be in a file that fits the following file name pattern "unit_test.*.R".

Each unit_test file must have a unit_test function of the following form:

unit_test = function(base.ofolder = getwd(), zips=NULL, mapred=list(mapred.job.tracker='local')){
	is.good = FALSE
	try({
		#do MapReduce job and decide if it is good.
		#See existing unit_test files for example.
	}) 

	if(is.good) 
		result = "GOOD"
	else 
		result = "BAD"
	return(as.list(environment()))
}


If those instructions are followed then run.all.unit_test.R will find the new file, try the new job, and consider the output of is.good and result for your job.





