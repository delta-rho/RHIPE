Author: Jeremiah Rounds

This folder contains standalone examples in the form of unit_test.  The unit_test don't conform to a known standard because I felt like the issue of running jobs on Hadoop would be difficult to manage.  If anyone has any suggestions besides the system I cooked up, I am all ears.





## RUNNING EXAMPLES ##

run.all.test.R runs my.hadoop.settings.R to prepare to run Hadoop jobs out of current working directories. Both getwd() and hdfs.getwd() will be used and not cleaned up. It runs unit_test function in all R files with unit_test in the name.











## TO ADD NEW UNIT TEST EXAMPLES ##

Each stand alone example needs to be in a file that fits the following file name pattern "unit_test.*.R".

Each unit_test file must have a unit_test function of the following form:

unit_test = function(base.ofolder = getwd(), zips=NULL, mapred=list(mapred.job.tracker='local')){
	is.good = FALSE
	try({
		#do MapReduce job and decide if it is good.
		#See existing unit_test files for example.
	}) 

	if(is.good) {
		result = "GOOD"
	} else { 
		result = "BAD"
	}
	return(as.list(environment()))
}


If those instructions are followed then run.all.unit_test.R will find the new file, try the new job, and consider the output of is.good and result for your job.





