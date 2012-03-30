################################################################################################
# YOU MUST CHANGE THESE VARIABLES TO SOMETHING GOOD FOR YOUR HADOOP
# THESE WORK FOR ME ON LOCAL HADOOP FOR EXAMPLE
################################################################################################
LOCAL = FALSE
library(Rhipe)
rhinit()

if(LOCAL){
	#YOU MAY NEED TO CHANGE THESE
	rhoptions()$runner                     	#MapReduce runner script do you need to change it?
	rhoptions(mapred = list(mapred.job.tracker='local'))  #Do you need mapred options to run jobs? Comment out if not
	rhoptions(zips = c())                  	#Do you need zips to run jobs? Otherwise leave c()
	hdfs.setwd(getwd())						#Working directory is required to be set before running unit_test
}else{
	#definately need to change these!
	rhoptions(runner="sh ./RhipeLib_0.67/RhipeMapReduce")
	rhoptions(zips = "/RhipeLib_0.67/RhipeMapReduce")
	hdfs.setwd("/jrounds/tmp")
	

}
