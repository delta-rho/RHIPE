################################################################################################
# YOU MUST CHANGE THESE VARIABLES TO SOMETHING GOOD FOR YOUR HADOOP
# THESE ARE FOR ME: JEREMIAH ROUNDS
# I ALSO WROTE THIS SCRIPT FOR ME AND NOT FOR EXAMPLE!
# THESE TAKE A LOOK AT SYSTEM VARIABLES TO DECIDE WHAT CLUSTER I AM ON.
################################################################################################

library("Rhipe")
rhinit()
HOSTNAME = Sys.getenv("HOSTNAME")
HOSTNAME = unlist(strsplit(HOSTNAME,".",fixed =TRUE))
if("stat" %in% HOSTNAME){
	cat("Setting Rhipe for Purdue Stat cluster.\n")
	rhoptions(zips = '/RhipeLib_0.67.tar.gz')
	rhoptions(runner = 'sh ./RhipeLib_0.67/library/Rhipe/bin/RhipeMapReduce.sh')
	hdfs.setwd("/jrounds/tmp")
	
} else if("rcac" %in% HOSTNAME){
	cat("Setting Rhipe for RCAC Rossmann cluster.\n")
	rhoptions(zips = '/wsc/jrounds/RhipeLib_0.67.tar.gz')
	rhoptions(runner = 'sh ./RhipeLib_0.67/library/Rhipe/bin/RhipeMapReduce.sh')
	hdfs.setwd("/wsc/jrounds/tmp")
} else {
	cat("HOSTNAME unknown.  Assuming running on local Hadoop such as a laptop.\n")
	rhoptions(mapred = list(mapred.job.tracker='local'))  
	hdfs.setwd(getwd())							#Working directory is required to be set before running unit_test
}

cat("hdfs.getwd() is", hdfs.getwd(),"\n")

