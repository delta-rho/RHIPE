################################################################################################
# YOU MUST CHANGE THESE VARIABLES TO SOMETHING GOOD FOR YOUR HADOOP
# THESE ARE FOR ME: JEREMIAH ROUNDS
# I ALSO WROTE THIS SCRIPT FOR ME AND NOT FOR EXAMPLE!
# THESE TAKE A LOOK AT SYSTEM VARIABLES TO DECIDE WHAT CLUSTER I AM ON.
################################################################################################

library("Rhipe")
rhinit()
HOSTNAME = Sys.getenv("HOSTNAME")
if(HOSTNAME == "jrlaptop"){						
	#RUNNING ON MY LAPTOP
	
	rhoptions(mapred = list(mapred.job.tracker='local'))  #Do you need mapred options to run jobs? Comment out if not
	hdfs.setwd(getwd())							#Working directory is required to be set before running unit_test
	
	
}else if(HOSTNAME == "spica.stat.purdue.edu"){
	#RUNNING ON PURDUE STAT CLUSTER
	

	rhoptions(zips = '/RhipeLib_0.67.tar.gz')
	rhoptions(runner = 'sh ./RhipeLib_0.67/library/Rhipe/bin/RhipeMapReduce.sh')
	hdfs.setwd("/jrounds/tmp")
	

}
