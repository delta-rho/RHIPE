################################################################################################
# I use these on a 10 node cluster
################################################################################################

    library(Rhipe)
    rhinit()
    rhoptions(runner="sh ./RhipeLib.67/RhipeMapReduce.sh")
    zips = "/wsc/jrounds/RhipeLib.67.tar.gz"
    ofolder = "/jrounds/tmp"
    try({rm("mapred")})
    
    
################################################################################################
# I use these on a local hadoop install on my laptop
################################################################################################
	library(Rhipe)
    rhinit()
    try({rm("zips")})
    ofolder	=getwd()
    mapred = list(mapred.job.tracker='local')
    
