
rhmv <- function(ifile, ofile) {
	ifile = rhabsolute.hdfs.path(ifile)
	ofile = rhabsolute.hdfs.path(ofile)
  system(command=paste(paste(Sys.getenv("HADOOP_BIN"),  "hadoop",
           sep=.Platform$file.sep), "fs", "-mv", ifile, ofile, sep=" "))
  ## v <- Rhipe:::send.cmd(rhoptions()$child$handle,list("rhmv",ifile, ofile))
}
