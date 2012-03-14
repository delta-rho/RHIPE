#Author: roundsjeremiah@gmail.com
#R DEFINES THE FOLLOWING THINGS WHEN IT CALLS THIS SCRIPT FOR A PACKAGE INSTALL
#HANDY TO DO SENSIBLE DEFINES FOR TESTING STARTING WITH THE src DIRECTORY AS CURRENT WORKING DIRECTORY.
if(!exists("SHLIB_EXT"))
	SHLIB_EXT = "so"
if(!exists("R_PACKAGE_DIR"))
	R_PACKAGE_DIR = normalizePath(paste(getwd(),"..",sep="/"))
if(!exists("R_PACKAGE_SOURCE"))
	R_PACKAGE_SOURCE = R_PACKAGE_DIR
if(!exists("R_ARCH"))
	R_ARCH = ""
initial.wd = getwd()   #unclear whether or not this is always R_PACKAGE_SOURCE


#NOW OUR WORK
rhipe.src.dir = normalizePath(paste(R_PACKAGE_SOURCE,"/src",sep=""))
################################################################################################
#CREATE TARGET LIB DIRECTORY
#do we need to do this?
libarch <- if (nzchar(R_ARCH)) paste('libs', R_ARCH, sep='') else 'libs'
pkg.lib.dir <- normalizePath(file.path(R_PACKAGE_DIR, libarch))
dir.create(pkg.lib.dir, recursive = TRUE, showWarnings = FALSE)
################################################################################################



################################################################################################
# get shared files
# this doesn't actually get Rhipe.so on windows, but if we are on windows we have bigger issues...
setwd(rhipe.src.dir)
shared.files <- Sys.glob(paste("*", SHLIB_EXT, sep=''))
file.copy(shared.files,pkg.lib.dir, overwrite=TRUE)
################################################################################################


################################################################################################
#get RhipeMapReduce
RhipeMapReduce = list.files(path=getwd(),pattern="RhipeMapReduce",full.names=TRUE)
file.copy(RhipeMapReduce,pkg.lib.dir, overwrite=TRUE)
################################################################################################






################################################################################################
# Report the constants
cat("\n")
cat("SHLIB_EXT ", SHLIB_EXT,"\n")
cat("R_PACKAGE_DIR ", R_PACKAGE_DIR,"\n")
cat("R_PACKAGE_SOURCE ", R_PACKAGE_SOURCE, "\n")
cat("R_ARCH ", R_ARCH,"\n")
cat("\n\n")
################################################################################################



#DONE!
setwd(initial.wd)

