################################################################################################
# bashRhipeArchive
#' THIS ONLY IS EXPECTED TO WORK FOR SOMEONE USING A BASH SHELL. 
#' Creates an archive on the HDFS with a runner script appropriate for running Rhipe jobs.
#' Uploads that archive to the HDFS
#'
#' WARNING: CHANGES YOUR RUNNER AND ZIPS RHOPTIONS TO USE THAT ARHIVE.
#'
#' YOU MUST SET BOTH WORKING DIRECTORIES AND HDFS WORKING DIRECTORIES PRIOR TO CALLING THIS.
#' USE setwd() AND hdfs.setwd() PRIOR TO CALLING THIS. 
#'
#'
#' @param archive.base.name  Base file name.  This will be the name of a temp directory left in the current
#' working directory and the name of the tar.gz placed on the HDFS.
#' @param delete.local.tmp.dir Delete the local.tmp.dir before starting if TRUE.
#' @examples
#' 
#' \dontrun{
#' library("Rhipe")
#' rhinit()
#' setwd("~")    		# MUST SET THIS
#' hdfs.setwd("/tmp") 	# MUST SET THIS
#' system("rm -Rf RhipeLib_0.67")
#' createBashRhipeLibArchive("RhipeLib_0.67")
#'}
#' @author Jeremiah Rounds 
#' @export

bashRhipeArchive = function(archive.base.name="RhipeLib", delete.local.tmp.dir=TRUE){
	################################################################################################
	# This is the most portable way I thought up to copy files recursively.
	# file.copy by itself has let me down....
	################################################################################################
	file.copy = function(src, dest, recursive=TRUE,...){
		if(recursive)
			system(paste("cp -RLf", src, dest))
		else
			system(paste("cp -Lf", src, dest))
	}
	#Also rewriting this to be recursive...
	Sys.chmod = function(dest){
		system(paste("chmod -R 777",dest))
	}
	################################################################################################
	#Copies the contents of .libPaths() to a destination folder that must already exist.
	################################################################################################
    copy.libPaths = function(dest){
    	dest = normalizePath(dest)
    	lib.paths = rev(.libPaths())
    	for(p in lib.paths){
    		contents = list.files(p, full.names=TRUE) 
    		if(length(contents) == 0) next
    		for(d in contents)
    			file.copy(d, dest,recursive=TRUE)
    	}
    
    }
    ################################################################################################
    #Find exectuables and shared Libs recursively
    #Two types of strats: 
    #1) find executables using a test I found on the internet
    #2) find things with .so extension.
    # Works off current working directory.
    ################################################################################################
    findExecutables = function(){
    	find.exec.cmd = "find  -type f -perm -100 -exec sh -c \"file -i '{}' | grep -q 'x-executable; charset=binary' \" \\; -print"
    	execs = system(find.exec.cmd, intern=TRUE)
    	shared.libs = list.files(pattern=paste(.Platform$dynlib.ext,"$",sep=""), recursive=TRUE, ignore.case=TRUE, all.files=TRUE)
    	return(append(execs,shared.libs))

    }
    ################################################################################################
    # sharedLibs
    # Finds shared libs that an exec depends on with "ldd"
    # Designed for multiple files.
    # Does not insert the same shared lib more then once.
    ################################################################################################
   	sharedLibs = function(files){
   		shared = list()
   		for(f in files){
   			try({
		   		a <- system(sprintf("ldd  %s",f),intern=TRUE)
				b <- lapply(strsplit(a,"=>"), function(r) if (length(r)>=2) r[2] else NULL)
				b <- strsplit(unlist(b)," ")
				b <- unlist(lapply(b,"[[",2))
				for(lib in b ) if(nchar(lib) > 0) shared[[lib]] = lib
			}, silent=TRUE)
		}
		return(unlist(shared))
   	}


    ################################################################################################
    # ARCHIVE CREATION
    ################################################################################################
    
    # TASK 1: CREATE FOLDER THAT WILL BECOME ARCHIVE
    tfolder = normalizePath(archive.base.name,mustWork = FALSE)
    if(delete.local.tmp.dir && file.exists(tfolder)){
     	cat("Trying to delete old", tfolder, "\n")
     	Sys.chmod(tfolder)
    	try({unlink(tfolder, recursive=TRUE, force=TRUE)},silent=TRUE)	
    }
    cat("Creating new", tfolder, "\n")
    if(!file.exists(tfolder))
    	dir.create(tfolder)
    Sys.chmod(tfolder)
	oldwd = normalizePath(getwd())
	setwd(tfolder)
    
    # TASK 2: COPY R.home 
    cat("Copying contents of R.home() to", tfolder, "\n")    
    src = Sys.glob(paste(R.home(),"*", sep=.Platform$file.sep))
    dest = normalizePath(".")
    for(f in src) file.copy(f,dest,recursive=TRUE)
    Sys.chmod(dest)
    
    # TASK 3: COPY R LIBRARIES
    if(!file.exists("library"))
    	dir.create("library")
    library = normalizePath("library")
    cat("Copying all detected R libraries to", library, "\n")
	copy.libPaths("library")
	Sys.chmod(getwd())

	# TASK 4: FIND ALL SHARED LIBRARY DEPENCIES AND COPY THEM 
    src = sharedLibs(findExecutables())
    dest = normalizePath("lib")
    cat("Copying all detected shared library dependencies to", dest,"\n")
    for(f in src) file.copy(f,dest)
    Sys.chmod(getwd())
    
    # TASK 5: DETECT RUNNER SCRIPT
    base.runner =paste("library","Rhipe","bin","RhipeMapReduce.sh",sep=.Platform$file.sep)
    runner = normalizePath(base.runner)
    if(file.exists(runner)){
    	cat("Runner script found at", runner, "\n")
    } else {
    	stop("Runner script not detected at", runner,"\n")
    }
    
	# TASK 6: CREATE ARCHIVE FROM TMP FOLDER
	tar.file.name = paste(archive.base.name, ".tar.gz", sep="")
	tar.file = paste(oldwd, tar.file.name, sep=.Platform$file.sep)
	cat("Creating archive at", tar.file, "\n")
	tar(tar.file, files=Sys.glob("*"),compression="gzip")
	
	setwd(oldwd)
	# TASK 7: UPLOAD ARCHIVE
	dest = rhabsolute.hdfs.path(tar.file.name)
	cat("Placing archive on HDFS at", dest,"\n")
	rhput(tar.file,tar.file.name) # YOU MUST HAVE USED hdfs.setwd() PRIOR TO CALLING
    
	# TASK 8: REPORT ZIPS AND RUNNERS FOR USER INSPECTION AND SET
	zips =  rhabsolute.hdfs.path(tar.file.name)
	
	runner = paste("sh .", archive.base.name, base.runner, sep=.Platform$file.sep)
	cat("\n***********************************************************************\n")
	cat("In the future use:\n")
	cat("rhoptions(zips = '", zips,"')\n",sep="")
	cat("rhoptions(runner = '", runner, "')\n",sep="")
	cat("Setting these options now.\n")
	cat("***********************************************************************\n")
	rhoptions(zips = zips)
	rhoptions(runner = runner)
        
}

