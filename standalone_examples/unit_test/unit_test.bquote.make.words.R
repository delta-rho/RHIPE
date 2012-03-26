################################################################################################
# See README.txt
################################################################################################


################################################################################################
# This file both implements a version of make.words from:
# https://github.com/saptarshiguha/RHIPE/wiki/Wordgen
################################################################################################


#' Make Words 
#' Creates a comma separated set of words in a text file.
#'
#' ASSUMES Rhipe library is loaded, rhinit has been called, and rhoptions()$runner is set.
#' Param default values are appropriate for local Hadoop job.
#' Otherwise change them to Hadoop appropriate values.  
#' NOTE: the "testing" aspect of this is little more then "did it run?"
#' @param base.ofolder Folder to place the output directory into (Not the actual ofolder).
#' @param zips Argument to pass to rhmr so that this runs on your Hadoop.
#' @param mapred Argument to pass to rhmr so that this runs on your Hadoop.
#' @author Jeremiah Rounds
unit_test = function(base.ofolder = getwd(), zips=NULL, mapred=list(mapred.job.tracker='local')){
	is.good = FALSE
	try({
	
		################################################################################################
		# PREAMBLE 
		# CODE TO MAKE A LINE OF RANDOM WORDS
		################################################################################################
		
		MU.LETTERS = 7 #
		MU.WORDS = 10
		#make.line poisson distributed number of words and letters per a line (well... subtract 1 and it is true).
		#mu.letters must be 2 or more.
		#mu.words must be 2 or more.
		make.line = function(mu.letters, mu.words){
			if(mu.words < 2) mu.words = 2
			if(mu.letters < 2) mu.letters = 2
			nwords = rpois(1,mu.words - 1) + 1
			nletters = rpois(nwords, mu.letters - 1) + 1
			words = mapply(function(n) paste(sample(letters,n),collapse=""), nletters)
			words = paste(words,collapse=" ")
			return(words)
		}
		#make.line(MU.LETTERS,MU.WORDS)
		
		################################################################################################
		# MAKE THE BQUOTE BASED MAP
		################################################################################################
		
		map.args = list(MU.LETTERS=MU.LETTERS, MU.WORDS=MU.WORDS, make.line=make.line)
		param = list()
		param$map <- as.expression(bquote({
			args = .(map.args)
			attach(args)
			for(v in map.values){
				set.seed(v)
				rhcollect(v,make.line(MU.LETTERS,MU.WORDS))
			}
			detach(args)
		}))
		
		################################################################################################
		# RHMR DETAILS...
		################################################################################################
		
		param$inout = c("lapply","text")
		param$N = 1000
		param$jobname = "bquote_make_words_out"

		#where do we put this output?
		param$ofolder = paste(base.ofolder, param$jobname, sep="/")


		#do you want to run local with mapred=list(mapred.job.tracker='local')?
		if(exists("mapred")){
			param$mapred = mapred
		}else{
			param$mapred = list()
		}
			
		################################################################################################
		# NEED TO HANDLE THE TEXT OUTPUT FORMAT
		################################################################################################
		param$mapred$mapred.field.separator=" "
  		param$mapred$mapred.textoutputformat.usekey=FALSE
  		param$mapred$mapred.reduce.tasks=0

		#do you need an archive for your runner
		if(exists("zips"))
			param$zips = zips

		mr = do.call("rhmr", param)
		ex = rhex(mr,async=FALSE)

		output = rhread(param$ofolder,type="text")
		#do the results look sane?
		if(length(output) == 1000)
			is.good = TRUE
		l = output[[1000]][[2]]
		if(l != "tgmonfbp angspwzcbtjr jvtauwd kultmvjes gylbe qbtwpav vjwaiedtgbx avehzcxgr zgluroxm puilh zdahbrcntq xgktcnlir aultb\r")
			is.good = FALSE
			
	}) #end try


		
	if(is.good) 
		result = "GOOD"
	else 
		result = "BAD"
	return(as.list(environment()))
}


