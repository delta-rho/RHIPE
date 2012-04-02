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
#' ASSUMES Rhipe is set to run arbitrary jobs.
#' NOTE: the "testing" aspect of this is little more then "did it run?"
#' @author Jeremiah Rounds
unit_test = function(){
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
		param$ofolder = param$jobname
		param$mapred = list()
		################################################################################################
		# NEED TO HANDLE THE TEXT OUTPUT FORMAT
		################################################################################################
		param$mapred$mapred.field.separator=" "
  		param$mapred$mapred.textoutputformat.usekey=FALSE 
  		param$mapred$rhipe.eol.sequence= "\n"



		mr = do.call("rhmr", param)
		ex = rhex(mr,async=FALSE)
		output = rhread(param$ofolder,type="text")
		#do the results look sane?
		l = output[[1000]]
		if(l != "rapfctvjslw savqdbx phwqkufi cahnrtgemy umpoztgwyf dql qguk abvdwklsm zqowjyn euqgifpjz pozbceqf rizajph")
			stop("Output incorrect")
		if(length(output) != 1000)
			stop("Length of output is wrong")
		is.good = TRUE	
	}) #end try

	if(is.good) {
		result = "GOOD"
	} else { 
		result = "BAD"
	}
	return(as.list(environment()))
}


