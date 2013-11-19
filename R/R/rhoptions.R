#' Get or Set Rhipe Options
#'
#' Used to set Rhipe options (called by \code{rhinit} for example).  Most often
#' called by a user to set a \emph{runner} that starts the embedded R Rhipe
#' executable.
#' 
#' One may set the following options specific to Rhipe and many options
#' specific to mapred (not listed here but present in the Hadoop
#' documentation).
#' 
#' @param li A list of options.  Names of elements indicate the option being
#'   modified.  Values of elements indicate the value the option is to take on.
#'   If NULL then a list of current rhoptions is returned.  See details.
#' @param \ldots Options maybe assigned values as key=value.  See examples.
#' @return rhoptions() returns a list of current options.
#' @author Saptarshi Guha
#' @note Default values can be observed by simply typing \code{rhoptions()}
#' 
#' \itemize{ \item runner
#' This is the launcher for the embedded R Rhipe executable used in MapReduce
#' jobs.
#' }
#' 
#' Often other options are easiest to set in the \code{mapred} argument of
#' \code{\link{rhmr}}.
#' @seealso \code{\link{rhinit}}, \code{\link{rhmr}}
#' @examples
#' 
#' 	#RUNNABLE BUT LIKELY DOES NOT APPLY TO MOST USERS:
#' 	#sets the runner to be a shell script in the HOME directory.
#' 	#NOT RUN
#' 	#rhoptions(runner=paste(Sys.getenv("HOME"),"rhipe.runner.sh", sep="/"))
#' 	#list all the options
#' 	#rhoptions()
#' 	
#' 	#END NOT RUN
#' 
#' @export
rhoptions <- function(li=NULL,...){
  if(missing(li) && length(list(...))==0){
    get("rhipeOptions",envir=.rhipeEnv)
  }else{
    N <- if(is.null(li)) list(...) else li
    v <- rhoptions()
    for(x in names(N))
      v[[x]] <- N[[x]]
    assign("rhipeOptions",v,envir=.rhipeEnv)
  }
}

rhsetoptions <- function(li=NULL,...){
  warning(sprintf("Use rhoptions instead\n"))
  rhoptions(li,...)
}

optmerge <- function(la,lb){
  ##merges lists la and lb
  ##lb overrides la

  x <- la
  for(n in names(lb)){
    x[[n]] <- lb[[n]]
  }
  x
}

rhmropts <- function(){
  rhuz(rhoptions()$server$rhmropts())
}
