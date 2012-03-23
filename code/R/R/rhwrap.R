#' Macro to Wrap Boilerplate Around RHIPE Map code
#'
#' Returns an expression corresponding to given input
#'
#' @param expr Any R expression, that operates on current map.keys, map.values and current index (given by \code{k},\code{r}, and \code{.index} respectively) 
#' @param before An R expression to run before the loop across map.values,map.keys and .index. If map.values is shortened, make map.keys the same length!
#' @param after An R expression to run after the loop. The results of the loop is contained in \code{result}
#' @param trap Any R error causes the MapReduce job to quit if set to TRUE.
#' @seealso \code{\link{rhmr}}
#' @keywords MapReduce Map
#' @export
rhwrap <- 
function(expr=NULL,before=NULL,after=NULL,trap=FALSE){
  co <- substitute(expr); before=substitute(before)
  err <- if(trap) function(e) { rhcounter("R_ERRORS",as.character(e),1)} else function(e) {rhcounter("R_UNTRAPPED_ERRORS",as.character(e),1)}
  as.expression(bquote({
    .(BE)
    result <- mapply(function(.index,k,r){
      tryCatch(
               .(CO)             
               ,error=.(TRAP))
    },1:length(map.values),map.keys,map.values)
    .(AF)
  },list(CO=co,BE=before,AF=after,TRAP=err)))
}
