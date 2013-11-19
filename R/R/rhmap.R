#' Macro to Wrap Boilerplate Around RHIPE Map code
#'
#' Returns an expression corresponding to given input
#'
#' @param expr Any R expression, that operates on current map.keys, map.values and current index (given by \code{k},\code{r}, and \code{.index} respectively) 
#' @param before An R expression to run before the loop across map.values,map.keys and .index. If map.values is shortened, make map.keys the same length!
#' @param after An R expression to run after the loop. The results of the loop is contained in \code{result}
#' @seealso \code{\link{rhmr}}
#' @keywords MapReduce Map
#' @export
## rhmap <- 
## function(co1=NULL,before=NULL,after=NULL,.fnformals=NULL){
##   ## 
##   if(!is.null(.fnformals)) co <- co1 else co <- substitute(co1);
##   before=substitute(before);after=substitute(after)
##   mu <-function(.index, k,r){}
##   body(mu) <- co
##   if(!is.null(.fnformals)){
##     ## browser()
##     xx <- formals(mu)
##     names(xx) <- append(names(xx)[1], names(.fnformals))
##     formals(mu) <- xx
##     environment(mu) <- .BaseNamespaceEnv
##   }
##   j <- as.expression(bquote({
##     .(BE)
##     result <- mapply(.(F),seq_along(map.values),map.keys,map.values,SIMPLIFY=FALSE)
##     .(AF)
##   },list(F=mu,BE=before,AF=after)))
##   environment(j) <- .BaseNamespaceEnv
##   class(j) <- c(class(j),"rhmr-map")
##   j
## }


rhmap <- 
  function(co1=NULL,before=NULL,after=NULL){
    ## repl <- function(code){
    ##   ..r <- substitute(code)
    ##   r <- if (is(..r, "name")) 
    ##     get(as.character(..r))
    ##   else ..r
    ## }
  co <- substitute(co1); before=substitute(before);after=substitute(after)
  j <- as.expression(bquote({
    .(BE)
    result <- mapply(function(.index,k,r){
      .(CO)
      },seq_along(map.values),map.keys,map.values,SIMPLIFY=FALSE)
    .(AF)
  },list(CO=co,BE=before,AF=after)))
  environment(j) <- .BaseNamespaceEnv
  class(j) <- c(class(j),"rhmr-map")
  j
}



