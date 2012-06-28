#' Retrieves errors from a job when debug=='collect' in rhwatch
#'
#' Retrieves errors from a job when debug=='collect' in rhwatch
#' @param job is the result of rhwatch (when read=FALSE) or rhmr
#' @param prefix is the prefix string of the debug files
#' @param num.file is the number of debug files to read
#' @author Saptarshi Guha
#' @export
rherrors <- function(job,prefix="rhipe_debug",num.file=1){
  files <- rhls(rhofolder(job))$file
  files <- files[grepl(prefix,files)][1:num.file]
  y <- new.env()
  unlist(lapply(files, function(r){
    rhload(r,envir=y)
    y$rhipe.errors
  }),recursive=FALSE)
}
