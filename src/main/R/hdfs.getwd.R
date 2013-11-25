#' Get HDFS Working Directory
#'
#' Returns the current HDFS working directory used for relative paths in Rhipe.
#' Take a look at \code{\link{hdfs.getwd}} for more information.
#' @author Jeremiah Rounds
#' @seealso \code{\link{hdfs.getwd}}
#' @export
hdfs.getwd = function(){
	return(rhoptions()$hdfs.working.dir)
}
