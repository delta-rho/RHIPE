#' Get Absolute HDFS Path
#' 
#' Changes a relative path (a path without a leading /) into an absolute path
#' based on the value of hdfs.getwd().  If it is already an absolute path there 
#' is no change to the returned path.
#' 
#' For all returns, any trailing '/' are removed from the path (if path nchar > 1).
#' 
#' @param paths Path to examine and change to absolute.  Input characters or a list or vector of characters.
#' @param fsExcept Filesystem prefix exceptions that indicate that the path is a fully qualified path name
#' @return Absolute HDFS path corresponding to relative path in the input.  If input is a vector or list returns a vector or list of paths.  Class of elements of return are always character even if NA.
#' @author Jeremiah Rounds
#' @export
rhabsolute.hdfs.path <- function (paths,fsExcept = c("hdfs","s3","s3n","file"))
{
    inpaths <- as.character(unlist(paths))
    ret <- list()
    grepExpr <- sprintf("^(%s)://", paste(fsExcept,collapse="|"))
    wd <- hdfs.getwd()
    for (i in seq_along(inpaths)) {
        p <- inpaths[i]
        if (is.na(p) || length(p) == 0 || nchar(p) == 0 || grepl(grepExpr,p) ) {
            ret[[i]] <- p
            next
        }
        n <- nchar(p)
        if (n > 1 && substr(p, n, n) == "/")
            p <- substr(p, 1, n - 1)
        lead.char <- substr(p, 1, 1)
        if (lead.char == "/") {
            retp <- p
        }
        else {
            if (nchar(wd) == 1) {
                retp <- paste(wd, p, sep = "")
            }
            else {
                retp <- paste(wd, p, sep = "/")
            }
        }
        ret[[i]] <- retp
    }
    if ("list" %in% class(paths))
        return(ret)
    return(as.character(unlist(ret)))
}
