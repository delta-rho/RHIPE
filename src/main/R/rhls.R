#' @export
as.character.ls.nice <- function(x, ...) {
   ml <- c(b = 1, k = 1024, m = 1024^2, g = 1024^3)
   nice <- attr(x, "nice")
   sapply(x, function(b) {
      if (b == 0) 
         return("0")
      if (nice %in% c("b", "k", "m", "g")) 
         formatC(b/ml[nice], digits = 4, format = "fg", big.mark = ",") else {
         if (b < 1024) 
            sprintf("%s bytes", formatC(b, digits = 4, format = "fg", big.mark = ",")) else if (b < 1024^2) 
            sprintf("%s kb", formatC(b/1024, digits = 4, format = "fg", big.mark = ",")) else if (b < 1024^3) 
            sprintf("%s mb", formatC(b/1024^2, digits = 4, format = "fg", big.mark = ",")) else sprintf("%s gb", formatC(b/1024^3, digits = 4, format = "fg", big.mark = ","))
      }
   })
}

#' @export
format.ls.nice <- function(x, ...) {
   as.character.ls.nice(x)
}

#' @export
print.ls.nice <- function(x, ...) cat(sprintf("%s\n", as.character(x)))

#' List Files On HDFS
#'
#' List all files and directories contained in a directory on the HDFS.
#' 
#' @param folder  Path of directory on HDFS or output from rhmr or rhwatch(read=FALSE)
#' @param recurse If TRUE list all files and directories in sub-directories.
#' @param nice One of 'g','m','b' or 'h' (gigabytes, megabytes, bytes, human readable)
#' @author Saptarshi Guha
#' @details Returns a data.frame of filesystem information for the files located
#'   at \code{path}. If \code{recurse} is TRUE, \code{rhls} will recursively
#'   travel the directory tree rooted at \code{path}. The returned object is a
#'   data.frame consisting of the columns: \emph{permission, owner, group, size
#'   (which is numeric), modification time}, and the \emph{file name}.
#'   \code{path} may optionally end in `*' which is the wildcard and will match
#'   any character(s).
#' @return vector of file and directory names
#' @seealso \code{\link{rhput}}, \code{\link{rhdel}},
#'   \code{\link{rhread}}, \code{\link{rhwrite}},
#'   \code{\link{rhsave}}, \code{\link{rhget}}
#' @keywords list HDFS directory
#' @export
rhls <- function(folder = NULL, recurse = FALSE, nice = "h") {
   ## List of files,
   if (is(folder, "rhmr") || is(folder, "rhwatch")) 
      folder <- rhofolder(folder)
   if (is.null(folder)) 
      folder <- hdfs.getwd()
   folder <- rhabsolute.hdfs.path(folder)
   v <- rhoptions()$server$rhls(folder, if (recurse) 
      1L else 0L)
   v <- rhuz(v)
   if (is.null(v)) 
      return(NULL)
   # condition nothing in the directory?
   if (length(v) == 0) {
      f <- as.data.frame(matrix(NA, 0, 6))
   } else {
      f <- as.data.frame(do.call("rbind", sapply(v, strsplit, "\t")), stringsAsFactors = F)
   }
   rownames(f) <- NULL
   colnames(f) <- c("permission", "owner", "group", "size", "modtime", "file")
   f <- unique(f)
   H <- as.numeric(f$size)
   class(H) <- "ls.nice"
   attr(H, "nice") <- nice
   f$size <- H
   f
}


 
