#' Creates Directories on the HDFS
#'
#' Equivalent of mkdir -p  in Unix
#' 
#' @param path is the location of the directory to be created. It is expanded using \code{\link{rhabsolute.hdfs.path}}
#' @param permissions either of the integer form e.g. 777 or string e.g. rwx. If missing the default is used.
#' @author Saptarshi Guha
#' @return Logical TRUE if success
#' @seealso \code{\link{rhcp}}, \code{\link{rhmv}},
#' @keywords mkdir HDFS file
#' @export
rhmkdir <- function(path,permissions){
  src = rhabsolute.hdfs.path(path)
  fs <- rhoptions()$clz$filesystem
  path <- .jnew("org/apache/hadoop/fs/Path",path)
  if(missing(permissions))
    fs$mkdirs(path)
  else if(is.integer(permissions) || is.numeric(permissions))
    fs$mkdirs(path, .jnew("org/apache/hadoop/fs/permission/FsPermission"
                          ,.jshort(as.integer(permissions))))
  else if(is.character(permissions))
    fs$mkdirs(path, .jnew("org/apache/hadoop/fs/permission/FsPermission"
                          ,permissions))
}
