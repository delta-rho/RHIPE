#' Change Permissions of Files on HDFS
#'
#' Change permissions of files on HDFS
#'
#' @param path is the location of the directory to be created. It is expanded using \code{\link{rhabsolute.hdfs.path}}
#' @param permissions a character string such as '777'
#' @author Ryan Hafen
#' @seealso \code{\link{rhwrite}}, \code{\link{rhmkdir}}
#' @export
rhchmod <- function(path, permissions) {
   path <- rhabsolute.hdfs.path(path)
   path <- .jnew("org/apache/hadoop/fs/Path", path)
   fs <- path$getFileSystem(rhoptions()$clz$config)
   a <- .jnew("org/apache/hadoop/fs/permission/FsPermission", as.character(permissions))

   fs$setPermission(path, a)
}

