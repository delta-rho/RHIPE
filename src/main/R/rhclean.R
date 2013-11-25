#' Cleans the RHIPE temporary folder of rhipe-temp files
#'
#' Does not return anything
#'
#' @param temp the temporary folder, defaults to rhoptions()$HADOOP.TMP.FOLDER
#' @export
rhclean <- function(temp=rhoptions()$HADOOP.TMP.FOLDER){
  if(is.null(temp))
    stop("No Temporary Folder Provided")
  else {
    files <- rhls(rhoptions()$HADOOP.TMP.FOLDER)$file
    files <- files[grepl("/rhipe-temp-",files)]
    if(length(files)>0){
      rhdel(files)
      message(sprintf("Deleted %s file%s",length(files),if(length(files)>1) "s" else ""))
    } else {
      message("Nothing to delete")
    }
  }
}
