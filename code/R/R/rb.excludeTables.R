#' Excludes some tables from any write access since HBase
#' @param ban the regular expression corresponding to the banned tables
#' @details this cannot be modified by the user (at least not at the moment)
rb.excludeTables <- function(r,ban= c("^(c|t|m)")){
  if(grepl(ban, r)) stop("Cannot create/add/delete/drop modify that table")
}
