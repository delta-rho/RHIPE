rhextract <-  function(alist, what="keys",unlist=FALSE,lapply=lapply,...){
  what <- what[pmatch(what,c("keys","values"))]
  j <- if(what=="keys") lapply( alist,"[[",1) else lapply(alist, "[[",2)
  if(unlist) unlist(j, ...) else j
} # lapply could be mclapply


mkdtemp <- function(indir){
  .Call("createTempDir",sprintf("%s/rhipedir-XXXXXX",indir),PACKAGE="Rhipe")
}

summer <- Rhipe::rhoptions()$templates$scalarsummer

  



