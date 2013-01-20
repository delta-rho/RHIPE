
mkdtemp <- function(indir){
  .Call("createTempDir",sprintf("%s/rhipedir-XXXXXX",indir),PACKAGE="Rhipe")
}


initPRNG <- function(seed=NULL){
  seed <- eval(seed)
  mi <- function(){
    getUID <- function(id = Sys.getenv("mapred.task.id")) {
      a <- strsplit(id, "_")[[1]]
      a <- as.numeric(a[c(2, 3, 5)])
    }
    library(parallel)
    RNGkind("L'Ecuyer-CMRG")
    if(!is.null(iseed)) set.seed(iseed)
    current.task.number <- getUID()[3]
    seed <- .Random.seed
    if (current.task.number > 1) {
      for (i in 2:current.task.number) seed <- nextRNGStream(seed)
    }
    RNGkind("default")
    assign(".Random.seed", seed, envir = .GlobalEnv)
    rhcounter("Seed",paste(.Random.seed,collapse=":"),1)
  }
  e1 <- new.env(parent = .BaseNamespaceEnv)
  assign("iseed",seed,envir=e1)
  environment(mi) <- e1
  mi
}

  




