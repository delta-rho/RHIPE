.rhipeEnv <- new.env()
assign("rhipeOptions" ,list(version="_VER_") ,envir=.rhipeEnv )


.onLoad <- function(libname,pkgname){
  require(methods)
  require(rJava)
  cp <- c(list.files(Sys.getenv("HADOOP"),pattern="jar$",full=T),
          list.files(Sys.getenv("HADOOP_LIB"),pattern="jar$",full=T),
          ## list.files(Sys.getenv("SIPE"),pattern="jar$",full=T),
          Sys.getenv("HADOOP_CONF_DIR")
          )

  .jpackage(pkgname, jars="*",morePaths=cp)

  ## .jaddClassPath(cp)
  cfg <- .jnew("org/apache/hadoop/conf/Configuration")
  .jcall(cfg,"V","setClassLoader",.jcast(.jclassLoader(),"java/lang/ClassLoader"))
  opts <- get("rhipeOptions",envir=.rhipeEnv)
  opts$hadoop.cfg <- cfg
  opts$jarloc <- list.files(paste(system.file(package="sipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
  opts$runner <- list.files(paste(system.file(package="sipe"),"libs",.Platform$r_arch,
                                  sep=.Platform$file.sep),pattern="imperious.so",full=T)
  opts$runner <- c("R","CMD", opts$runner,"--slave","--silent","--vanilla")
  opts$fsshell <- .jnew("org/godhuli/rhipe/FileUtils",cfg)
  assign("rhipeOptions",opts,envir=.rhipeEnv)

}
