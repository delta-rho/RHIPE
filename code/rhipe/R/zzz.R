.rhipeEnv <- new.env()
assign("rhipeOptions", list(rport =  8888,version="_VER_") ,envir=.rhipeEnv  )

.onLoad <- function(libname, pkgname) {
  require(methods)
  require(rJava)
  if(Sys.getenv("HADOOP_LIB")=="") stop("Missing HADOOP_LIB environment variable")
  if(Sys.getenv("HADOOP_CONF_DIR")=="") stop("Missing HADOOP_CONF_DIR environment variable")

  if(Sys.getenv("RHIPE")=="") stop("Missing $RHIPE environment variable")
  cp <- c(list.files(paste(Sys.getenv("HADOOP_LIB"),"/lib",sep=""),pattern="jar$",full=T),
          list.files(Sys.getenv("HADOOP_LIB"),pattern="jar$",full=T),
          Sys.getenv("HADOOP_CONF_DIR"),
          list.files(paste(Sys.getenv("RHIPE"),sep=""),pattern="jar$",full=T))
  tryCatch(.jinit(),error=function(r){},finally=function(r){}) ##classpath=cp)
  .jaddClassPath(cp)
  cfg <- .jnew("org/apache/hadoop/conf/Configuration")
  .jcall(cfg,"V","setClassLoader",.jcast(.jclassLoader(),"java/lang/ClassLoader"))
  opts <- get("rhipeOptions",envir=.rhipeEnv)
  opts$hadoop.cfg <- cfg
  assign("rhipeOptions",opts,envir=.rhipeEnv)
}

##--quiet","--no-save","--max-nsize=1G","--max-ppsize=100000

##--quiet --no-save --max-nsize=1G --max-ppsize=100000 --RS-port 8888
