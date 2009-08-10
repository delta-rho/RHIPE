
##  RHIPE - software that integrates Hadoop mapreduce with R

##   This program is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as published by
##   the Free Software Foundation; either version 2 of the License, or
##   (at your option) any later version.

##   This program is distributed in the hope that it will be useful,
##   but WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.

##   You should have received a copy of the GNU General Public License
##   along with this program; if not, write to the Free Software
##   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

##  Saptarshi Guha sguha@purdue.edu



## ...rdrbytes<- serialize(NULL,NULL)[1:14]
## ...rdrbyteslength <- length(...rdrbytes)
## ...types <- c('list'=1,'integer'=2,'double'=3,'logical'=4,'complex'=5,'character'=6,'raw'=7)
## ...raw0 <- c(as.raw(0xf0),as.raw(0x00))
## ...one <- as.raw(c(0x0,0x0,0x0,0x1))
## rhunser___<- function(X){
##   if(X[1]==0xf0){
##     X[1] <- ...raw0[2]
##     unserialize(c(...rdrbytes,X[1:4],...one,X[-(1:4)]))
##   }else unserialize(c(...rdrbytes,X))
## }
## rhser___<- function(obj,...){
##   x <- serialize(obj,NULL)[-(1:...rdrbyteslength)]
##   ty <- ...types[typeof(obj)]
##   if(!is.na(ty) && length(obj)==1){
##     x[1] <- ...raw0[1]
##     return(x[-c(5,6,7,8)])
##   } else return(x)
## }
...rdrbytes<- serialize(NULL,NULL)[1:14];
...rdrbyteslength <- length(...rdrbytes);
...types <- c('list'=1,'integer'=2,'double'=3,'logical'=4,'complex'=5,'character'=6,'raw'=7);
...one <- as.raw(c(0x0,0x0,0x0,0x1));...what <- as.raw(c(0x00, 0x00,0x10 ,0x09))
rhsz<- function(obj,...){  x <- .Call('R_serialize',obj,NULL,F,NULL,PACKAGE='base')[-(1:...rdrbyteslength)];
                           z <- x
##                            ty <- ...types[typeof(obj)];
##                            flgs <- as.raw(0);
##                            z <- x;
##                            if(F && x[4]==0x10 && x[3]==0x00){ #simplifychar now is F
##                              number <- readBin(x[5:8],'integer',n=1,endian='big');
##                              pos <- 9;
##                              dlt <- vector(mode='integer',length=number);
##                              for(i in 1:number){
##                                dlt[i] <- pos;
##                                pos <- pos+4;
##                                nz <- readBin(x[pos:(pos+3)],'integer',1,endian='big');
##                                pos <- pos+4+max(nz,0);
##                              }
##                              f <- sapply(dlt,function(r) -(r:(r+3))); f <- as.vector(f);
##                              z <- x[f];flgs <- flgs | as.raw(2);
##                           }
##                            if(length(obj)==1){
##                              flgs <- flgs | as.raw(4);
##                              z <- z[-c(5,6,7,8)] ; }
##                            z[1] <- flgs;
                           return(z);}


rhuz<- function(X){
  Z <- X
##   mods <- X[1];
##   X[1] <- as.raw( 0x00);
##   onlyone <- (mods & as.raw(4))>0;
##   ischarsimp <- (mods & as.raw(2))>0;
##   if(onlyone){
##     Z <- c(X[1:4],...one,X[-(1:4)]);
##   }else{
##     Z <- X;
##   }
##   if(ischarsimp) {
##     pos <- 5;
##     num <- readBin(Z[pos:(pos+3)],'integer',1,endian='big');
##     v <- vector('raw',length(Z)+4*num);
##     v[1:8] <- Z[1:8];
##     vpos <- 9;xpos <- 9;
##     for(i in 1:num){
##       v[vpos:(vpos+3)] <- ...what; v[(vpos+4):(vpos+7)] <- Z[xpos:(xpos+3)];
##       lng <- readBin( Z[xpos:(xpos+3)],'integer',1,endian='big');
##       if(lng>0) v[(vpos+8):(vpos+8+lng-1)] <- Z[(xpos+4):(xpos+4+lng-1)];
##       xpos <- xpos+4+max(lng,0);vpos <- vpos+8+max(lng,0);
##     }
##     Z <- v;
##   }
  Z <- c(...rdrbytes,Z);
  .Call('R_unserialize',Z,NULL,PACKAGE='base');
}
## rhunser___ <- function(X){
##   .Call("R_unserialize", c(...rdrbytes,X),NULL, PACKAGE = "base")
## }
## rhser___ <- function(X){
##  .Call("R_serialize", X, NULL, F, NULL, 
##         PACKAGE = "base")[-(1:...rdrbyteslength)]
## }




rhsqallKV <- function(ford,n=-1,verbose=F,ignore.key=T,local=F,...){
  if(class(ford)=="sqreader") rdr <- ford else rdr <- rhsqreader(ford,local,...)
  on.exit({
    if(class(rdr$current.sqob)=="externalptr"){
      rhsqclose(rdr$current.sqob)}
  })
  if(n<0) notgive=T else notgive=F
  h <- vector("list", length=max(1,n)) 
  if(verbose) message(rdr$df[rdr$current])
  i <- 1
  repeat{
    value <-rhsqnextKVR(rdr)
    if(is.null(value)) {
      rdr <- rhsqnextpath(rdr)
      if(is.null(rdr) ) break;
      if(verbose) message(rdr$df[rdr$current])
    }else {
      if(ignore.key) h[[i]]=value$value else h[[i]]=value
      if(i >= n && notgive==F) { rhsqclose(rdr);break;}
      i <- i+1;
    }
  }
  return(h)
}

newReaderR <- function(pth,loc=0){
  cfg <- rhoptions()$hadoop.cfg
  filesystem <- .jcall("org/apache/hadoop/fs/FileSystem","Lorg/apache/hadoop/fs/FileSystem;","get",cfg)
  if(loc==1){
    filesystem <- .jcall("org/apache/hadoop/fs/FileSystem","Lorg/apache/hadoop/fs/FileSystem;","getLocal",cfg)
  }
  path <- .jnew("org/apache/hadoop/fs/Path",pth)
  srr <- .jnew("org/apache/hadoop/io/SequenceFile$Reader",filesystem,path,cfg)
  return(srr)
}
closeReaderR <- function(obj){
  res <- .jcall(obj,"V","close")
  res
}

rhsqclose <- function(sqpt){
  if(class(sqpt)=="sqreader") closeReaderR(sqpt$current.sqob) else closeReaderR(sqpt)
}

nextKVR <- function(sqob){
  key <- .jnew("org/saptarshiguha/rhipe/hadoop/RXWritableRAW")
  value <- .jnew("org/saptarshiguha/rhipe/hadoop/RXWritableRAW")
  res <- .jcall(sqob,"Z","next",.jcast(key,"org.apache.hadoop.io.Writable"),.jcast(value,"org.apache.hadoop.io.Writable"))
  key <- .jcall(key,"[B","get")
  value <- .jcall(value,"[B","get")
  if(!res || length(key)==0 ||length(value)==0) return(NULL)
  else return(list(key=rhuz(key),value=rhuz(value)))
}

rhsqnextKVR <- function(sqro){
  #returns NULL if no more. in a file
  if(class(sqro)!="sqreader") {warning("not a sqreader object"); return(NULL)}
  x <- nextKVR(sqro$current.sqob)
  return(x)
}

rhsqnextpath <- function(sqro){
  ##returns null on error or end of file list
  if(class(sqro)!="sqreader") {warning("not a sqreader object"); return(NULL)}
  if(class(sqro$current.sqob)=="jobjRef") rhsqclose(sqro$current.sqob)
  sqro$current <- sqro$current+1
  if(sqro$current>length(sqro$df)) return(NULL)
  d= newReaderR(sqro$df[sqro$current],sqro$local)  ##.Call("newReaderR",sqro$df[sqro$current],sqro$local)
  if(is.null(d)) {warning(paste("could not open ",sqro$df[sqro$current],":",d));return(NULL)}
  sqro$current.sqob=d
  return(sqro)
}
rhsqreader <- function(ford,local=F,...){
  ##ford can be a filename or a directory
  ##if a directory, then it will iterate through
  ##all files that start with prefix
  lofp <- rhlsd(ford,local,...)[,c("perm","path")]
  if(is.null(lofp) || nrow(lofp)==0) return(NULL);
  ##create a list that will store
  ##current file
  ##data frame of files
  ##skip anything that is a directory
  w=apply(lofp,1,function(r){
    x=r["perm"]
    r2 <- if(length(grep("^d",x))>0)  F else  T
    if(r2) T else F
  })
  lofp=as.character(lofp[w,2])
  if(length(lofp)==0) return(NULL)
  ob <- list(df = lofp, current=0,local=local*1.0,current.sqob=NULL)
  class(ob) <- "sqreader"
  return(rhsqnextpath(ob))
}

rhlsd <- function(ford,local=F,...){
  if(local) return(rhlsd.local(...,ford))
  else return(rhlsd.dfs(ford))
}
.tohperm <- function(m){
  n <- m[1]
  nn <- sapply(1:nchar(n),function(ut) { as.numeric(substr(n,ut,ut))})
  u <- function(j){
    k=rbind(c("-","-","-"),c("-","-","x"),c("-","w","-"),c("-","w","x"),c("r","-","-"),c("r","-","x"),c("r","w","-"),c("r","w","x"))
    return(paste(k[j+1,],sep="",collapse=""))
  }
  mt <- sapply(nn,u)
  if(m[2]) pp="d" else pp="-"
  paste(pp,paste(mt,sep="",collapse=""),sep="")
}
rhlsd.local <- function(ford,pattern="",...){
  dd=list.files(ford,pattern,full=T,...)
  if(length(dd)==0) return(NULL);
  dd1=file.info(dd)
  z=cbind(as.character(dd1[,"mode"]),as.logical(dd1[,"isdir"]))
  perm=apply(z,1,.tohperm)
  dd2=data.frame(perm,dd1[,"uname"],dd1[,"grname"],as.numeric(dd1[,"size"]),as.Date(dd1[,"mtime"],format="%Y-%m-%d %H:%M"),dd)
  colnames(dd2) <- c("perm","owner","group","size","date","path")
  dd2
}

rhlsd.dfs <- function(fold){
  dd=system(paste(paste(Sys.getenv("HADOOP_BIN"),"hadoop",sep=.Platform$file.sep,collapse=""),"dfs","-ls",fold,collapse=" "),intern=T)
  if(length(dd)==0){
    return(NULL);
  }
  numofitems <- 0
  skip=NULL
  if(length(grep("^Found",dd[1]))>0) skip=c(1)
  if(length(grep("^ls",dd[1]))>0) skip=c(1)
  dd2=dd
  if(!is.null(skip)) dd2=dd[-skip]
  r <- sapply(dd2,function(r) {
    x <- strsplit(r," +")[[1]]
    c(x[1:5],paste(x[6],x[7]),x[8])
  })
  colnames(r) <- NULL
  r=t(r)
  d=data.frame(perm=r[,1],bk=r[,2],owner=r[,3],group=r[,4],size=as.numeric(r[,5]),
    date=as.Date(r[,6],format="%Y-%m-%d %H:%M"),path=as.character(r[,7]))
  d
}


cfgget <- function(cfg,option){
  .jcall(cfg,"S","get",option)
}
makenewsq <- function(n,cdc=""){
  cfg <- rhoptions()$hadoop.cfg
  filesystem <- .jcall("org/apache/hadoop/fs/FileSystem","Lorg/apache/hadoop/fs/FileSystem;","get",cfg)
  rww <- .jnew("org.saptarshiguha.rhipe.hadoop.RXWritableRAW")
  rww2 <- .jnew("org.saptarshiguha.rhipe.hadoop.RXWritableRAW")
  path=.jnew("org/apache/hadoop/fs/Path",n)
  swr <- .jnew("org/apache/hadoop/io/SequenceFile$Writer",filesystem,cfg,path, .jcast(rww$getClass(),"java/lang/Class"), .jcast(rww2$getClass(),"java/lang/Class"))
  return(swr)
}
writeKeyValue <- function(sfobj,ke,dv){
  key <- .jnew("org/saptarshiguha/rhipe/hadoop/RXWritableRAW")
  value <- .jnew("org/saptarshiguha/rhipe/hadoop/RXWritableRAW")
  .jcall(key,"V","set",ke)
  .jcall(value,"V","set",dv)
  .jcall(sfobj,"V","append",.jcast(key,"java/lang/Object"),.jcast(value,"java/lang/Object"))
}

rhnewSqfile <- function(l,name,byrow=T,f=NA,pct=0.1){
  rdr = makenewsq(name);
  on.exit({
    if(!is.null(rdr)) closeReaderR(rdr)
  })
  if(is.null(rdr)) stop("could not create sequence file")
  ##Get the range and names
  if((is.matrix(l)||is.data.frame(l)) && byrow) {
    ra <- seq(1,nrow(l))
    k <- rownames(l)
  }
  else if((is.matrix(l)||is.data.frame(l)) && !byrow){
    ra <- seq(1,ncol(l))
    k <- colnames(l)
  }else {
    ra <- seq(1,length(l))  # pass on to the system-independent part

    k<- names(l)
  }
  j1 = seq(0,1,by=pct)
  j2 = cbind( j1, as.integer(max(ra)*j1))
  rownames(j2)=j2[,2]
  nnrownames=as.numeric(j2[,2])
  ##Now add key and values
  lapply(ra, function(r){
    if(r %in% nnrownames) print(paste(r," ",j2[as.character(r),1]*100,"%",sep=""))
      if(is.matrix(l)|is.data.frame(l) && byrow) {
        x1 <- l[r,]
      }else if(is.matrix(l)|is.data.frame(l) && !byrow){
        x1 <- l[,r]
      }else {
        x1 <- l[[r]]
      }
    if(is.function(f))
      rr <- f(x1)
    else
      rr <- x1
    n <- names(r)
    if(is.list(rr) && length(rr)==2
       && "key" %in% n && "value" %in% n){
      dk=rhsz(rr$key,connection=NULL)
      dv=rhsz(rr$value,connection=NULL)
      writeKeyValue(rdr,dk,dv)
    }else{
      dv=rhsz(rr,connection=NULL)
      if(!is.null(k))
        ke <- rhsz(k[r],connection=NULL)
      else ke <- rhsz(r,connection=NULL)
      writeKeyValue(rdr,ke,dv) ##key
    }
    NULL
  })
  
  invisible(NULL)
}

    
##
## rdr <- sqreader("/test/one")
## rdr <- sqstart(rdr)
## while(TRUE){
##   value <-sqnextKVR(rdr)
##   if(is.null(value)) {
##     rdr <- sqnextpath(rdr)
##     if(is.null(rdr)) break;
##   }
##   print(value$key)
## }
