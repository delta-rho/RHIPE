d=read.csv("/tmp/data",stringsAsFactors=F,row.names=NULL,head=T)
getSec <- function(f){
  x=strsplit(f,"m")[[1]]
  mm=as.numeric(x[1])
  ss=as.numeric(strsplit(x[2],"s")[[1]][1])
  mm*60+ss
}

d=t(apply(d,1,function(r) {
  f=strsplit(r," +")[[1]]
  trial <- as.numeric(f[1])
  which <- as.integer(f[2])
  unifsize <- as.integer(f[3])
  realsecs <- getSec(f[5])
  c(trial,which,unifsize,realsecs)
}))

dd <- as.data.frame(d)
colnames(dd) <- c("trial","which","unif","real")
library(lattice)


fd <- dd[dd$which==2,]
ss <- dd[dd$which==0,]
ch <- dd[dd$which==1,]

dd$fd2ss <- fd$real/ss$real
dd$fd2ch <- fd$real/ch$real

xyplot(log(real)~log(unif) | as.factor(trial), group=which, data=dd, type='l',strip = strip.custom(strip.names = TRUE, strip.levels=T,var.name = "Trial"),scales=list(y="free"),auto.key = list(space = "right"))



xyplot(fd2ss ~ log(unif) | as.factor(trial), group=which, data=fd, type='b',strip = strip.custom(strip.names = TRUE, strip.levels=T,var.name = 'Trial'),scales=list(y='free'),auto.key = list(space = 'right'),lty=c(1,8,15))
