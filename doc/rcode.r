
## Converting Text to R Objects
setup <- expression({
  convertHHMM <- function(s){
    t(sapply(s,function(r){
      l=nchar(r)
      if(l==4) c(substr(r,1,2),substr(r,3,4))
      else if(l==3) c(substr(r,1,1),substr(r,2,3))
      else c('0','0')
    })
  )}
})
map <- expression({
  y <- do.call("rbind",lapply(map.values,function(r){
    if(substr(r,1,4)!='Year') strsplit(r,",")[[1]]
  }))
  mu <- rep(1,nrow(y));yr <- y[,1]; mn=y[,2];dy=y[,3]

  hr <- convertHHMM(y[,5])
  depart <- ISOdatetime(year=yr,month=mn,day=dy,hour=hr[,1],min=hr[,2],sec=mu)
  hr <- convertHHMM(y[,6])
  sdepart <- ISOdatetime(year=yr,month=mn,day=dy,hour=hr[,1],min=hr[,2],sec=mu)
  hr <- convertHHMM(y[,7])
  arrive <- ISOdatetime(year=yr,month=mn,day=dy,hour=hr[,1],min=hr[,2],sec=mu)
  hr <- convertHHMM(y[,8])
  sarrive <- ISOdatetime(year=yr,month=mn,day=dy,hour=hr[,1],min=hr[,2],sec=mu)
  d <- data.frame(depart= depart,sdepart = sdepart
                  ,arrive = arrive,sarrive =sarrive
                  ,carrier = y[,9],origin = y[,17]
                  ,dest=y[,18],dist = y[,19], year=yr, month-mn, day=dy
                  ,cancelled=y[,22], stringsAsFactors=FALSE)
  d <- d[order(d$sdepart),]
  rhcollect(d[c(1,nrow(d)),"sdepart"],d)
})
reduce <- expression(
    reduce = {
      lapply(reduce.values,function(i)
             rhcollect(reduce.key,i))}
    )
mapred <- list(rhipe_map_buff_size=5000)
mapred$rhipe_map_output_keyclass='org.godhuli.rhipe.RHNumeric'
z <- rhmr(map=map,reduce=reduce,setup=setup,inout=c("text","sequence")
          ,ifolder="/airline/data/",ofolder="/airline/blocks",mapred=mapred)
rhex(z)

a=rhread("/airline/blocks",max=10)


## Create Southwest Database

map <- expression({
  h <- do.call("rbind",map.values)
  d <- h[h$carrier=='WN',,drop=FALSE]
  if(nrow(d)>0){
    sp <- apply(d[,c("year","month","mday")],1,function(r) paste(r,collapse=" "))
    e <- split(d,sp)
    lapply(e,function(r){
      k <- r[1,c("year","month","mday")]
      rhcollect(k, r)
    })
  }   
})

reduce <- expression(
    pre = { collec <- NULL },
    reduce = {
      collec <- rbind(collec, do.call("rbind",reduce.values))
      collec <- collec[order(collec$depart),]
    },
    post = {
      rhcollect(as.vector(unlist(reduce.key)), collec)
    }
    )
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","map")
          ,ifolder="/airline/blocks/",ofolder="/airline/southwest"
          ,mapred=list(rhipe_map_buff_size=10))
rhex(z)


## Busiest Cities
map <- expression({
  a <- do.call("rbind",map.values)
  inbound <- table(a[,'origin'])
  outbound <- table(a[,'dest'])
  total <- table(unlist(c(a[,'origin'],a['dest'])))
  for(n in names(total)){
    inb <- if(is.na(inbound[n])) 0 else inbound[n] 
    ob <- if(is.na(outbound[n])) 0 else outbound[n]
    rhcollect(n, c(inb,ob, total[n]))
  }
})
reduce <- expression(
    pre={sums <- c(0,0,0)},
    reduce = {
      sums <- sums+apply(do.call("rbind",reduce.values),2,sum)
    },
    post = {
      rhcollect(reduce.key, sums)
    }
    )
## k=NULL
## for( i in c(5,10,15,20,25,125)){
##   for(j in 1:3){
mapred <- list()
mapred$rhipe_map_buff_size <- 20
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/volume"
          ,mapred=mapred)
z=rhex(z)
## k=rbind(k,c(i,j,z$counters$job_time))
## }
## }
counts <- rhread("/airline/volume")

pdf("~/tmp/rhipemapbuff.pdf")
g <- tapply(k[,3],k[,1],mean)
xyplot(log(g,10)~log(as.numeric(names(g)),10),xlab='Log rhipe_map_buffsize',ylab='Log Time (sec)',type='b',col='black')
dev.off()



ap <- read.table("~/tmp/airports.csv",sep=",",header=TRUE,stringsAsFactors=FALSE)
aircode <- unlist(lapply(counts, "[[",1))
count <- do.call("rbind",lapply(counts,"[[",2))
results <- data.frame(aircode=aircode,
                      inb=count[,1],oub=count[,2],all=count[,3]
                      ,stringsAsFactors=FALSE)
results <- results[order(results$all,decreasing=TRUE),]
results$airport <- sapply(results$aircode,function(r){
  nam <- ap[ap$iata==r,'airport']
  if(length(nam)==0) r else nam
})
airport=results
library(lattice)
r <- results[1:20,]
af <- reorder(r$airport,r$all)
pdf("~/tmp/volume.pdf")
dotplot(af~log(r[,'all'],10),xlab='Log_10 Total Volume',ylab='Airport',col='black')
dev.off()

###
## Carrier Popularity
## Number of flights conditioned on carrier and year
map <- expression({
  a <- do.call("rbind",map.values)
  total <- table(years=a[,'year'],a[,'carrier'])
  ac <- rownames(total)
  ys <- colnames(total)
  for(yer in ac){
    for(ca in ys){
      if(total[yer,ca]>0) rhcollect(c(yer,ca), total[yer,ca])
    }
  }
})
reduce <- expression(
    pre={sums <- 0},
    reduce = {sums <- sums+sum(do.call("rbind",reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )

mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/carrier.pop"
          ,mapred=mapred)
z=rhex(z)

a=rhread("/airline/carrier.pop")
yr <- as.numeric(unlist(lapply(lapply(a,"[[",1),"[[",1)))
carrier <- unlist(lapply(lapply(a,"[[",1),"[[",2))
count <- unlist(lapply(a,"[[",2))
results <- data.frame(yr=yr,carcode=carrier,count=count,stringsAsFactors=FALSE)
results <- results[order(results$yr,results$count,decreasing=TRUE),]
carrier <- read.table("~/tmp/carriers.csv",sep=",",header=TRUE,
                      stringsAsFactors=FALSE,na.strings="XYZB")
results$carrier <- sapply(results$carcode,function(r){
  cd <- carrier[carrier$Code==r,'Description']
  if(is.na(cd)) r else cd
})
results$yr <- results$yr+1900
carr <- reorder(results$carrier,results$count, median)
pdf("~/tmp/carrierpop.pdf")
xyplot(log(count,10)~yr|carr, data=results,xlab="Years", ylab="Log10 count",col='black'
        ,scales=list(scale='free',tck=0.5,cex=0.7),layout=c(2,8),type='l'
       ,par.strip.text = list(lines = 0.8,cex=0.7),cex=0.5,aspect="fill",
       panel=function(...){
         panel.grid(h=-1,v=-1)
         panel.xyplot(...)
       })
dev.off()

## Flights delayed by 15 minutes, proportions by day

map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  e <- split(a,list(a$year,a$yday))
  lapply(e,function(r){
    n <- nrow(r); numdelayed <- sum(r$isdelayed)
    rhcollect(as.vector(unlist(c(r[1,c("year","yday")]))), c(n, numdelayed))
  })
})
reduce <- expression(
    pre={sums <- c(0,0)},
    reduce = {sums <- sums+apply(do.call("rbind",reduce.values),2,sum)},
    post = { rhcollect(reduce.key, sums) }
    )

mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/delaybyyear"
          ,mapred=mapred)
z=rhex(z)

b <- rhread("/airline/delaybyyear")
y1 <- do.call("rbind",lapply(b,"[[",1))
y2 <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(year=1900+y1[,1],yday=y1[,2],
                      nflight=y2[,1],ndelay=y2[,2])
results$prop <- results$ndelay/results$nflight
results <- results[order(results$year,results$yday),]

pdf("~/tmp/propdelayed.pdf")
xyplot(prop~yday|year, data=results,type='h',col='black',
       xlab='Day of year',ylab="Proportion of Flights Delayed > 15 minutes",
       par.strip.text = list(lines = 0.8,cex=0.7),strip = FALSE,
       cex=0.5, scales=list(tck=0.5,y=list(cex=0.5)),
       layout=c(1,11),strip.left=TRUE,panel=function(...){
         panel.grid(v=52,h=-1,col=c('#eeeeee','#333333'));panel.xyplot(...)
       })
dev.off()


prop <- results$prop
prop <- prop[!is.na(prop)]
tprop <- ts(prop,start=c(1987,273),frequency=365)
pdf("~/tmp/propdelayedxyplot.pdf")
plot(stl(tprop,s.window="periodic"))
dev.off()


tprop <- ts(prop,start=c(1987,273),frequency=7)
plot(stl(tprop,s.window=31))





## Day of week delays
map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  e <- split(a,a$wday)
  lapply(e,function(r){
    n <- nrow(r); numdelayed <- sum(r$isdelayed)
    rhcollect(as.vector(unlist(c(r[1,c("wday")]))), c(n, numdelayed))
  })
})
reduce <- expression(
    pre={sums <- c(0,0)},
    reduce = {sums <- sums+apply(do.call("rbind",reduce.values),2,sum)},
    post = { rhcollect(reduce.key, sums) }
    )

mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/delaybydow"
          ,mapred=mapred)
z=rhex(z)

b <- rhread("/airline/delaybydow")
y1 <- do.call("rbind",lapply(b,"[[",1))
y2 <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(dow=y1[,1],
                      nflight=y2[,1],ndelay=y2[,2])
results <- results[order(results$dow),]
results$prop <- results$ndelay/results$nflight
results$dow <- factor(results$dow,labels=c("Sun","Mon","Tue","Wed","Thu",'Fri','Sat'))
pdf("~/tmp/propdelayeddow.pdf")
dotplot(dow~prop, data=results,col='black',
       ylab='Day of Week',xlab="Proportion of Flights Delayed > 15 minutes")
dev.off()


## Quantiles for Delays by month
## USE THIS FOR DEBUG EXAMPLE
## CHANGE a[[a$isdelayed==TRUE] to a[isdelayed,]
map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  a <- a[a$isdelayed==TRUE,] ## only look at delays greater than 15 minutes
  apply(a[,c('month','delay.sec')],1,function(r){
        k <- as.vector(unlist(r))
        if(!is.na(k[1])) rhcollect(k,1) # ignore cases where month is missing
      })
})
reduce <- expression(
    pre={sums <- 0} ,
    reduce = {sums <- sums+sum(unlist(reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )


a <- do.call("rbind",lapply(rhread("/airline/blocks",max=1),"[[",2))
mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/quantiledelay"
          ,mapred=mapred)
z=rhex(z)
b <- rhread("/airline/quantiledelay")
y1 <- do.call("rbind",lapply(b,"[[",1))
count <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(month = y1[,1], n=y1[,2], count=count)
results <- results[order(results$month, results$n),]
results.2 <- split(results, results$month)

discrete.quantile<-function(x,n,prob=seq(0,1,0.25),type=7){
  sum.n<-sum(n)
  cum.n<-cumsum(n)
  np<-if(type==7) (sum.n-1)*prob+1 else sum.n*prob+0.5
  np.fl<-floor(np)
  j1<-pmax(np.fl,1)
  j2<-pmin(np.fl+1,sum.n)
  gamma<-np-np.fl
  id1<-unlist(lapply(j1,function(r) seq_along(cum.n)[r<=cum.n][1]))
  id2<-unlist(lapply(j2,function(r) seq_along(cum.n)[r<=cum.n][1]))
  x1<-x[id1]
  x2<-x[id2]
  qntl<-(1-gamma)*x1+gamma*x2
  qntl
}

DEL <- 0.05
results.3 <- lapply(seq_along(results.2),function(i){
  r <- results.2[[i]]
  a <- discrete.quantile(r[,2],r[,3],prob=seq(0,1,DEL))/60
  data.frame(month=as.numeric(rep(names(results.2)[[i]],length(a))),prop=seq(0,1,DEL),qt=a)
})
results.3 <- do.call("rbind",results.3)
results.3$month <- factor(results.3$month,
                          label=c("Jan","Feb","March","Apr","May","June",
                            "July","August","September","October","November","December"))
pdf("~/tmp/quantiles_by_month.pdf")
xyplot(log(qt,2)~prop|month, data=results.3,cex=0.4,col='black',
       scales=list(x=list(tick.number=10),y=list(tick.number=10)),
       layout=c(4,3),type='l',
       xlab="Proportion",ylab="log_2 delay (minutes)",panel=function(x,y,...){
         panel.grid(h=-1,v=-1);panel.xyplot(x,y,...)
       }
)
dev.off()


a <- do.call("rbind",lapply(rhread("/airline/blocks",max=1),"[[",2))



##delay proportion by hours of the day
map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  a$hrs <- as.numeric(format(a[,'sdepart'],"%H"))
  e <- split(a,a$hrs)
  lapply(e,function(r){
    n <- nrow(r); numdelayed <- sum(r$isdelayed)
    rhcollect(as.vector(unlist(c(r[1,c("hrs")]))), c(n, numdelayed))
  })
})
reduce <- expression(
    pre={sums <- c(0,0)},
    reduce = {sums <- sums+apply(do.call("rbind",reduce.values),2,sum)},
    post = { rhcollect(reduce.key, sums) }
    )


mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/delaybyhours"
          ,mapred=mapred)
z=rhex(z)

b <- rhread("/airline/delaybyhours")
y1 <- do.call("rbind",lapply(b,"[[",1))
y2 <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(hod=y1[,1],
                      nflight=y2[,1],ndelay=y2[,2])
results[results$hod==0,'hod'] <- 24
results <- results[order(results$hod),]
results$prop <- results$ndelay/results$nflight
pdf("~/tmp/propdelaybyhour.pdf")
dotplot(hod~prop, data=results,col='black',
       ylab='Hour of Day',xlab="Proportion of Flights Delayed > 15 minutes")
dev.off()









## Quantile delay by hours
map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  a <- a[a$isdelayed==TRUE,] ## only look at delays greater than 15 minutes
  a$hrs <- as.numeric(format(a[,'sdepart'],"%H"))
  apply(a[,c('hrs','delay.sec')],1,function(r){
        k <- as.vector(unlist(r))
        if(!is.na(k[1])) rhcollect(k,1) 
      })
})
reduce <- expression(
    pre={sums <- 0} ,
    reduce = {sums <- sums+sum(unlist(reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )

mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/quantiledelaybyhour"
          ,mapred=mapred)
z=rhex(z)
b <- rhread("/airline/quantiledelaybyhour")
y1 <- do.call("rbind",lapply(b,"[[",1))
count <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(hr = y1[,1], n=y1[,2], count=count)
for(i in 1:nrow(results)) if(results[i,"hr"]==0) results[i,"hr"] <- 24
results <- results[order(results$hr, results$n),]
results.2 <- split(results, results$hr)

DEL <- 0.05
results.3 <- lapply(seq_along(results.2),function(i){
  r <- results.2[[i]]
  a <- discrete.quantile(r[,2],r[,3],prob=seq(0,1,DEL))/60
  data.frame(hr=as.numeric(rep(names(results.2)[[i]],length(a))),prop=seq(0,1,DEL),qt=a)
})
results.3 <- do.call("rbind",results.3)
pdf("~/tmp/quantiles_by_hr.pdf")
xyplot(log(qt,2)~prop|factor(hr), data=results.3,cex=0.4,col='black',
       scales=list(x=list(tick.number=10),y=list(tick.number=10)),
       layout=c(4,3),type='l',
       xlab="Proportion",ylab="log_2 delay (minutes)",panel=function(x,y,...){
         panel.grid(h=-1,v=-1);panel.xyplot(x,y,...)
       }
)
dev.off()



a <- do.call("rbind",lapply(rhread("/airline/blocks",max=1),"[[",2))

## Quantile delay by by airport, inbound and outbound
map <- expression({
  cc <- c("ORD","SEA","DFW","SFO")
  a <- do.call("rbind",map.values)
  a <- a[a$origin %in% cc| a$dest %in% cc,]
  if(nrow(a)>0){
    a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
    a <- a[!is.na(a$delay.sec),]
    a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
    a <- a[a$isdelayed==TRUE,]
    for(i in 1:nrow(a)){
      dl <- a[i,'delay.sec']
      if(a[i,'origin'] %in% cc) rhcollect(data.frame(dir="outbound",ap=a[i,"origin"]
                                                     ,delay=dl,stringsAsFactors=FALSE),1)
      if(a[i,'dest'] %in% cc) rhcollect(data.frame(dir="inbound",ap=a[i,"dest"]
                                              ,delay=dl,stringsAsFactors=FALSE),1)
    }
  }
})

reduce <- expression(
    pre={sums <- 0} ,
    reduce = {sums <- sums+sum(unlist(reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )

mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/inoutboundelay"
          ,mapred=mapred)
z=rhex(z)

b <- rhread("/airline/inoutboundelay")
results <- do.call("rbind",lapply(b,"[[",1))
results$count <- unlist(lapply(b,"[[",2))
results <- results[order(results$ap,results$dir,results$delay),]
results.2 <- split(results,list(results$ap,results$dir))

DEL=0.01
results.3 <- lapply(seq_along(results.2),function(i){
  r <- results.2[[i]]
  a <- discrete.quantile(r[,3],r[,4],prob=seq(0,1,DEL))/60
  x1 <- strsplit(names(results.2)[[i]],".",fixed=TRUE)[[1]]
  data.frame(dir=rep(x1[2],length(a)),ap=rep(x1[1],length(a)),prop=seq(0,1,DEL),qt=a)
  ## a
})
results.3 <- do.call("rbind",results.3)

results.3$apf <- reorder(results.3$ap,results.3$qt,mean)
pdf("~/tmp/quantiles_by_airport.pdf")
xyplot(log(qt,2)~prop|apf,group=dir, data=results.3,cex=0.4,auto.key=TRUE,
       scales=list(x=list(tick.number=10),y=list(tick.number=10)),
       ,type='l',
       xlab="Proportion",ylab="log_2 delay (minutes)",panel=function(x,y,groups,...){
         panel.grid(h=-1,v=-1);panel.xyplot(x,y,groups,...);
         panel.lines(c(0.25,0.25),c(0,20),col='#F78D8D',lty=2)
         panel.lines(c(0.5,0.5),c(0,20),col='#F78D8D',lty=2)
         panel.lines(c(0.75,0.75),c(0,20),col='#F78D8D',lty=2)
       }
)
dev.off()

inbound for DFW higher 0.7 == blue





## Carrier Prop Delay
map <- expression({
  a <- do.call("rbind",map.values)
  a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
  rhcounter("MISSING","1",sum(is.na(a$delay.sec)))
  a <- a[!is.na(a$delay.sec),]
  a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
  e <- split(a,a$carrier)
  lapply(e,function(r){
    n <- nrow(r); numdelayed <- sum(r$isdelayed)
    rhcollect(as.vector(unlist(c(r[1,c("carrier")]))), c(n, numdelayed))
  })
})
reduce <- expression(
    pre={sums <- c(0,0)},
    reduce = {sums <- sums+apply(do.call("rbind",reduce.values),2,sum)},
    post = { rhcollect(reduce.key, sums) }
    )


mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/carrierdelayprop"
          ,mapred=mapred)
z=rhex(z)

b <- rhread("/airline/carrierdelayprop")
y1 <- do.call("rbind",lapply(b,"[[",1))
y2 <- do.call("rbind",lapply(b,"[[",2))
results <- data.frame(ca=y1[,1],
                      nflight=y2[,1],ndelay=y2[,2])
results$prop <- results$ndelay/results$nflight
results <- results[order(results$prop),]
results$carrier <- sapply(results$ca,function(r){
  cd <- carrier[carrier$Code==r,'Description']
  if(is.na(cd) | nchar(cd)>30) substr(r,1,20) else cd
})
results.2 <- results #[1:20,]
carr <- reorder(results.2$carrier,results.2$prop, median)

pdf("~/tmp/carrdelay.pdf")
dotplot(carr~prop, data=results.2,col='black',scales=list(y=list(cex=0.5,tck=0.5))
        ,ylab='Carrier',xlab="Proportion of Flights Delayed > 15 minutes")
dev.off()


## Carrier Volume
carr <- reorder(results.2$carrier,results.2$nflight, median)

pdf("~/tmp/carrvol.pdf")
dotplot(carr~log(nflight,2), data=results.2,col='black',scales=list(x=list(tick.number=10),y=list(cex=0.5,tck=0.5))
        ,ylab='Carrier',xlab="Log_2 Volume")
dev.off()

pdf("~/tmp/carrvoldelayscatter.pdf")
xyplot(prop~log(nflight,2), data=results.2,col='black',
        ,ylab='Carrier',xlab="Log_2 Volume")
dev.off()




## Carrier Prop Delay by Years
## Not used
## map <- expression({
##   a <- do.call("rbind",map.values)
##   a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
##   a <- a[!is.na(a$delay.sec),]
##   a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
##   rhcounter("MISSING","YEARS", sum(is.na(a$year)))
##   rhcounter("MISSING","YEARS", sum(is.na(a$carrier)))
##   e <- split(a,list(a$year,a$carrier))
##   lapply(e,function(r){
##     n <- nrow(r); numdelayed <- sum(r$isdelayed)
##     ## ca <- as.vector(unlist(r[1,c("carrier")]))
##     ## yr <- as.vector(unlist(r[1,c("year")]))
##     rhcollect(r[1,c("carrier","year")], c(n, numdelayed))
##   })
## })
## reduce <- expression(
##     pre={sums <- c(0,0)},
##     reduce = {sums <- sums+apply(do.call("rbind",reduce.values),2,sum)},
##     post = { rhcollect(reduce.key, sums) }
##     )
## mapred <- list()
## mapred$rhipe_map_buff_size <- 5
## z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
##           ,ifolder="/airline/blocks/",ofolder="/airline/carrdelayyear"
##           ,mapred=mapred)
## z=rhex(z)

## b <- rhread("/airline/carrdelayyear")
## y1 <- do.call("rbind",lapply(b,"[[",1))
## y2 <- do.call("rbind",lapply(b,"[[",2))
## results <- data.frame(ca=y1[,1],year=y1[,2],
##                       nflight=y2[,1],ndelay=y2[,2])
## results$prop <- results$ndelay/results$nflight
## sum(t(tapply(results$nflight,results$ca,sum)))

## Count of (i,j)
map <- expression({
  a <- do.call("rbind",map.values)
  y <- table(apply(a[,c("origin","dest")],1,function(r){
    paste(sort(r),collapse=",")
  }))
  for(i in 1:length(y)){
    p <- strsplit(names(y)[[i]],",")[[1]]
    rhcollect(p,y[[1]])
  }
})
reduce <- expression(
    pre={sums <- 0},
    reduce = {sums <- sums+sum(unlist(reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )
mapred <- list()
mapred$rhipe_map_buff_size <- 5
mapred$mapred.job.priority="VERY_LOW"
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/ijjoin"
          ,mapred=mapred)
z=rhex(z)

b=rhread("/airline/ijjoin")
y <- do.call("rbind",lapply(b,"[[",1))
results <- data.frame(a=y[,1],b=y[,2],count=
           do.call("rbind",lapply(b,"[[",2)),stringsAsFactors=FALSE)
results <- results[order(results$count,decreasing=TRUE),]
results$cumprop <- cumsum(results$count)/sum(results$count)
a.lat <- t(sapply(results$a,function(r){
  ap[ap$iata==r,c('lat','long')]
}))
results$a.lat <- unlist(a.lat[,'lat'])
results$a.long <- unlist(a.lat[,'long'])
b.lat <- t(sapply(results$b,function(r){
  ap[ap$iata==r,c('lat','long')]
}))
b.lat["CBM",] <- c(0,0)
results$b.lat <- unlist(b.lat[,'lat'])
results$b.long <- unlist(b.lat[,'long'])

save(results,airport,file="~/tmp/a.Rdata")


airport$p <- cumsum(airport$all)/sum(airport$all)
rownames(airport) <- NULL
P <- 0.90
cuto <- rev(which(airport$p <= P))[1]

airport2 <- merge(airport,ap[,c("airport","lat","long")], by="airport")
library(maps)
mai <- airport2[1:cuto,];nmai <- airport2[(cuto+1):nrow(airport),]

## pop <- results[results$a %in% mai$aircode | results$b %in% mai$aircode,] ##mai are popular ones
pop <- results[results$a %in% "ORD" | results$b %in% "ORD",] ##mai are popular ones
pop$prop <- pop$count/sum(pop$count)
pop$pc <- cumsum(pop$count)/sum(pop$count)
NC <- 4
pop$cuts <- cut(pop$pc,NC);library(RColorBrewer)
cols <- brewer.pal(NC,"Spectral")
display.brewer.pal(NC, "Spectral")
cols <- sapply(seq_along(cols),function(r) sprintf("%s%s",cols[[r]], as.integer(seq(95,30,length=NC))[r]))
PL=TRUE
if(PL) pdf("~/mystuff/research/largedata/rmanual/images/flightord.pdf")

map("state", interior = FALSE);map("state", boundary = FALSE, col="gray", add = TRUE)
points(x=mai$long,y=mai$lat,pch=16,cex=0.8,col="#000000")
points(x=nmai$long,y=nmai$lat,pch=16,cex=0.4,col="#aaaaaa")
for(i in 1:nrow(pop)){ #nrow(pop)){
  lines(x=c(pop[i,"a.long"],pop[i,"b.long"]),y=c(pop[i,"a.lat"],pop[i,"b.lat"]),col=cols[pop[i,'cuts']],lwd=1.4)
} #50.1
legend(x=-125,y=27.1,box.lwd=0,horiz=TRUE,inset=0.05,legend=c("Top 25%","Upto 50%","Upto 75%","Bottom 25%"),fill=cols,cex=0.9)

if(PL) dev.off()
## cut(mai$p,10)
## grey.colors
## points(x=results$b.long,y=results$b.lat,col='#0000ff50',cex=0.5)


## DEBUG
map <- expression({
  tryCatch({
    a <- do.call("rbind",map.values)
    a$delay.sec <- as.vector(a[,'arrive'])-as.vector(a[,'sarrive'])
    a <- a[!is.na(a$delay.sec),]
    a$isdelayed <- sapply(a$delay.sec,function(r) if(r>=900) TRUE else FALSE)
    a <- a[isdelayed==TRUE,] ## only look at delays greater than 15 minutes
    apply(a[,c('month','delay.sec')],1,function(r){
      k <- as.vector(unlist(r))
      if(!is.na(k[1])) rhcollect(k,1) # ignore cases where month is missing
    })
  },error=function(e){
    e$message <- sprintf("Input File:%s\n Attempt ID:%s\nR INFO:%s",
                 Sys.getenv("map.input.file"),Sys.getenv("mapred.task.id"),e$message)
    stop(e) ## WONT STOP OTHERWISE
  })
})
  reduce <- expression(
    pre={sums <- 0} ,
    reduce = {sums <- sums+sum(unlist(reduce.values))},
    post = { rhcollect(reduce.key, sums) }
    )
mapred <- list()
mapred$rhipe_map_buff_size <- 5
z <- rhmr(map=map,reduce=reduce,combiner=TRUE,inout=c("sequence","sequence")
          ,ifolder="/airline/blocks/",ofolder="/airline/quantiledelay"
          ,mapred=mapred)
z=rhex(z)



map <- expression({
  lapply(seq_along(map.values),function(r){
    catlevel <- map.keys[[r]] #numeric
    timepoint <- map.values[[r]]$timepoint #numeric
    datastructure <- map.values[[r]]$data
    key <- c(catlevel,timepoint)
    rhcollect(key,datastructure)
  })
})
redsetup <- expression({
  currentkey <- NULL
})
reduce <- expression(
    pre={
      catlevel <- reduce.key[1]
      time <- reduce.key[2]
      if(!identical(catlevel,currentkey)){
        ## new categorical level
        ## so finalize the computation for
        ## the previous level
        if(!identical(currentkey,NULL))
          FINALIZE(F)
        ## store current categorical level
        currentkey <- catlevel
        ## initialize computation for new level
        INITIALIZE(F)
      }
    },
    reduce={
      F <- UPDATE(F, reduce.values[[1]])
    })
redclose <- expression({
  FINALIZE(F)
})
rhmr(..., combiner=FALSE,setup=list(reduce=redsetup),close=list(reduce=redclose),
     mapred=list(rhipe_map_output_keyclass='org.godhuli.rhipe.RHNumeric'),
     part=list(lims=1,type='numeric'))

     
       
      
##text transform

map <- expression({
  ## Each element of map.values is a line of text
  ## this needs to be tokenized and then combined
  tkn <- strsplit(unlist(map.values),",")
  text <- do.call("rbind",tkn)
  text <- text[text[,9] %in% c("WN","DL"),,drop=FALSE]
  if(nrow(text)>0) apply(text,1, function(r) rhcollect(r[9], r))
  rhcounter("addaad",c("a","e"),1)
})

## reduce=expression(
##     reduce={ lapply(reduce.values,function(r) rhcollect(reduce.key,r)) }

z <- rhmr(map=map,ifolder="/airline/data/1987.csv",ofolder="/airline/southdelta",
          ,inout=c("text","text"),
          part=list(lims=1,type="string"),
          mapred=list(
            rhipe_map_output_keyclass = "org.godhuli.rhipe.RHText",
            mapred.reduce.tasks=2,
            rhipe_string_quote='',
            mapred.field.separator=",",
            mapred.textoutputformat.usekey=FALSE))
rhex(z)













  airport <- read.table("~/tmp/airports.csv",sep=",",header=TRUE,stringsAsFactors=FALSE)
  aton <- new.env()
  for(i in 1:nrow(airport)){
    aton[[ airport[i,"iata"] ]] <- list(ap=airport[i,"airport"],latlong=airport[i,c("lat","long")])
  }
  rhsave(aton,file="/tmp/airports.Rdata")
  
  setup <- expression({
    load("airports.Rdata")
    co <- function(N){
      sapply(text[,N],function(r){
        o <- aton[[ r[1] ]]$ap
        if(is.null(o)) NA else sprintf('"%s"',o)
      })
    }
  })

  map <- expression({
    tkn <- strsplit(unlist(map.values),",")
    text <- do.call("rbind",tkn)
    text[,17] <- co(17)
    text[,18] <- co(18)
    apply(text,1,function(r){
      rhcollect(NULL,r)
    })
  })
  
  z <- rhmr(map=map,ifolder="/airline/data/2005.csv",ofolder="/airline/transform",
            ,inout=c("text","text"),
            shared=c("/airport/airports.Rdata"),
            setup=setup,
            mapred=list(
              mapred.reduce.tasks=0,
              rhipe_string_quote='',
              mapred.field.separator=",",
              mapred.textoutputformat.usekey=FALSE))
  rhex(z)
