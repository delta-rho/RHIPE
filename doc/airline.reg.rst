##
## Create A File with all subsets done for you
##
map <- expression({
    a <- do.call("rbind",map.values)
    arr.delay <- (as.vector(a[,'arrive'])-as.vector(a[,'sarrive']))/60
    dow <- a[,'wday']
    hod <- as.POSIXlt(a[,'depart'])$hour
    x <- data.frame(arr.delay=arr.delay, dow=dow, hod=hod)
    x <- x[!is.na(x$arr.delay) & !is.na(x$dow) & !is.na(x$hod),]
    x <- x[x$arr.delay>0,]
    if(nrow(x)>0){
      rhcounter("ALL","a",nrow(x))
      rhcollect(map.keys[[1]],x)
    }
  })

z <- rhmr(map=map, ifolder="/airline/blocks/",ofolder="/airline/airlinesubset",inout=c("sequence",'sequence'),mapred=list(rhipe_map_buff_size=10,mapred.reduce.tasks=0))
rhex(z)
inputfile <- "/airline/airlinesubset"
##############
## Local means
##############

map <- expression({
  x <- do.call("rbind",map.values)
  y <- split(x,list(x$hod,x$dow))
  lapply(y,function(r){
    tot <- sum(r$arr.delay)
    len <- nrow(r)
    code <- as.integer(r[1,c("dow","hod")])
    if(!is.na(code[1])) rhcollect(code,c(len,tot)) 
  })})
reduce <- expression(
    pre={
      summ=0
    },
    reduce={
      summ <- summ+apply(do.call("rbind",reduce.values),2,sum)
    }
    ,post={ rhcollect(reduce.key, summ)}
    )
z <- rhmr(map=map, reduce=reduce, combiner=TRUE, ifolder=inputfile, ofolder="/tmp/tof", inout=c("sequence","sequence")
     ,mapred=list(rhipe_map_buff_size=5,mapred.max.split.size=67108864))
rhex(z)

r <- rhread("/tmp/tof")
cs <- cbind(do.call("rbind",lapply(r,"[[",1))
      ,do.call("rbind",lapply(r,"[[",2)))
colnames(cs) <- c("dow","hod","n","ad")
cs <- as.data.frame(cs)
cs$adm <- cs$ad/cs$n
cs <- cs[order(cs$hod,cs$dow),]

aa <- cs[cs$hod==0 & cs$dow==0,]
sum(aa$ad)/sum(aa$n)

head(cs)

##############
## TRY rhlm
##############

rs <- rhlm(arr.delay~dow*hod
           ,data=inputfile,factor=list(dow=0:6, hod=0:23)
           ,apred=list(rhipe_map_buff_size=10,mapred.max.split.size=67108864),debug=2)








## z=rhmr(map=map, ifolder="/tmp/x1234",ofolder="/tmp/macv",inout=c("sequence","sequence"),
##   mapred=list(rhipe_map_buff_size=3,mapred.reduce.tasks=0))
## xpx <- 0;xpy <- 0;count <- 0
## for(i in rhls("/tmp/macv")$file){
##   print(i)
##   p <- rhread(i)
##   for(o in p){
##     key <- o[[1]]
##     value <- o[[2]]
##     count <- count+1
##     print(count)
##     if(key==0L)
##       xpx <- xpx+ value
##     else
##       xpy <- xpy+value
##   }
## }
## betahat <- solve(xpx) %*% xpy
