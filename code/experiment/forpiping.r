library(Rhipe)
x <- .Call("make_new_java")
f1 <- c(tempfile(pattern=c("rhipe.fromworker.","rhipe.toworker.")))
r <- .Call("exec_child", x, "/ln/meraki/custom/hadoop/bin/hadoop",c("/ln/meraki/custom/hadoop/bin/hadoop","jar",rhoptions()$jarloc,"org.godhuli.rhipe.Richmond",f1),f1)
y=.Call("read_d",x)
y


.Call("send_d",x,list("rhput",c("/home/sguha/airline.Rdata","/home/sguha/a.out"),"/",FALSE))
y=.Call("read_d",x)
cat(y)

.Call("send_d",x,list("rhput",c("/home/sguha/airline.Rdata"),"/zangam",FALSE))
y=.Call("read_d",x)
cat(y)



.Call("send_d",x,list("rhdel",c("/airline.Rdata")))
y=.Call("read_d",x)
cat(y)

.Call("send_d",x,list("rhdel",c("/zan*")))
y=.Call("read_d",x)
cat(y)

.Call("send_d",x,list("rhget","/zan*","/tmp/mugo"))
y=.Call("read_d",x)
cat(y)

l <- lapply(runif(100000),function(r){
  list(runif(1),r)
})
howmany <- 10
groupsize <- as.integer(length(l)/howmany)
N <- length(l)
tmf <- tempfile()
.Call("writeBinaryFile", l, tmf , as.integer(16384))
.Call("send_d",x,list("binaryAsSequence","goofy",as.integer(groupsize),as.integer(howmany),as.integer(N),tmf))
## .Call("sendListToWorker",x,l, as.integer(16384))

y=.Call("read_d",x)
cat(y)

.Call("send_d",x,list("sequenceAsBinary",c("/user/sguha/goofy/0","/user/sguha/goofy/100"),-1L))
z <- .Call("readDataFromSequence",x,16384L)
y=.Call("read_d",x)
cat(y)


## It is preferable not to 
.Call("send_d",x,list("sequenceAsBinary",c("/user/sguha/goofy/0","/user/sguha/goofy/1","/user/sguha/goofy/2","/user/sguha/goofy/3","/user/sguha/goofy/4","/user/sguha/goofy/5","/user/sguha/goofy/6","/user/sguha/goofy/7")))
z <- .Call("readDataFromSequence",x,1L)
y=.Call("read_d",x)
cat(y)


.Call("send_d",x,list("rhmropts",1))
y=.Call("read_d",x)
y



.Call("send_d",x,list("rhls","/",0L))
y=.Call("read_d",x)
y



.Call("kill_worker_and_wait",x)


.Call("send_d",x,list("rhls","/",0L))
y=.Call("read_d",x)
y


