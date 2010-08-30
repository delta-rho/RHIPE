// #include "ream.h"
// #include <vector>
// #include <iostream>
// #include <google/protobuf/io/coded_stream.h>
// #include <google/protobuf/io/zero_copy_stream_impl.h>
// #include <unistd.h>
// #include <stdint.h>
// #include <fcntl.h>
// #include <sys/wait.h>
// #include <signal.h>
// #include <stdio.h>
// #include <errno.h>
// #include <string.h>
// #include <sys/select.h>

// using namespace std;
// using namespace google::protobuf;
// using namespace google::protobuf::io;

// #define BUGBUG 0
// extern "C" {
//   static  SEXP NewList1(void)
//   {
//     SEXP s = Rf_cons(R_NilValue, R_NilValue);
//     SETCAR(s, s);
//     return s;
//   }
  
//   /* Add a new element at the end of a stretchy list */
  
//   static  SEXP GrowList1(SEXP l, SEXP s)
//   {
//     SEXP tmp;
//     PROTECT(s);
//     tmp = Rf_cons(s, R_NilValue);
//     UNPROTECT(1);
//     SETCDR(CAR(l), tmp);
//     SETCAR(l, tmp);
//     return l;
//   }
//   void RefObjectFinalizer(SEXP exref) {
//     if (TYPEOF(exref)!=EXTPTRSXP) 
//       Rf_error("Not a pointer\n");
  
//     SEXP ref  = (SEXP)R_ExternalPtrAddr(exref);
//     SEXP fdp = VECTOR_ELT(ref,1);
//     int status;
//     // Rprintf("FINAL Value is ");
//     // Rf_PrintValue(ref);

//     int pid = INTEGER(fdp)[0];
//     // Rprintf("Finalizing Java child helper for object %p(pid:%d)\n",exref,pid);
//     switch(waitpid(pid, &status, WNOHANG))
//         {
//           case 0 :
//             //process has not exited, kill it
// 	    Rprintf("RHIPE: Killing Java child helper for object %p(pid:%d)\n",exref,pid);
// 	    kill(pid, SIGTERM);
// 	    wait(&status);
//             break;
//           case -1 :
// #if BUGBUG	    
// 	    Rprintf("Error finalizing object:%s \n",strerror(errno));
// #endif
//             break;
//           default :
//             // process has exited,
//             break;
//         }
//     for(int i=1; i < LENGTH(fdp);i++)
//       close(INTEGER(fdp)[i]);

//     SEXP pipes = VECTOR_ELT(ref,0);
//     for(int i=0; i < LENGTH(pipes);i++){
//       remove(CHAR(STRING_ELT(pipes,i)));
//     }
//   }
  
//   SEXP robjectForRef(SEXP exref){
//     if (TYPEOF(exref)!=EXTPTRSXP) 
//       Rf_error("Not a pointer\n");
  

//     SEXP r = (SEXP)R_ExternalPtrAddr(exref);
//     //    SEXP n;
//     // char *names[] = { "pid","tofd","fromfd","errfd"};
//     // PROTECT(n = Rf_allocVector(STRSXP,4));
//     // for(int i=0;i < 4; i++){
//     //   SET_STRING_ELT(n,i,Rf_mkChar(names[i]));
//     // }
//     // // Rf_PrintValue(n);
//     // Rf_setAttrib(VECTOR_ELT(r,1),Rf_install("names"),n);
//     // UNPROTECT(1);
//     return(r);
//   }

//   SEXP makeErrorCode(int* n,int length){
//     SEXP r;
//     PROTECT(r = Rf_allocVector(INTSXP,length));
//     for(int i=0;i < length;i++){
//       INTEGER(r)[i]= n[i];
//     }
//     UNPROTECT(1);
//     return(r);
//   }
  
//   SEXP createProcess(SEXP program,SEXP processpipes,SEXP whatclose,SEXP bufsz){
    
//     // int errorpipe, fromJ, toJ;
//     if(       
//        mkfifo((const char*)CHAR(STRING_ELT(processpipes,0)),S_IRUSR|S_IWUSR)    // writing to child
//        |mkfifo((const char*)CHAR(STRING_ELT(processpipes,1)),S_IRUSR|S_IWUSR)  // reading from child
//        |mkfifo((const char*)CHAR(STRING_ELT(processpipes,2)),S_IRUSR|S_IWUSR)) // errors from child
//       {
// 	SEXP rr;
// 	PROTECT(rr = Rf_allocVector(STRSXP,1));
// 	SET_STRING_ELT(rr,0, Rf_mkChar(strerror(errno)));
// 	UNPROTECT(1);
// 	return(rr);
//       }
//     int pid = fork();
//     if(pid == 0){ // child
//       fclose(stdin);
//       if( INTEGER(whatclose)[0]==0){
// 	int nullend = open("/dev/null",666);
// 	dup2(nullend,2);
//       }
//       if( INTEGER(whatclose)[1]==0){
// 	int nullend = open("/dev/null",666);
// 	dup2(nullend,1);
// 	// fclose(stdout);
//       }

// #if BUGBUG
//       Rprintf("Running %s\n",(const char*)CHAR( STRING_ELT(program,0)));
// #endif
//       execl("/bin/sh","sh","-c", (const char*)CHAR( STRING_ELT(program,0)),(char*)NULL);
//       fprintf(stderr, "Program faild to run, errno=%d errstr=%s\n",errno,strerror(errno));
//       _exit(255);
//     }
//     // fromJ = open((const char*)CHAR(STRING_ELT(processpipes,1)), O_RDONLY) ;
//     // errorpipe = open((const char*)CHAR(STRING_ELT(processpipes,2)), O_RDONLY) ; 
//     // char u;
//     // read(fromJ, &u,1);
//     // toJ = open((const char*)CHAR(STRING_ELT(processpipes,0)),O_WRONLY);
//     // int r[] = {pid, toJ,fromJ,errorpipe};


//     FILE *fJ = fopen((const char*)CHAR(STRING_ELT(processpipes,1)), "r") ;
//     FILE *eP = fopen((const char*)CHAR(STRING_ELT(processpipes,2)), "r") ;
//     setvbuf(fJ, 0, _IOFBF , INTEGER(bufsz)[0]);
//     setvbuf(eP, 0, _IOFBF , INTEGER(bufsz)[0]);
//     char u;
//     read(fileno(fJ),&u,1);
//     FILE *tJ = fopen((const char*)CHAR(STRING_ELT(processpipes,0)),"w");
//     setvbuf(tJ, 0, _IOFBF, INTEGER(bufsz)[0]);
//     int r[] = {pid, fileno(tJ),fileno(fJ),fileno(eP)};

//     SEXP result;
//     PROTECT(result = Rf_allocVector(VECSXP,2));
//     SET_VECTOR_ELT(result,0,processpipes);
//     SET_VECTOR_ELT(result,1,makeErrorCode(r,4));
//     UNPROTECT(1);
//     SEXP rz =  R_MakeExternalPtr(result, R_NilValue,result);
//     R_RegisterCFinalizerEx(rz, RefObjectFinalizer, (Rboolean)1);
//     return(rz);
//   }



//   SEXP _readSomething(int fromworker, int errorfd,timeval* tout){
//     fd_set rfds;
//     FD_ZERO(&rfds);
//     FD_SET(fromworker, &rfds); FD_SET(errorfd,&rfds);
//     int maxfd = fromworker > errorfd ? fromworker+1 : errorfd+1;
//     SEXP v = R_NilValue;
//     int retval;
// #if BUGBUG
//     Rprintf("Waiting for a result\n");
// #endif
//     while( (retval = select(maxfd, &rfds,NULL,NULL, tout))){
// #if BUGBUG
//       Rprintf("Got one\n");
// #endif
//       if(retval == -1){
// 	PROTECT(v = Rf_allocVector(STRSXP,1));
// 	SET_STRING_ELT(v,0, Rf_mkChar(strerror(errno)));
// 	UNPROTECT(1);
// 	break;
//       }
//       int myfd = fromworker;
//       bool error = false;
//       if(FD_ISSET(errorfd,&rfds)){
// #if BUGBUG
// 	Rprintf("RHIPE:Got a result on errorfd\n");
// #endif
// 	myfd =  errorfd;error=true;
//       }
//       uint32_t n0 = readVInt64FromFD(myfd);
//       PROTECT(v = Rf_allocVector(RAWSXP,n0));
//       read(myfd,RAW(v),n0);
//       UNPROTECT(1);
//       break;
//     }
//     return(v);
//   }

//   SEXP readSomething(SEXP eref,SEXP wai){
//     if (TYPEOF(eref)!=EXTPTRSXP) 
//       Rf_error("RHIPE: At the very least, not a pointer\n");
//     SEXP ref = (SEXP)R_ExternalPtrAddr(eref);
//     int fromworker, errorfd;
//     fromworker = INTEGER(VECTOR_ELT(ref,1))[1];
//     errorfd = INTEGER(VECTOR_ELT(ref,1))[2];
//     if(INTEGER(wai)[0])
//       return(_readSomething(fromworker,errorfd,NULL));
//     else{
//       struct timeval *tv = (struct timeval*)calloc(sizeof(struct timeval),1);
//       tv->tv_sec = 0;
//       tv->tv_usec = 0;
//       return(_readSomething(fromworker,errorfd,tv));
//     }
//   }

//   SEXP send_command_1(SEXP eref,SEXP what,SEXP rd){
//     if (TYPEOF(eref)!=EXTPTRSXP) 
//       Rf_error("RHIPE: At the very least, not a pointer\n");
//     SEXP ref = (SEXP)R_ExternalPtrAddr(eref);
//     int toworker, fromworker, errorfd;

//     toworker = INTEGER(VECTOR_ELT(ref,1))[1];
//     fromworker = INTEGER(VECTOR_ELT(ref,1))[2];
//     errorfd = INTEGER(VECTOR_ELT(ref,1))[3];
    
//     REXP *rexp_container = new REXP();
//     rexp2message(rexp_container,what);
//     uint32_t bs = rexp_container->ByteSize();
//     uint8 *d = (uint8*)malloc(bs);
//     uint32_t bs_rev = reverseUInt(bs);
//     rexp_container->SerializeWithCachedSizesToArray(d);
//     write(toworker, &bs_rev,sizeof(uint32_t));
//     write(toworker,d,bs);
//     free(d);
//     delete rexp_container;
//     // select on fromworker and errorfd
//     SEXP r = R_NilValue;
//     if(INTEGER(rd)[0])
//       r = _readSomething(fromworker, errorfd,NULL);
//     return(r);
//   }
 
    
//   SEXP wbFile(SEXP eref,SEXP data,SEXP nn){
//     if (TYPEOF(eref)!=EXTPTRSXP) 
//       Rf_error("RHIPE: At the very least, not a pointer\n");
//     SEXP ref = (SEXP)R_ExternalPtrAddr(eref);
//     int toworker = INTEGER(VECTOR_ELT(ref,1))[1];
//     int fromworker = INTEGER(VECTOR_ELT(ref,1))[2];
//     int errorfd = INTEGER(VECTOR_ELT(ref,1))[3];
//     struct timeval *tv = (struct timeval*)calloc(sizeof(struct timeval),1);

//     tv->tv_sec = 0;
//     tv->tv_usec = 0;

//     REXP *rexp_container = new REXP();
//     int m = INTEGER(nn)[0],n = m;
//     uint8_t *_k=(uint8_t*)malloc(m);
//     uint32_t kvlength;
//     SEXP errsxp;
//     uint32_t countsum = 0;
//     for(int i=0;i < LENGTH(data);i++){
//       R_CheckUserInterrupt();

//       SEXP a = VECTOR_ELT(data,i);
//       SEXP k = VECTOR_ELT(a,0);
//       SEXP v = VECTOR_ELT(a,1);

//       rexp_container->Clear();
//       rexp2message(rexp_container,k);  
//       int bs = rexp_container->ByteSize();
//       if(bs>n){
// 	_k = (uint8_t *)realloc(_k,bs+m);n=bs+m;
//       }
//       rexp_container->SerializeWithCachedSizesToArray(_k);
//       kvlength = reverseUInt((uint32_t)bs);
//       write(toworker, &kvlength,sizeof(uint32_t));
//       write(toworker,_k,bs);
//       countsum += bs;
//       errsxp = _readSomething(fromworker,errorfd,tv);
//       if(TYPEOF(errsxp) != NILSXP){
// 	free(tv);
// 	return(errsxp);
//       }
//       rexp_container->Clear();
//       rexp2message(rexp_container,v);  
//       bs = rexp_container->ByteSize();
//       if(bs>n){
// 	_k = (uint8_t*) realloc(_k,bs+m);n=bs+m;
//       }
//       rexp_container->SerializeWithCachedSizesToArray(_k);
//       kvlength = reverseUInt((uint32_t)bs);
//       write(toworker,&kvlength,sizeof(uint32_t));
//       write(toworker,_k,bs);
//       countsum += bs;
//       errsxp = _readSomething(fromworker,errorfd,tv);
//       if(TYPEOF(errsxp) != NILSXP){
// 	free(tv);
// 	return(errsxp);
//       }

//     }
//     free(_k);
//     delete(rexp_container);
//     SEXP res = _readSomething(fromworker,errorfd,NULL);
//     // Rf_PrintValue(res);
//     free(tv);

//     if(countsum< 12*1024)
//       Rprintf("RHIPE: Wrote %.2f KB\n", ((double)countsum)/(1024));
//     else
//       Rprintf("RHIPE: Wrote %.2f MB\n", ((double)countsum)/(1024*1024));

//     return(res);   
//   }

//   // SEXP readKVpossiblyE(int fromworker,int errorfd, int *errsignal,
//   // 		       timeval* tv, uint32_t* countsum){
//   //   fd_set rfds;
//   //   FD_ZERO(&rfds);
//   //   FD_SET(fromworker, &rfds); FD_SET(errorfd,&rfds);
//   //   int maxfd = fromworker > errorfd ? fromworker+1 : errorfd+1;
//   //   int retval = select(maxfd, &rfds,NULL,NULL, tv);
//   //   SEXP v = R_NilValue;
//   //   int myfd=0;
//   //   *errsignal = 1 ;
//   //   switch(retval){
//   //   case -1:// Rprintf("Got SUPER bad error value from worker\n");
//   //     *errsignal = 2;
//   //     SEXP r;
//   //     PROTECT(v = Rf_allocVector(STRSXP,1));
//   //     SET_STRING_ELT(r,0,Rf_mkChar(strerror(errno)));
//   //     UNPROTECT(1);
//   //     break;
//   //   case 0:
//   //     break;
//   //   default:
//   //     if(FD_ISSET(errorfd,&rfds)){
//   // 	*errsignal = 3;
//   // 	myfd = errorfd;
//   //     }else if(FD_ISSET(fromworker,&rfds)){
//   // 	*errsignal = 0;
//   // 	myfd = fromworker;
//   //     }
//   //     uint32_t n00 = readVInt64FromFD(myfd);
//   //     if(n00 == 0) { *errsignal = 4;return(v);} //end of data
//   //     *countsum = *countsum+n00;
//   //     // printf(" [size:%d] ", n00);
//   //     PROTECT(v = Rf_allocVector(RAWSXP,n00));
//   //     read(myfd,RAW(v),n00);
//   //     UNPROTECT(1);
//   //   }
//   //   return(v);
//   //   //errsignal:  
//   //   //            0  something read (good)
//   //   //            1  nothing read, but should not occur
//   //   //            2  error in select (contains errno)
//   //   //            3  error from worker (contains error)
//   //   //            4  the end
//   // }

//   SEXP readKVpossiblyE(int fromworker, int errorfd,FILE* fromworkerFP,FILE* errorfdFP, int *errsignal,
//   		       timeval* tv, uint32_t* countsum){
//     fd_set rfds;
//     // int fromworker = fileno(fromworkerFP), errorfd = fileno(errorfdFP);
//     FD_ZERO(&rfds);
//     FD_SET(fromworker, &rfds); FD_SET(errorfd,&rfds);
//     int maxfd = fromworker > errorfd ? fromworker+1 : errorfd+1;
//     int retval = select(maxfd, &rfds,NULL,NULL, tv);
//     SEXP v = R_NilValue;
//     FILE* myfd=NULL;
//     *errsignal = 1 ;


//     switch(retval){
//     case -1:// Rprintf("Got SUPER bad error value from worker\n");
//       *errsignal = 2;
//       SEXP r;
//       PROTECT(v = Rf_allocVector(STRSXP,1));
//       SET_STRING_ELT(r,0,Rf_mkChar(strerror(errno)));
//       UNPROTECT(1);
//       break;
//     case 0:
//       break;
//     default:
//       if(FD_ISSET(errorfd,&rfds)){
//   	*errsignal = 3;
//   	myfd = errorfdFP;
//       }else if(FD_ISSET(fromworker,&rfds)){
//   	*errsignal = 0;
//   	myfd = fromworkerFP;
//       }
//       uint32_t n00 = readVInt64FromFileDescriptor(myfd);
//       if(n00 == 0) { *errsignal = 4;return(v);} //end of data
//       *countsum = *countsum+n00;
//       // printf(" [size:%d] ", n00);
//       PROTECT(v = Rf_allocVector(RAWSXP,n00));
//       fread(RAW(v),n00,1,myfd);
//       UNPROTECT(1);
//     }
//     return(v);
//     //errsignal:  
//     //            0  something read (good)
//     //            1  nothing read, but should not occur
//     //            2  error in select (contains errno)
//     //            3  error from worker (contains error)
//     //            4  the end
//   }
//   /************************************************
//    * this function returns a list if successful
//    * if there is an error
//    * - an object of class worker_error
//    * - an integer containing select errno
//    * - NIL, in (for whatever reason) select was called with 0 timeout
//             and it returned immediately - we should never see this
//   ****************************************/

//   SEXP rbFile(SEXP eref){
//     if (TYPEOF(eref)!=EXTPTRSXP) 
//       Rf_error("RHIPE: At the very least, not a pointer\n");
//     SEXP ref = (SEXP)R_ExternalPtrAddr(eref);
//     int fromworkerFD = INTEGER(VECTOR_ELT(ref,1))[2];
//     int errorfdFD = INTEGER(VECTOR_ELT(ref,1))[3];

//     FILE* fromworker = fdopen(INTEGER(VECTOR_ELT(ref,1))[2],"r");
//     FILE* errorfd = fdopen(INTEGER(VECTOR_ELT(ref,1))[3],"r");

//     struct timeval *tv = (struct timeval*)calloc(sizeof(struct timeval),1);

//     tv->tv_sec = 0;
//     tv->tv_usec = 0;

//     SEXP rv;
//     uint32_t countsum=0;
//     PROTECT(rv = NewList1());
//     bool abort = false;
//     int errsignal=0;
//     uint32_t j=0;
//     while(!abort){
//       // R_ProcessEvents(); 
//       R_CheckUserInterrupt();
//       SEXP l=R_NilValue,k,val;
//       PROTECT(k = readKVpossiblyE(fromworkerFD,errorfdFD,fromworker,errorfd,&errsignal, NULL,&countsum));
//       //we have an error we need to abort
//       switch(errsignal){
//       case 0:
//       PROTECT(l = Rf_allocVector(VECSXP,2));
//       SET_VECTOR_ELT( l, 0,k) ;
//       // printf("Read 1 of 2,");
//       break;
//       case 1:
//       case 2:
//       case 3:
// 	UNPROTECT(2); //rv & k
// 	return(k);
//       case 4:
// 	UNPROTECT(1); //k
// 	abort =true;
// 	goto skipvalue;
//       }

//       // printf(" ... reading value ..., ");
//       PROTECT(val = readKVpossiblyE(fromworkerFD,errorfdFD,fromworker,errorfd,&errsignal, NULL,&countsum));
//       switch(errsignal){
//       case 0:
// 	j++;
// 	// printf("... setting value of pair .... ");
// 	SET_VECTOR_ELT( l, 1,val) ;
// 	UNPROTECT(3); //l,val,k
// 	rv = GrowList1(rv, l);
// 	// printf("2 of 2: %d pairs\n",j);
// 	break;
//       case 1:
//       case 2:
//       case 3:
// 	UNPROTECT(3); //rv and l,k
// 	return(k);
//       }
//  skipvalue:
//       ;
//     }
//     fflush(fromworker);fflush(errorfd);
//     rv = CDR(rv);
//     SEXP rval;
//     PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
//     for (int n = 0 ; n < LENGTH(rval) ; n++, rv = CDR(rv)){
//       SET_VECTOR_ELT(rval, n, CAR(rv));
//     }
//     if(countsum< 12*1024)
//       Rprintf("RHIPE: %d pairs, about to unserialize %.2f KB, please wait.\n", j,((double)countsum)/(1024));
//     else
//       Rprintf("RHIPE: %d pairs, about to unserialize %.2f MB, please wait.\n", j,((double)countsum)/(1024*1024));
//     UNPROTECT(2); //rval, rv
//     return(rval);
//   }


//   SEXP isalive(SEXP eref){
//     if (TYPEOF(eref)!=EXTPTRSXP) 
//       Rf_error("RHIPE: At the very least, not a pointer\n");
//     SEXP ref = (SEXP)R_ExternalPtrAddr(eref);
//     // Rprintf("Value is ");
//     // Rf_PrintValue(ref);
//     int pid = INTEGER(VECTOR_ELT(ref,1))[0];
//     int status;
//     int code;
//     SEXP s=R_NilValue;
//     switch(waitpid(pid, &status, WNOHANG)){    
//     case 0:
//       {
// 	// process has not exited;
// 	code  = -1;
// 	s = makeErrorCode(&code,1);
// 	break;
//       }
//     default:
//       if(WIFEXITED(status)){
// 	code = 5000+WEXITSTATUS(status);
// 	s = makeErrorCode(&code,1);
//       }
//       if(WIFSIGNALED(status)){
// 	code = WTERMSIG(status);
// 	PROTECT( s = Rf_allocVector(STRSXP,1));
// 	SET_STRING_ELT(s,0, Rf_mkChar(strerror(code)));
// 	UNPROTECT(1);
//       }
//     }
//     SEXP c;
//     PROTECT(c = Rf_allocVector(STRSXP,1));
//     SET_STRING_ELT(c,0,Rf_mkChar("error"));
//     UNPROTECT(1);
//     Rf_setAttrib(s,Rf_install("class"),c);
//     return(s);
//   }
// }

