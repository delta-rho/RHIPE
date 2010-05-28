#ifndef __PIPEDRHIPECONTROLLER__
#define __PIPEDRHIPECONTROLLER__

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <rexp.pb.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "ream.h"

#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
using namespace google::protobuf;
using namespace google::protobuf::io;

class PipedRhipeController;


class PipedRhipeController {
private:
  int32_t fd_toworker, fd_fromworker;
  pid_t pid;
  int returncode;
  string fifo_fromworker, fifo_toworker;
public:
  PipedRhipeController(){}
  /* ~PipedRhipeController(){ */
  /* } */
  inline int wait(void){
    int stat_loc = 0;
    waitpid(pid, &stat_loc, WNOHANG);
    if(WIFSIGNALED(stat_loc)){
      returncode = -WTERMSIG(stat_loc);
    }else if (WIFEXITED(stat_loc)){
      returncode = WEXITSTATUS(stat_loc);
    }
    return(returncode);
  }
  inline string fromworker(){
    return fifo_fromworker;
  }
  inline string toworker(){
    return fifo_toworker;
  }

  inline  void send_signal(int sig){
    kill(pid,sig);
  }
  inline SEXP execute_child(const char *prog,char **args, const char* fromworker, const char* toworker){
    if(mkfifo(fromworker,0666)  | mkfifo(toworker,S_IRUSR| S_IWUSR))
      return(R_NilValue);
    fifo_toworker = string(toworker);
    fifo_fromworker = string(fromworker);
    pid = fork();
    if(pid == 0){
      // in Child
      if(execvp(prog, args)==-1)
	{
	  //error
	  FILE *fp = fopen("/tmp/gox","w");
	  fprintf(fp,"Error in child exec, errno==%d\n",errno);
	  fclose(fp);
	}
      _exit(255);
    }
    /* FILE * file = fopen("/tmp/gox", "r"); */
    /* if (file){ */
    /*   //we had an error starting child */
    /*   char er[1024*1024]; */
    /*   fread(er,1024*1024,1,file); */
    /*   er[1024*1024-1]=0; */
    /*   SEXP result = R_NilValue; */
    /*   PROTECT(result = Rf_allocVector(STRSXP,1)); */
    /*   SET_STRING_ELT(result,0,Rf_mkChar(er)); */
    /*   remove("/tmp/gox"); */
    /*   UNPROTECT(1); */
    /*   fclose(file); */
    /*   return(result); */
    /* } */
    //parent
    /* printf("opening FIFO(%s) to read from worker process\n",fromworker); */
    fd_fromworker = open(fromworker, O_RDONLY) ; 
    char u;
    read(fd_fromworker, &u,1);
    fd_toworker = open(toworker,O_WRONLY);
    SEXP result = R_NilValue;
    PROTECT(result = Rf_allocVector(INTSXP,1));
    INTEGER(result)[0]=u;
    UNPROTECT(1);    
    return(result);
  }

  inline int send_message(const REXP *m){
    uint32_t bs = m->ByteSize();
    uint8 *d = (uint8*)malloc(bs);
    uint32_t bs_rev = reverseUInt(bs);
    m->SerializeWithCachedSizesToArray(d);
    FILE *f = fdopen( fd_toworker,"w" ); 
    /* write(fd_toworker, &bs_rev, sizeof(uint32)); */
    /* write(fd_toworker, d, bs); */
    fwrite(&bs_rev,sizeof(uint32_t),1,f);
    fwrite(d,bs,1,f);
    fflush(f);
    free(d);
    return(bs);
  }
  
  /* inline void read_message(REXP* rexp_container){ */
  /*   uint32 h = (uint32)readJavaInt(fd_fromworker); */
  /*   printf("Read and will process %d bytes\n",h); */
  /*   uint8 *d = (uint8*) malloc(h); */
  /*   int a =read(fd_fromworker, d, h); */
  /*   printf("Read %d bytes\n",a); */
  /*   CodedInputStream cds(d,h); */
  /*   cds.SetTotalBytesLimit(306*1024*1024,306*1024*1024); */
  /*   rexp_container->ParseFromCodedStream(&cds); */
  /*   free(d); */
  /* } */
  inline void read_message(REXP* rexp_container){
    FileInputStream *fis = new FileInputStream(fd_fromworker,1024*1024);
    CodedInputStream* cis = new CodedInputStream(fis);
    cis->SetTotalBytesLimit(300*1024*1024,300*1024*1024);
    uint32_t n0;
    if(!cis->ReadVarint32(&n0)) return;;
    uint8 * h = (uint8*)malloc(n0);
    cis->ReadRaw(h,n0);

    CodedInputStream cds(h,n0);
    cds.SetTotalBytesLimit(306*1024*1024,306*1024*1024);
    rexp_container->ParseFromCodedStream(&cds);

    delete cis;
    delete fis;
  }

  SEXP writeBinaryFile(SEXP d, int n){
    REXP *rexp_container = new REXP();
    int m = n;
    uint8_t *_k=(uint8_t*)malloc(n);
    FILE *fp = fdopen(fd_toworker,"w");
    uint32_t kvlength;
    for(int i=0;i < LENGTH(d);i++){
      SEXP a = VECTOR_ELT(d,i);
      SEXP k = VECTOR_ELT(a,0);
      SEXP v = VECTOR_ELT(a,1);

      rexp_container->Clear();
      rexp2message(rexp_container,k);  
      int bs = rexp_container->ByteSize();
      if(bs>n){
	_k = (uint8_t *)realloc(_k,bs+m);n=bs+m;
      }
      rexp_container->SerializeWithCachedSizesToArray(_k);
      kvlength = reverseUInt((uint32_t)bs);
      fwrite(&kvlength,sizeof(uint32_t),1,fp);
      fwrite(_k,bs,1,fp);

      rexp_container->Clear();
      rexp2message(rexp_container,v);  
      bs = rexp_container->ByteSize();
      if(bs>n){
	_k = (uint8_t*) realloc(_k,bs+m);n=bs+m;
      }
      rexp_container->SerializeWithCachedSizesToArray(_k);
      kvlength = reverseUInt((uint32_t)bs);
      fwrite(&kvlength,sizeof(uint32_t),1,fp);
      fwrite(_k,bs,1,fp);
    }
    fflush(fp);
    free(_k);
    delete(rexp_container);
    return(R_NilValue);   
  }

   SEXP readSQFromPipe(int n){
    uint32_t n0;
    FileInputStream *fis = new FileInputStream(fd_fromworker, n);
    CodedInputStream* cis = new CodedInputStream(fis);
    cis->SetTotalBytesLimit(266*1024*1024,266*1024*1024);
    SEXP rv;
    uint32_t countsum=0;
    PROTECT(rv = NewList());
    while(true){
      SEXP l,k,v;
      cis->ReadVarint32(&n0);
      if(n0==0) break;
      PROTECT(l = Rf_allocVector(VECSXP,2));

      PROTECT(k = Rf_allocVector(RAWSXP,n0));
      cis->ReadRaw(RAW(k),n0);
      SET_VECTOR_ELT( l, 0, k);
      countsum+= n0;

      cis->ReadVarint32(&n0);
      PROTECT(v = Rf_allocVector(RAWSXP,n0));
      cis->ReadRaw(RAW(v),n0);
      SET_VECTOR_ELT( l, 1, v);
      countsum+= n0;

      rv = GrowList(rv, l);
      UNPROTECT(3);
    }
    delete cis;
    delete fis;
  
    rv = CDR(rv);
    SEXP rval;
    PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
    for (int n = 0 ; n < LENGTH(rval) ; n++, rv = CDR(rv)){
      SET_VECTOR_ELT(rval, n, CAR(rv));
    }
    if(countsum< 12*1024)
      printf("About to unserialize %.2f kb, please wait\n", ((double)countsum)/(1024));
    else
      printf("About to unserialize %.2f mb, please wait\n", ((double)countsum)/(1024*1024));
    UNPROTECT(2);
    return(rval);
  }
};

#endif
