#include "PipedRhipeController.h"
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <rexp.pb.h>
#include <signal.h>
#include "ream.h"

extern SEXP message2rexp(const REXP&);
extern void rexp2message(REXP* rxp,const SEXP robj);
using namespace google::protobuf;
using namespace google::protobuf::io;

extern "C" {
  void RefObjectFinalizer(SEXP ref) {
    if (TYPEOF(ref)==EXTPTRSXP) {
      PipedRhipeController * o = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
      if(o) {
	// printf("Deleting %p and killing child java process\n",o);
	if(!o->wait()){
	  o->send_signal(SIGKILL);
	}
	o->wait();
	remove(o->toworker().c_str());
	remove(o->fromworker().c_str());
	delete o;
      }
    }
  }
    
  SEXP make_new_java(void){
    PipedRhipeController *pp = new PipedRhipeController();
    SEXP r =  R_MakeExternalPtr(pp, R_NilValue,R_NilValue);
    R_RegisterCFinalizerEx(r, RefObjectFinalizer, (Rboolean)1);
    return r;
  }

  SEXP exec_child(SEXP ref, SEXP cmd,SEXP rest,SEXP workernames){
    char ** m = (char**) malloc(LENGTH(rest)+1);
    for(int i=0;i< LENGTH(rest);i++){
      const char *s = CHAR(STRING_ELT(rest,i));
      m[i] = (char*)malloc(strlen(s)+1);
      strcpy(m[i], s);
    }
    m[LENGTH(rest)]=0;
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    SEXP a = prc->execute_child(CHAR(STRING_ELT(cmd,0)), m,  CHAR(STRING_ELT(workernames,0)),
		       CHAR(STRING_ELT(workernames,1)));
    return(a);
  }

  SEXP send_d(SEXP ref, SEXP what){
    REXP *rexp_container = new REXP();
    rexp2message(rexp_container,what);  
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    int sent = prc->send_message(rexp_container);
    SEXP result = R_NilValue;
    PROTECT(result = Rf_allocVector(INTSXP,1));
    INTEGER(result)[0]=sent;
    UNPROTECT(1);
    delete rexp_container;
    return(result);
  }

  SEXP read_d(SEXP ref){
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    REXP *rexp_container = new REXP();
    prc->read_message(rexp_container);
    SEXP ans=R_NilValue;
    PROTECT(ans = message2rexp(*rexp_container));
    UNPROTECT(1);
    delete(rexp_container);
    return(ans);
  }
  SEXP kill_worker_and_wait(SEXP ref){
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    prc->send_signal(SIGKILL);
    int a = prc->wait();
    SEXP result = R_NilValue;
    PROTECT(result = Rf_allocVector(INTSXP,1));
    INTEGER(result)[0]=a;
    UNPROTECT(1);
    return(result);
  }
  SEXP sendListToWorker(SEXP ref,SEXP list, SEXP buf){
    int n=INTEGER(buf)[0];
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    prc->writeBinaryFile(list,n);
    return(R_NilValue);
  }
  SEXP readDataFromSequence(SEXP ref,SEXP buf){
    int n=INTEGER(buf)[0];
    PipedRhipeController * prc = static_cast<PipedRhipeController *>(R_ExternalPtrAddr(ref));
    SEXP a = prc->readSQFromPipe(n);
    return(a);
  }

}
	    
