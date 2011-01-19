#include "ream.h"
#include <vector>
#include <iostream>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdio.h>
#include <errno.h>

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::io;

// static REXP *rexp_container = new REXP();
extern uintptr_t R_CStackLimit; 
const int i___ = 1;
#define is_bigendian() ( (*(char*)&i___) == 0 )

void CaptureLogInLibrary(LogLevel level, const char* filename, int line,
                const string& message) {
  static char* pb_log_level[] = {"LOGLEVEL_INFO","LOGLEVEL_WARNING",
				"LOGLEVEL_ERROR","LOGLEVEL_FATAL",
				"LOGLEVEL_DFATAL" };
  Rf_error("PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename,line, message.c_str());  
}

extern "C" {
  void
  R_init_Rhipe(DllInfo *info)
  {
    R_CStackLimit = (uintptr_t)-1;
    google::protobuf::SetLogHandler(&CaptureLogInLibrary);
  }
  


  //Neither of these are thread safe...
  SEXP serializeUsingPB(SEXP robj)
  {
    REXP *rexp_container = new REXP();
    rexp_container->Clear();
    rexp2message(rexp_container,robj);  
    int bs = rexp_container->ByteSize();
    SEXP result = R_NilValue;
    PROTECT(result = Rf_allocVector(RAWSXP,bs));
    rexp_container->SerializeWithCachedSizesToArray(RAW(result));
    UNPROTECT(1);
    delete(rexp_container);
    return(result);
  }
  
  SEXP unserializeUsingPB(SEXP robj)
  {
    if (TYPEOF(robj)!=RAWSXP)
      Rf_error("Must pass a raw vector");
    SEXP ans  = R_NilValue;
    REXP *rexp_container = new REXP();
    CodedInputStream cds(RAW(robj),LENGTH(robj));
    cds.SetTotalBytesLimit(REXP_MAX_SIZE,REXP_MAX_SIZE);
    rexp_container->ParseFromCodedStream(&cds);
    PROTECT(ans = message2rexp(*rexp_container));
    UNPROTECT(1);
    delete(rexp_container);
    return(ans);
  }
}

    
     
