#ifndef	__ream_h
#define	__ream_h

#include <iostream>
#include <vector>
#include <map>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/io/coded_stream.h>

#include <rexp.pb.h>
#include <stdint.h>
#include <sys/types.h>	
#include <sys/time.h>	
#include <time.h>	
#include <errno.h>
#include <fcntl.h>	
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include "jni.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
using namespace google::protobuf::io;
using namespace std;

#define R_NO_REMAP
#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1 
#include <Rversion.h>
#include <R.h>
#include <Rdefines.h>
#include <Rinterface.h>
#include <Rembedded.h>
#include <R_ext/Boolean.h>
#include <R_ext/Parse.h>
#include <R_ext/Rdynload.h>
#define REXP_MAX_SIZE 128*1024*1024
class JMessages{
 public:
  JMessages(jobject o);
  ~JMessages();
  uint32_t size_kv_stage_buffer_out;
  uint32_t count_kv_stage_buffer_out;
  /* jclass spill_method_class; */
  jmethodID spill_method_id;
  ArrayOutputStream * z_kv_stage_buffer_out;
  CodedOutputStream* cdo_kv_stage_buffer_out;
  jobject engineObject; //the RHMRHelper object,bollocks!
  jobject engineClass;
  REXP *rexp_container;
  void * staging_backingstore;
  void * reduce_key_backingstore;
  int  staging_bytes,reduce_key_bytes;
  jobject staging_buffer;
  jobject reduce_key_buffer;
  void cdo_staged_writer(SEXP ,SEXP );
  jobject create_storage(jint);
  jobject create_reduce_key_storage(jint);
  map<string, vector<string> > map_output_buffer;
  int combiner_spill_size;
};
extern JavaVM *cached_jvm;
extern JMessages *stored_jv_messages;

SEXP rexpress(const char*);
void rexp2message(REXP *, const SEXP);
void fill_rexp(REXP *, const SEXP );
SEXP message2rexp(const REXP&);
void jstring2REXP(JNIEnv* ,jbyteArray ,REXP* );
SEXP rh_spool_kv(SEXP,SEXP,SEXP);
SEXP rh_combine_kv(SEXP,SEXP,SEXP);
SEXP rh_status(SEXP);
SEXP rh_counter(SEXP,SEXP);
void spill_to_reducer(void);

/*********************************
 * Defined in display.cc
 ********************************/
SEXP readFromMem(void * ,uint32_t );
/****************************************
 * Signal Handler Related, in display.cc
 ***************************************/
typedef void Sigfunc(int);
Sigfunc *signal(int , Sigfunc *);
Sigfunc *Signal(int , Sigfunc *);
void signal_handler(int );
void CaptureLog(google::protobuf::LogLevel , const char*, int,const string& message);


extern "C" {
  void exitR(void);
  int embedR(int ,char **);
  int voidevalR(const char* );
  void Re_ShowMessage(const char*);
  void Re_WriteConsoleEx(const char *, int , int );
  JNIEnv *JNU_GetEnv();
  void JNU_throw_general_exception(const char *, const char *) ;
  void JNU_throw_R_exception(const char *) ;
  void JNU_system_print(const char *,int );
  void JNU_mcount(const char* , const char* , int );
  void JNU_mstatus(const char* );
}
#endif
