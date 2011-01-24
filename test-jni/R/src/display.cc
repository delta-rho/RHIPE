#include <unistd.h>
#include "ream.h"
#include <google/protobuf/io/coded_stream.h>
using namespace google::protobuf::io;

#include <iostream>
uint32_t BSIZE= 32768;

static inline uint32_t tobytes(SEXP x,std::string* result){
  REXP r = REXP();
  rexp2message(&r,x);
  uint32_t size = r.ByteSize();
  r.SerializeToString(result);
  return(size);
}

SEXP rh_status(SEXP a){
  JNU_mstatus(CHAR(STRING_ELT(a,0)));
  return(R_NilValue);
}

SEXP rh_counter(SEXP a,SEXP b){
  JNU_mcount(CHAR(STRING_ELT(a,0)),CHAR(STRING_ELT(a,1)),INTEGER(b)[0]);
  return(R_NilValue);
}

void spill_to_reducer(void){
  rexpress("rhcollect <- function(k,v,buffer=TRUE) .Call('rh_spool_kv',k,v,buffer)");
  map<string, vector<string> >::iterator it;
  for(it=stored_jv_messages->map_output_buffer.begin(); it!=stored_jv_messages->map_output_buffer.end(); it++){
    string key = (*it).first;
    //we'll create reduce.key and reduce.values
    SEXP rkey, rvalues;
    REXP r = REXP();
    r.ParseFromArray((void*)key.data(),key.length());
    PROTECT(rkey = message2rexp(r));
    Rf_defineVar(Rf_install("reduce.key"),rkey,R_GlobalEnv);
    rexpress("eval(mapred.opts$reduce.pre)");
    int i;
    vector<string> values = (*it).second;
    vector<string>::iterator itvalue;
    PROTECT(rvalues =  Rf_allocVector(VECSXP,values.size()));
    for (i=0, itvalue=values.begin(); itvalue !=values.end(); itvalue++,i++ ){
      REXP v;
      string aval = (*itvalue);
      v.ParseFromArray((void*)aval.data(),aval.length());
      SET_VECTOR_ELT(rvalues, i, message2rexp(v));
    }
    Rf_defineVar(Rf_install("reduce.values"),rvalues,R_GlobalEnv);
    // I should actually store the parsed strings in stored_jv_messages
    // But compared to evaluating the code on vectors, this is a *tiny* cost
    // overall
    rexpress("eval(mapred.opts$reduce.reduce);eval(mapred.opts$reduce.post)");
    UNPROTECT(2);
  }
  rexpress("rhcollect <- function(k,v,buffer=TRUE) .Call('rh_combine_kv',k,v,buffer)");
}


SEXP rh_spool_kv(SEXP k,SEXP v,SEXP fast){
  stored_jv_messages->cdo_staged_writer(k,v);
  return R_NilValue;
}

SEXP rh_combine_kv(SEXP k,SEXP v,SEXP buffer){
  /***************************************************************************
   * In local run, this native library is loaded once and never loaded again,
   * unless the server is restarted
   **************************************************************************/

  static bool _once = false;
  static std::string *_ks;
  static std::string *_vs;
  static int _total_count;
  uint32_t ksize=0,vsize=0;
  if(!_once){
    _ks = new std::string();
    _vs = new std::string();
    _total_count = 0;
    _once = true;
  }
  _ks->clear();_vs->clear();
  ksize=tobytes(k,_ks);
  vsize=tobytes(v,_vs);
  _total_count += ksize+vsize;
  stored_jv_messages->map_output_buffer[*_ks].push_back(*_vs);
  if( _total_count >=  stored_jv_messages->combiner_spill_size) {
    JNU_mcount("combiner","bytesent",_total_count);
    spill_to_reducer();
    _total_count = 0;
    stored_jv_messages->map_output_buffer.clear();
  }
  return(R_NilValue);
}

/********************************
 * Blindly copied from stevens
 ********************************/

void sig_chld(int signo)
{
        pid_t   pid;
        int             stat;
        while ( (pid = waitpid(-1, &stat, WNOHANG)) > 0);
        return;
}

Sigfunc *signal(int signo, Sigfunc *func)
{
  struct sigaction      act, oact;
  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef  SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;       /* SunOS 4.x */
#endif
  } else {
#ifdef  SA_RESTART
    act.sa_flags |= SA_RESTART;         /* SVR4, 44BSD */
#endif
  }
  if (sigaction(signo, &act, &oact) < 0)
    return(SIG_ERR);
  return(oact.sa_handler);
}
Sigfunc * Signal(int signo, Sigfunc *func)      /* for our signal() function */
{
  Sigfunc *sigfunc;
  if ( (sigfunc = signal(signo, func)) == SIG_ERR)
    fprintf(stderr,"signal error");
  return(sigfunc);
}

void signal_handler(int i) {
  fprintf(stdout,"Signal:%d received\n",i);
  return;
}

using namespace google::protobuf;
void CaptureLog(LogLevel level, const char* filename, int line,
                const string& message) {
  static const char* pb_log_level[] = {"LOGLEVEL_INFO","LOGLEVEL_WARNING",
				       "LOGLEVEL_ERROR","LOGLEVEL_FATAL",
				       "LOGLEVEL_DFATAL" };
  char foo[1024];
  snprintf(foo,1022,"PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename,line, message.c_str());  
  JNU_throw_R_exception(foo);
}
