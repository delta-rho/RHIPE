#ifndef	__ream_h
#define	__ream_h

#include <iostream>
#include <vector>
#include <map>

#include <google/protobuf/stubs/common.h>
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
#include <stdarg.h>
using namespace std;

#define R_NO_REMAP
#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1

#include <Rversion.h>
#include <R.h>
//#include <Rdefines.h>
#include <Rinternals.h>
#include <Rembedded.h>
#include <R_ext/Boolean.h>
#include <R_ext/Parse.h>
#include <R_ext/Rdynload.h>



// GLOBAL VARIABLES DEFINED IN VARIOUS FILES

extern map<string, vector<string> > map_output_buffer;
extern uint32_t spill_size;
extern uint32_t total_count;
/* extern SEXP comb_pre_red,comb_do_red,comb_post_red; */
extern  const char* REDUCESETUP;
extern  const char* REDUCEPREKEY;
extern  const char* REDUCE;
extern  const char* REDUCEPOSTKEY ;
extern  const char* REDUCECLEANUP;
extern const char* MAPSETUPS;
extern const char* MAPRUNNERS;
extern const char* MAPCLEANS;
extern bool combiner_inplace;




//MESSAGE CODES FROM JAVA
//REALLY SHOULD BE UNIQUE BUT THAT REQUIRES REWRITING THE JAVA CODE
#define EVAL_SETUP_MAP  -1
#define EVAL_CLEANUP_MAP -2
#define EVAL_SETUP_REDUCE -1
#define EVAL_REDUCE_PREKEY -2
#define EVAL_REDUCE_POSTKEY -3
#define EVAL_REDUCE_THEKEY -4
#define EVAL_CLEANUP_REDUCE -5
#define EVAL_FLUSH -10

//ADDED BY JEREMIAH ROUNDS TO INDICATE A PROBLEM IN THE COMMUNICATION
//SHOULD NOT BE ANY OF THE ABOVE
#define RHIPE_PIPE_READ_ERROR -10000
#define RHIPE_PIPE_READ_FULL 10000
#define RHIPE_PIPE_READ_EMPTY 0

//ADDED BY JEREMIAH ROUNDS
//THESE ARE RHIPEWHAT VALUES
#define RHIPEWHAT_MAPPER 0
#define RHIPEWHAT_REDUCER 1
#define RHIPEWHAT_REDUCER_PERHAPS_UNUSED 2



#define DLEVEL -9

#ifdef FILEREADER
extern FILE *FILEIN;
#endif

#ifdef RHIPEDEBUG
#define LOGG(...) logg(__VA_ARGS__)
#else
#define LOGG(...)
#endif

#ifdef USETIMER
#include "time.h"
#include <sys/time.h>
extern long int collect_total;
extern long int collect_buffer_total;
extern long int time_in_reval;
extern long int collect_spill_total;
extern long int time_in_reduce_reval;
SEXP TIMER_REDUCE_R_tryEval(SEXP, SEXP, int *);
SEXP TIMER_R_tryEval(SEXP, SEXP, int *);

#define WRAP_R_EVAL TIMER_R_tryEval
#define WRAP_R_EVAL_FOR_REDUCE TIMER_REDUCE_R_tryEval
#else
#define WRAP_R_EVAL R_tryEval
#define WRAP_R_EVAL_FOR_REDUCE R_tryEval
#endif

/* extern void (*ptr_R_ShowMessage)(const char *); */
/* extern void (*ptr_R_WriteConsole)(const char *, int); */
/* extern int  (*ptr_R_ReadConsole)(char *, unsigned char *, int, int); */
/* extern void (*ptr_R_WriteConsoleEx)(const char *, int , int ); */
/* extern FILE* R_Consolefile; */
/* extern FILE* R_Outputfile;  */
extern FILE* LOG;
extern int _STATE_;

/*
 * MESSAGES.cc
 */
SEXP rexpress(const char*);
void sexpToRexp(REXP *, const SEXP);
void fill_rexp(REXP *, const SEXP );
SEXP rexpToSexp(const REXP&);
void writeSexp32(FILE* fout, REXP* prexp_buffer, SEXP obj);
void writeSexp64(FILE* fout, REXP* prexp_buffer, SEXP obj);






/*********
 * Utility
 *********/
/* void CaptureLog(LogLevel , const char* , int ,const string& ) ; */
/* void CaptureLogInLibrary(LogLevel , const char* , int ,const string& ) ; */

uint32_t nlz(const int64_t);
uint32_t getVIntSize(const int64_t) ;
uint32_t isNegativeVInt(const int8_t);
uint32_t decodeVIntSize(const int8_t);
uint32_t reverseUInt (uint32_t );
void writeVInt64ToFileDescriptor( int64_t , FILE* );
void writeUInt32(FILE* fout, uint32_t value);
int64_t readVInt64FromFileDescriptor(FILE* );
int64_t readVInt64FromFD(int );

int32_t readJavaInt(FILE* );

/************************
 * Signal Handler Related
 ************************/
typedef void Sigfunc(int);
Sigfunc *signal_ours(int , Sigfunc *);
Sigfunc *Signal(int , Sigfunc *);
void sigHandler(int );


/*************
 ** Tests
 ************/
void doTest_Serialize2String(char *,const int );
void doTest_Serialize2Char(char *,const int);
void doTest_Serialize2FD(char *,const int );



/*****************
 ** File Pointers
 ****************/
struct Streams {
  FILE* BSTDERR,*BSTDIN,*BSTDOUT;
  int NBSTDERR,NBSTDIN,NBSTDOUT;
};
extern Streams *CMMNC;
int setup_stream(Streams *);


/*****************
 ** writen,Readn
 ****************/
ssize_t readn(int , void *, size_t );
ssize_t Readn(int , void *, size_t);
ssize_t writen(int , const void *, int );
void do_unser(void);
/******************
 ** Map & Reduce
 *****************/
extern "C" SEXP execMapReduce();
int readToKeyValueBuffers(FILE* fin, SEXP keys, SEXP values, int max_keyvalues,int32_t max_bytes_to_read,int* actual_keyvalues,int* reason) ;
void shallowCopyVector(SEXP,SEXP);
void setupCombiner();
void cleanupCombiner();
void setupMapper();
void cleanupMapper();

const int old_mapper_run2(void);
const int mapper_run(void);
const int mapper_setup(void);
const int reducer_run(void);
const int reducer_setup(void);

/*****************
 ** Displays
 *****************/
void Re_ShowMessage(const char*);
void Re_WriteConsoleEx(const char *, int , int );
void merror(const char *, ...);
void mmessage(char *fmt, ...);
void logg(int , const char *, ...);

/******************
 ** Counter/Collect
 *****************/
extern "C" {
  SEXP counter(SEXP );
  SEXP status(SEXP );
  SEXP collect(SEXP ,SEXP );
  SEXP collect_buffer(SEXP ,SEXP );
}

SEXP collectList(SEXP ,SEXP );
SEXP readFromHadoop(const uint32_t,int* );
SEXP readFromMem(void * ,uint32_t );
SEXP persUnser(SEXP);
SEXP persSer(SEXP);
SEXP dbgstr(SEXP);
void spill_to_reducer(void);
void mcount(const char *,const char*, uint32_t);


extern  R_CallMethodDef callMethods[];


#endif
