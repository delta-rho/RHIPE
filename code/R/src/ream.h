#ifndef	__ream_h
#define	__ream_h

#include <iostream>

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

#define R_NO_REMAP
#include <Rversion.h>
#include <R.h>
#include <Rdefines.h>
#include <Rinternals.h>
#include <Rinterface.h>
#include <Rembedded.h>
#include <R_ext/Boolean.h>
#include <R_ext/Parse.h>
#include <R_ext/Rdynload.h>
  
#define DLEVEL 9

#ifdef RHIPEDEBUG
#define LOGG(...) logg(__VA_ARGS__)
#else
#define LOGG(...)
#endif


extern void (*ptr_R_ShowMessage)(const char *);
extern void (*ptr_R_WriteConsole)(const char *, int);
extern int  (*ptr_R_ReadConsole)(char *, unsigned char *, int, int);
extern void (*ptr_R_WriteConsoleEx)(const char *, int , int );
extern FILE* R_Consolefile;
extern FILE* R_Outputfile; 
extern FILE* LOG;


SEXP rexpress(const char*);
void rexp2message(REXP *, const SEXP);
void fill_rexp(REXP *, const SEXP );
SEXP message2rexp(const REXP&);





/*********
 * Utility
 *********/
uint32_t nlz(const int64_t);
uint32_t getVIntSize(const int64_t) ;
uint32_t isNegativeVInt(const int8_t);
uint32_t decodeVIntSize(const int8_t);
uint32_t reverseUInt (uint32_t );
void writeVInt64ToFileDescriptor( int64_t , FILE* );
int64_t readVInt64FromFileDescriptor(FILE* );

/************************
 * Signal Handler Related
 ************************/
typedef void Sigfunc(int);
Sigfunc *signal(int , Sigfunc *);
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

/******************
 ** Map & Reduce
 *****************/
const int mapper_run(void);
const int mapper_setup(void);
const int reducer_run(void);
const int reducer_setup(void);

/*****************
 ** Displays
 *****************/
void Re_ShowMessage(const char*);
void Re_WriteConsoleEx(const char *, int , int );
void merror(char *, ...);
void mmessage(char *fmt, ...);
void logg(int , const char *, ...);

/******************
 ** Counter/Collect
 *****************/
void counter(SEXP );
void status(SEXP );
void collect(SEXP ,SEXP );
SEXP readFromHadoop(uint32_t);
static R_CallMethodDef callMethods [] = {
  {"rh_counter",(DL_FUNC) counter,1},
  {"rh_status",(DL_FUNC) status,1},
  {"rh_collect",(DL_FUNC) collect,2},
  {NULL, NULL, 0}
};

/*******************
 ** CONSTANTS
 ******************/
static uint8_t ERROR_MSG = 0x00;
static uint8_t PRINT_MSG = 0x01;
static uint8_t SET_STATUS = 0x02;
static uint8_t SET_COUNTER = 0x03;

const static int32_t  EVAL_SETUP_MAP =   -1;
const static int32_t  EVAL_CLEANUP_MAP = -2;

const static int32_t EVAL_SETUP_REDUCE = -1;
const static int32_t EVAL_REDUCE_PREKEY = -2;
const static int32_t EVAL_REDUCE_POSTKEY = -3;
const static int32_t EVAL_REDUCE_THEKEY = -4;
const static int32_t EVAL_CLEANUP_REDUCE = -5;



#endif
