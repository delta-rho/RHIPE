#include "ream.h"
#include <Rinterface.h>
#include <iostream>


using namespace std;

#if (R_VERSION < R_Version(3,2,0))
extern int R_running_as_main_program;
#endif

extern uintptr_t R_CStackLimit;





/***************************************************
 **
 ** Stream Setup
 **
 ***************************************************/
Streams *CMMNC;
int setup_stream(Streams *s){
   if (!(s->BSTDOUT = freopen(NULL, "wb", stdout))){
    fprintf(stderr,"ERROR: Could not reopen standard output in binary mode");
    return(-1);
  }
  if (!(s->BSTDIN = freopen(NULL, "rb", stdin))){
    fprintf(stderr,"ERROR: Could not reopen standard input in binary mode");
    return(-1);
  }
  if (!(s->BSTDERR = freopen(NULL, "wr", stderr))){
    fprintf(stderr,"ERROR: Could not reopen standard error in binary mode");
    return(-1);
  }
  char *buffsizekb;
  int buffs=1024*10;
  if ((buffsizekb=getenv("rhipe_stream_buffer")))
    buffs = (int)strtol(buffsizekb,NULL,10);

#ifndef FILEREADER
  setvbuf(s->BSTDOUT, 0, _IOFBF , buffs);
  setvbuf(s->BSTDIN,  0, _IOFBF,  buffs);
  setvbuf(s->BSTDERR, 0, _IONBF , 0);
#endif
  s->NBSTDOUT = fileno(s->BSTDOUT);
  s->NBSTDIN =  fileno(s->BSTDIN);
  s->NBSTDERR = fileno(s->BSTDERR);
  return(0);
}




void Re_ResetConsole()
{
}
void Re_FlushConsole()
{
}
void Re_ClearerrConsole()
{
}


int embedR(int argc, char **argv){

  structRstart rp;
  Rstart Rp = &rp;

  if (!getenv("R_HOME")) {
    fprintf(stderr, "R_HOME is not set. Please set all required environment variables before running this program.\n");
    return(-1);
  }

  R_running_as_main_program = 1;
  R_DefParams(Rp);
  Rp->NoRenviron = 0;
  Rp->R_Interactive = (Rboolean)1;
  R_SetParams(Rp);
  R_SignalHandlers=0;
  R_CStackLimit = (uintptr_t)-1;


  int stat= Rf_initialize_R(argc, argv);
  if (stat<0) {
    fprintf(stderr,"Failed to initialize embedded R!:%d\n",stat);
    return(-2);
  }
  R_SignalHandlers=0;
  R_CStackLimit = (uintptr_t)-1;

  R_Outputfile = NULL;
  R_Consolefile = NULL;
  R_Interactive = (Rboolean)1;

  //Function pointers to rewritten functions in display.cc
  ptr_R_ShowMessage = Re_ShowMessage;
  ptr_R_WriteConsoleEx =Re_WriteConsoleEx;

  ptr_R_WriteConsole = NULL;
  ptr_R_ReadConsole = NULL;

  // ptr_R_ReadConsole = NULL;
  // ptr_R_ResetConsole = Re_ResetConsole;;
  // ptr_R_FlushConsole = Re_FlushConsole;
  // ptr_R_ClearerrConsole = Re_ClearerrConsole;

  // ptr_R_Busy = NULL;
  // ptr_R_ShowFiles = NULL;
  // ptr_R_ChooseFile = NULL;
  // ptr_R_loadhistory = NULL;
  // ptr_R_savehistory = NULL;



  Signal(SIGPIPE,sigHandler);
  // Signal(SIGQUIT,sigHandler);
  // Signal(SIGCHLD,sigHandler);
  // Signal(SIGHUP,sigHandler);
  // Signal(SIGTERM,sigHandler);
  // Signal(SIGINT,sigHandler);
  setup_Rmainloop();
  return(0);
}


void quitR(){
   R_RunExitFinalizers();
   Rf_KillAllDevices();
   R_CleanTempDir();
   fflush(NULL);

}

extern "C" int main(int argc,char **argv){
	//everything is hard coded to get streams from CMMNC so it has to be set up first.
	//Specifically rewritten Re_WriteConsole in display.cc
	CMMNC = (Streams*) malloc(sizeof(Streams));
	setup_stream(CMMNC);


	if (embedR(argc,argv) != 0) exit(101);
	execMapReduce();
	quitR();


	free(CMMNC);
	return(0);
 }


