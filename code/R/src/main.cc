#include "ream.h"
#include <iostream>


using namespace std;

Streams *CMMNC;
FILE* LOG;

int embedR(int argc, char *argv){
  structRstart rp;
  Rstart Rp = &rp;
  R_DefParams(Rp);
  Rp->NoRenviron = 0;
  Rp->R_Interactive = (Rboolean)1;
  R_SetParams(Rp);
  R_SignalHandlers=0;
#ifdef CSTACK_DEFNS
  R_CStackLimit = (uintptr_t)-1;
#endif
  if (!getenv("R_HOME")) {
    fprintf(stderr, "R_HOME is not set. Please set all required environment variables before running this program.\n");
    return(-1);
  }

  int stat= Rf_initialize_R(argc,(char **) argv);
  if (stat<0) {
    fprintf(stderr,"Failed to initialize embedded R!:%d\n",stat);
    return(-2);
  }

  R_Outputfile = NULL;
  R_Consolefile = NULL;
  R_Interactive = (Rboolean)1;
  ptr_R_ShowMessage = Re_ShowMessage;
  ptr_R_WriteConsoleEx =Re_WriteConsoleEx;

  ptr_R_WriteConsole = NULL;
  ptr_R_ReadConsole = NULL;

  Signal(SIGPIPE,sigHandler);
  Signal(SIGQUIT,sigHandler);
  Signal(SIGCHLD,sigHandler);
  Signal(SIGHUP,sigHandler);
  Signal(SIGTERM,sigHandler);
  Signal(SIGINT,sigHandler);

  return(0);
}

int main(int argc,char *argv){
  LOG=fopen("/tmp/logger","a");
  fprintf(LOG,"\n.....................\n");
  LOGG(10,"Starting Up\n");

  CMMNC = (Streams*)malloc(sizeof(Streams));
  setup_stream(CMMNC);

  if (embedR(argc,argv))
    {
      exit(1);
    }
  LOGG(10,"R Embedded\n");
  GOOGLE_PROTOBUF_VERIFY_VERSION;
  LOGG(9,"Google Protobuf version verified\n");

  setup_Rmainloop();
  LOGG(9,"R MainLoop Running\n");

  DllInfo *info = R_getEmbeddingDllInfo();
  R_registerRoutines(info, NULL, callMethods, NULL, NULL);
  LOGG(9,"Registed functions\n");


  // Load the functions into the R global environmen
  rexpress("rhstatus<-function(string) .Call('rh_status',string);");
  rexpress("rhcounter<-function(group,counter,n) .Call('rh_counter',paste(group,counter,n,sep=','))");
  rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");
  LOGG(9,"Loaded R Wrappers\n");




  LOGG(10,"STD{IN,OUT,ERR}  in binary \n");



  char *rhipewhat;
  int state=0;
  if (rhipewhat=getenv("RHIPEWHAT"))
    state = (int)strtol(rhipewhat,NULL,10);
  int ret=0;

  LOGG(10,"Running in STATE=%d\n",state);
  switch(state){
  case 0: 
    {
      if ((ret=mapper_setup())!=0){
	merror("Error while running mapper setup: %d\n",ret);
	exit(2);
      }
      if ((ret=mapper_run()!=0)){
	merror("Error while running mapper: %d\n",ret);
	exit(3);
      }
    }
    break;
  case 1:
    {
      if ((ret=reducer_setup())!=0){
	merror("Error while running reducer setup: %d\n",ret);
	exit(4);
      }
      if ((ret=reducer_run()!=0)){
	merror("Error while running reducer: %d\n",ret);
	exit(5);
      }
    }
    break;
  default:
    merror("Bad value for RHIPEWHAT: %d\n",state);
    break;
  }
  free(CMMNC);
  fclose(LOG);
  exit(0);
}

  
