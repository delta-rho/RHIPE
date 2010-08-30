#include "ream.h"
#include <iostream>
R_CallMethodDef callMethods [] = {
  {"rh_counter",(DL_FUNC) counter,1},
  {"rh_status",(DL_FUNC) status,1},
  {"rh_collect",(DL_FUNC) collect,2},
  {"rh_collect_buffer",(DL_FUNC) collect_buffer,2},
  {"rh_uz",(DL_FUNC) persUnser,1},
  {"rh_dbgstr",(DL_FUNC) dbgstr,1},
  {NULL, NULL, 0}
};


using namespace std;

Streams *CMMNC;
FILE* LOG;
#ifdef FILEREADER
FILE* FILEIN;
#endif
int _STATE_;
uint32_t spill_size;
map<string, vector<string> > map_output_buffer;
bool combiner_inplace;
SEXP comb_pre_red,comb_do_red,comb_post_red;

extern int R_running_as_main_program;
extern uintptr_t R_CStackLimit; 

using namespace google::protobuf;
void CaptureLog(LogLevel level, const char* filename, int line,
                const string& message) {
  static const char* pb_log_level[] = {"LOGLEVEL_INFO","LOGLEVEL_WARNING",
				"LOGLEVEL_ERROR","LOGLEVEL_FATAL",
				"LOGLEVEL_DFATAL" };
  merror("PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename,line, message.c_str());  
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

  return(0);
}

int main(int argc,char **argv){
#ifndef FILEREADER
  char *rhipewhat;
  if ((rhipewhat=getenv("RHIPEWHAT")))
    _STATE_ = (int)strtol(rhipewhat,NULL,10);
#else
  _STATE_=0;
#endif
#ifdef FILEREADER
  FILEIN = fopen("/ln/mapinput","rb");
  if(!FILEIN){
    printf("Could not not find file /ln/mapinput\n");
    exit(2001);
  }
#endif
  int uid = geteuid();
  char fn[256];
  sprintf(fn,"/tmp/logger-%d.log",uid);
  LOG=fopen(fn,"a");
  LOGG(10,"\n.....................\n");
  LOGG(10,"Starting Up\n");

  CMMNC = (Streams*)malloc(sizeof(Streams));
  setup_stream(CMMNC);
  
  if (embedR(argc,argv))
    {
      exit(101);
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
  // rexpress("options(width=200)");
  rexpress("rhstatus<-function(string) .Call('rh_status',string);");
  rexpress("rhcounter<-function(group,counter='',n=1) .Call('rh_counter',list(group,counter,n))");
  rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");
  LOGG(9,"Loaded R Wrappers\n");




  LOGG(10,"STD{IN,OUT,ERR}  in binary \n");



  int ret=0;
  
  LOGG(10,"Running in STATE=%d\n",_STATE_);
  google::protobuf::SetLogHandler(&CaptureLog);
  rexpress("Sys.setenv(rhipe_iscombining=0);rhipe_iscombining=FALSE");
  combiner_inplace=false;
  switch(_STATE_){
  case 0: 
    {
#ifndef FILEREADER
      /* we check if combiner is set and if so
	 read in the value of io.sort.mb
      */
      if((int)strtol(getenv("rhipe_combiner"),NULL,10)==1){
	combiner_inplace = true;
	spill_size = (uint32_t)((strtod(getenv("io.sort.mb"),NULL)*1024*1024));
	// mcount("combiner","spill_size",spill_size);
	// mcount("combiner","ncall",1);

	// mmessage("\n\nSPILL_SIZE==%d bytes\n\n",spill_size);
	// map_output_buffer = map<string, vector<string> >();
	rexpress("Sys.setenv(rhipe_iscombining=1);rhipe_iscombining=TRUE;rhcollect<-function(key,value) .Call('rh_collect_buffer',key,value)");
	SEXP reducesetup;
	int Rerr=0;
	PROTECT(reducesetup=rexpress(REDUCESETUP));
	R_tryEval(Rf_lang2(Rf_install("eval"),reducesetup),NULL,&Rerr);
	UNPROTECT(1);
	// SEXP dummy;
	// PROTECT(dummy=rexpress(REDUCEPREKEY));
	// PROTECT(comb_pre_red=Rf_lang2(Rf_install("eval"),dummy));
	// UNPROTECT(1);

	// PROTECT(dummy=rexpress(REDUCE));
	// PROTECT(comb_do_red=Rf_lang2(Rf_install("eval"),dummy));
	// UNPROTECT(1);
	
	// PROTECT(dummy=rexpress(REDUCEPOSTKEY));
	// PROTECT(comb_post_red=Rf_lang2(Rf_install("eval"),dummy));
	// UNPROTECT(1);
      }
      if ((ret=mapper_setup())!=0){
	LOGG(12,"FAILURE IN MAP SETUP:%d\n",ret);
	merror("Error while running mapper setup: %d\n",ret);
	// exit(ret);
	break;
      }
#endif
      if (((ret=mapper_run2())!=0)){
	LOGG(12,"FAILURE IN MAP RUN:%d\n",ret);
	merror("Error while running mapper: %d\n",ret);
	// exit(ret);
	break;
    }else{
      // if combiner is true, we must spill left over!
      if(combiner_inplace){
	rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");
	if(!map_output_buffer.empty() || total_count>0) { //mmessage("\n\n SPILLING LEFTOVER\n\n ");
	  mcount("combiner","bytesent",total_count);
	  spill_to_reducer(); //do i need total_count >0 ?
	  fflush(NULL);
	}
      }
    }
    }
    break;
  case 1:
  case 2:
    {
      // if ((ret=reducer_setup())!=0){
      // 	merror("Error while running reducer setup: %d\n",ret);
      // 	exit(4);
      // }
      if ((ret=reducer_run())!=0){
	LOGG(12,"FAILURE IN REDUCER:%d\n",ret);
	merror("Error while running reducer: %d\n",ret);
	// exit(ret);
	break;
      }
    }
    break;
  default:
    merror("Bad value for RHIPEWHAT: %d\n",_STATE_);
    break;
    }
  R_RunExitFinalizers();
  Rf_KillAllDevices();
  R_CleanTempDir();
  fflush(NULL);
  free(CMMNC);
  fclose(LOG);


  exit(ret);
    }

  
