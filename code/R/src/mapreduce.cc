#include "ream.h"

using namespace std;
using namespace google::protobuf;

R_CallMethodDef callMethods [] = {
  {"rh_counter",(DL_FUNC) counter,1},
  {"rh_status",(DL_FUNC) status,1},
  {"rh_collect",(DL_FUNC) collect,2},
  {"rh_collect_buffer",(DL_FUNC) collect_buffer,2},
  {"rh_uz",(DL_FUNC) persUnser,1},
  {"rh_dbgstr",(DL_FUNC) dbgstr,1},
  {NULL, NULL, 0}
};




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
#ifdef USETIMER
long int collect_buffer_total ,collect_total ,time_in_reval ,collect_spill_total,time_in_reduce_reval;
#endif


void CaptureLog(LogLevel level, const char* filename, int line,
                const string& message) {
  static const char* pb_log_level[] = {"LOGLEVEL_INFO","LOGLEVEL_WARNING",
				"LOGLEVEL_ERROR","LOGLEVEL_FATAL",
				"LOGLEVEL_DFATAL" };
  merror("PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename,line, message.c_str());
}


extern "C" {

SEXP execMapReduce(){
#ifndef FILEREADER
  char *rhipewhat;
  if ((rhipewhat=getenv("RHIPEWHAT")) != NULL){
    _STATE_ = (int)strtol(rhipewhat,NULL,10);
  }else{
	  merror("RHIPEWHAT must be defined in the environment.");
	  return(R_NilValue);
  }
#else
  _STATE_=0;
#endif
#ifdef FILEREADER
  FILEIN = fopen("/ln/mapinput","rb");
  if(!FILEIN){
    printf("Could not not find file /ln/mapinput\n");
    return(R_NilValue);
  }
#endif
  int uid = geteuid();
#ifdef RHIPEDEBUG
  char fn[1024];
  char *logfile=NULL;
  if( (logfile=getenv("RHIPELOGFILE")) )
    snprintf(fn,1023,"%s.euid-%d.pid-%d.log",logfile,uid,getpid());
  else
    snprintf(fn,1023,"/tmp/rhipe.euid-%d.pid-%d.log",uid,getpid());
  LOG=fopen(fn,"w");
  LOGG(10,"\n.....................\n");
  LOGG(10,"Starting Up\n");
  fflush(LOG);
#endif
  CMMNC = (Streams*)malloc(sizeof(Streams));
  setup_stream(CMMNC);
  LOGG(10,"R Embedded\n");
   GOOGLE_PROTOBUF_VERIFY_VERSION;
   LOGG(9,"Google Protobuf version verified\n");


   LOGG(9,"R MainLoop Running\n");

   DllInfo *info = R_getEmbeddingDllInfo();
   R_registerRoutines(info, NULL, callMethods, NULL, NULL);
   LOGG(9,"Registed functions\n");


   // Load the functions into the R global environmen
   // rexpress("options(width=200)");
   rexpress("rhstatus<-function(string) .Call('rh_status',string);");
   rexpress("rhcounter<-function(group,counter='',n=1) .Call('rh_counter',list(group,counter,n))");
   if(!strcmp(getenv("rhipe_outputformat_class"),"org.apache.hadoop.mapreduce.lib.output.NullOutputFormat"))
     rexpress("rhcollect<-function(key,value) {}");
   else
     rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");

   LOGG(9,"Loaded R Wrappers\n");




   LOGG(10,"STD{IN,OUT,ERR}  in binary \n");



   int ret=0;
 #ifdef USETIMER
   struct timeval tms;
   long int bstart, bend;
   collect_buffer_total = collect_total =  time_in_reval =collect_spill_total = time_in_reduce_reval =0;
 #endif
   LOGG(10,"Running in STATE=%d\n",_STATE_);
   google::protobuf::SetLogHandler(&CaptureLog);
   rexpress("Sys.setenv(rhipe_iscombining=0);rhipe_iscombining=FALSE");
   combiner_inplace=false;
   
   
   
   switch(_STATE_){
   case 0:
     {
 #ifndef FILEREADER
       /*
 	This is the mapper, but if the combiner is TRUE
 	we run the reduce setup code too
 	we check if combiner is set and if so
 	read in the value of io.sort.mb
       */
       if((int)strtol(getenv("rhipe_combiner"),NULL,10)==1){
		 	combiner_inplace = true;
		 	spill_size = (uint32_t)((strtod(getenv("io_sort_mb"),NULL)*1024*1024));
		 	rexpress(".rhipe.current.state<-'map.combine';rhcollect<-function(key,value) .Call('rh_collect_buffer',key,value)");
		 	SEXP reducesetup;
		 	int Rerr=0;
		 	PROTECT(reducesetup=rexpress(REDUCESETUP));
		 	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),reducesetup),NULL,&Rerr);
		 	UNPROTECT(1);
       }
       if ((ret=mapper_setup())!=0){
 			LOGG(12,"FAILURE IN MAP SETUP:%d\n",ret);
 			merror("Error while running mapper setup: %d\n",ret);
 	// exit(ret);
 			break;
       }
 #endif
 #ifdef USETIMER
       gettimeofday(&tms,NULL);
       bstart = tms.tv_sec*1000000 + tms.tv_usec;
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
 #ifdef USETIMER
       gettimeofday(&tms,NULL);
       bend = tms.tv_sec*1000000 + tms.tv_usec;
       char x[512];
       sprintf(x,"Timers-%s-%d",getenv("mapred.task.id"),getpid());
       mcount(x,"MapTime",bend-bstart);
       mcount(x,"TotalCollect",collect_total);
       mcount(x,"TotalCollectBuf",collect_buffer_total);
       mcount(x,"TotalinR",time_in_reval);
       mcount(x,"TotalinR-Reduce",time_in_reduce_reval);
       mcount(x,"TotalinSpill",collect_spill_total);
 #endif
     }
     break;
   case 1:
   case 2:
     {
 #ifdef RHIPEDEBUG
       {
 	char *cdd;
 	cdd = getenv("RHIPE_BUG_CODE");
 	if(cdd){
 	  rexpress(cdd);
 	}
       }
 #endif
       if ((ret=reducer_run())!=0){
 	LOGG(12,"FAILURE IN REDUCER:%d\n",ret);
 	merror("Error while running reducer: %d\n",ret);
 	break;
       }
     }
     break;
   default:
     merror("Bad value for RHIPEWHAT: %d\n",_STATE_);
     break;
     }



   // if(CMMNC)
   //   free(CMMNC); //crashes when run in cmd line with STATE=1
   LOGG(10,"Now shutting down log:\n");
 #ifdef RHIPEDEBUG
   fclose(LOG);
 #endif
   fflush(NULL);
   return(R_NilValue);
}

}//close extern "C"
