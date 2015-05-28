#include "ream.h"

using namespace std;
using namespace google::protobuf;

R_CallMethodDef callMethods [] = {
  {"rh_counter",(DL_FUNC) counter,1},
  {"rh_status",(DL_FUNC) status,1},
  {"rh_collect",(DL_FUNC) collect,2},
  {"rh_vcollect",(DL_FUNC) collectList,2},
  {"rh_collect_buffer",(DL_FUNC) collect_buffer,2},
  {"rh_uz",(DL_FUNC) persUnser,1},
  {"rh_sz",(DL_FUNC) persSer,1},
  {"rh_dbgstr",(DL_FUNC) dbgstr,1},
  {NULL, NULL, 0}
};





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

// from mapper.cc
#ifdef FILEREADER
/* fixed types*/
char* MAPRUNNERS =
  "expression({"
  "print(length(map.keys)); print(map.values);"
  "  v <- lapply(seq_along(map.keys),function(r) {"
  "      rhcollect(map.keys[[r]],map.values[[r]])"
  "})})";
char* MAPSETUPS = "expression()";
char* MAPCLEANS = "expression()";
static int _counter_=0;
#else
// const char* MAPSETUPS = "unserialize(charToRaw(Sys.getenv('rhipe_setup_map')))";
// const char* MAPRUNNERS = "unserialize(charToRaw(Sys.getenv('rhipe_map')))";
// const char* MAPCLEANS = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_map')))";
const char* MAPSETUPS = "{ff <- Sys.getenv('rhipe_setup_map'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* MAPRUNNERS = "{ff <- Sys.getenv('rhipe_map'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* MAPCLEANS = "{ff <- Sys.getenv('rhipe_cleanup_map'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
#endif


void CaptureLog(LogLevel level, const char* filename, int line,
                const string& message) {
  static const char* pb_log_level[] = {"LOGLEVEL_INFO","LOGLEVEL_WARNING",
				"LOGLEVEL_ERROR","LOGLEVEL_FATAL",
				"LOGLEVEL_DFATAL" };
  Rf_error("PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename,line, message.c_str());
}



/* shallowCopyVector
 * param: src
 * param: dest
 * param: n
 * shallow copies the first n elements of src to dest.  Both must have at least n elements allocated.
 */
void shallowCopyVector(SEXP src, SEXP dest, int n) {
	for (int i = 0; i < n; ++i) {
		SET_VECTOR_ELT(dest, i, VECTOR_ELT(src, i));
	}

}
/*
 * readToKeyValueBuffers
 * The reason I spun this out is I intended to use it from the reducer...
 * but there are issues with that point of view related to the command and control between the java classes
 * and the C code.  Basically the C code waits for an order from the Java code before it does anything.
 * That wasn't really noticable from the mapper, but in the reducer it is much more noticable.
 * param: fin file to read in from NOT CURRENTLY USED.
 * param: keys  preallocated vector of length at least nkv
 * param: values preallocates vector of length at least nkv
 * param: max_keyvalues number of key value pairs to read into buffers keys and values
 * param: actua_keyvalues number of key value pairs actually read in. NOTE this gets changed as a side effect.
 * return:
 *         if error RHIPE_PIPE_READ_BAD
 *         otherwise if nbytes is ever negative at the start of a read of key,value returns that negative.
 *         Those negative numbers are messages from the java classes.
 *         if the stream ends without a bad key,value read returns 0.
 *         if no error, empty streams, and no negative numbers appear it returns RHIPE_PIPE_READ_FULL
 *         Note that if a negative is read it is still a good pipe until a zero is returned.
 * NOTE: Function provides core read logic for Mappers and Reducers
 * author: roundsjeremiah@gmail.com
 */
int readToKeyValueBuffers(FILE* fin, SEXP keys, SEXP values, int32_t max_bytes_read,int max_keyvalues,
			  int* actual_keyvalues,int* reason) {
	int32_t nbytes;
	SEXP k = R_NilValue, v = R_NilValue;
	int err;
	int i;
	*actual_keyvalues = 0;
	int32_t bytesread=0;
	*reason = 0;
	for (i = 0; i < max_keyvalues; ++i) {
		nbytes = readVInt64FromFileDescriptor(fin);
		//IT IS NOT BYTES BUT A MESSAGE IF THIS IS TRUE:
		if (nbytes <= 0) {
			*actual_keyvalues = i;
			return nbytes;
		}
		bytesread += nbytes;
		PROTECT(k = readFromHadoop(nbytes,&err));
		if (err) {
			*actual_keyvalues = i;
			UNPROTECT(1);
			return RHIPE_PIPE_READ_ERROR;
		}
		nbytes = readVInt64FromFileDescriptor(fin);
		if (nbytes <= 0) { //error because a negative number should only be between Key,Value pairs
			*actual_keyvalues = i;
			UNPROTECT(1);
			return RHIPE_PIPE_READ_ERROR;
		}
		bytesread += nbytes;
		PROTECT(v = readFromHadoop(nbytes,&err));
		if (err) {
			*actual_keyvalues = i;
			UNPROTECT(2);
			return RHIPE_PIPE_READ_ERROR;
		}
		SET_VECTOR_ELT(keys, i, k);
		SET_VECTOR_ELT(values, i, v);
		UNPROTECT(2);
#ifdef USEAUTOSHORT
		if(bytesread >= max_bytes_read) {
		  *reason = 1;
		  break;
		}
#endif
	}
	if(i < max_keyvalues)
	  *actual_keyvalues = i+1;
	else
	  *actual_keyvalues = max_keyvalues;
	return RHIPE_PIPE_READ_FULL;
}

/*
 * setupCombiner
 * This is logically distinct from a reducer setup but it uses reduce setup for the time being.
 * Decided also the inspection for environmental variables should happen here.
 */
void setupCombiner(){
	//inspect environmental variables
	combiner_inplace = false;
	if ((int) strtol(getenv("rhipe_combiner"), NULL, 10) == 1)
		combiner_inplace = true;

	if(!combiner_inplace)
		return;


	//If we are here we are using a spill combiner and need to setup for it.

	spill_size = (uint32_t) ((strtod(getenv("io_sort_mb"), NULL) * 1024* 1024));
	rexpress(".rhipe.current.state<-'map.combine';rhcollect<-function(key,value) .Call('rh_collect_buffer',key,value)");
	SEXP reducesetup;
	int Rerr = 0;
	PROTECT(reducesetup=rexpress(REDUCESETUP));
	R_tryEval(Rf_lang2(Rf_install("eval"), reducesetup), NULL, &Rerr);
	UNPROTECT(1);
}
/*
 * cleanupCombiner
 * Runs after we are done with mapper and are shutting down.
 * This is an opportunity to run reduce cleanup code since we ran reduce setup code.
 * We don't currently run a reduce cleanup, so I won't add it yet.
 * Mainly we make sure the spill buffer is fully worked before leaving this
 */
void cleanupCombiner(){


	rexpress(
			"rhcollect<-function(key,value) .Call('rh_collect',key,value)");
	if (!map_output_buffer.empty() || total_count > 0) { //mmessage("\n\n SPILLING LEFTOVER\n\n ");
		mcount("rhipeInternal", "combine_bytes_sent", total_count);
		spill_to_reducer(); //do i need total_count >0 ?
		fflush(NULL);
	}

}
/*
 * cleanupMap
 * Spun this out as its own function for future effort.
 */
void cleanupMapper(){
	SEXP cleaner;
	int Rerr;

	//do we really need to evaluate type to see if we should be doing this?  Don't see why...
	Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.cleanup")),R_GlobalEnv);
	PROTECT(cleaner=rexpress(MAPCLEANS));
	R_tryEval(Rf_lang2(Rf_install("eval"),cleaner),NULL,&Rerr);
	UNPROTECT(1);


}





/*
 * mainMapperLoop is new mapper from old logic.
 * It is no longer hard coded to streams
 * input: in File pointer where to read from.
 * return: ints... all that I am sure of is 0 is not an error.
 *         The rest of the returns are weakly documented.
 *
 */
int mainMapperLoop(FILE* fin){
	  //indicate state to R.
	  Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map")),R_GlobalEnv);

	  int32_t type=0;
	  int32_t buffer_size=0;
	  SEXP runner1,runner2,kvector,vvector;
	  char * mapbustr;
	  int protect= 0;
	  int32_t  max_bytes_to_read = 0;

	  //CREATE MAP EXPRESSION LANGUAGE OBJECTS
	  PROTECT(runner1=rexpress(MAPRUNNERS));
	  PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
	  protect += 2;

	  if(runner2==NILSXP){
	    merror("RHIPE ERROR: Could not create mapper\n");
	    UNPROTECT(protect);
	    return(4);
	  }

	  //GET MAP BUFFER SIZE
	  if ((mapbustr=getenv("rhipe_map_buff_size"))){
	    buffer_size = (int)strtol(mapbustr,NULL,10);
	  }else{
	    buffer_size = 10000;
	  }

	  // Get How many Bytes to Read (so we don't adjust rhipe_map_buff_size)
	  if ((mapbustr=getenv("rhipe_map_bytes_read"))){
	    max_bytes_to_read = (int)strtol(mapbustr,NULL,10);
	  }else{
	    max_bytes_to_read = 150*1024*1024;
	  }
	  //CREATE PRIMARY BUFFERS
	  PROTECT(kvector = Rf_allocVector(VECSXP,buffer_size));
	  PROTECT(vvector = Rf_allocVector(VECSXP,buffer_size));
	  protect += 2;
	  Rf_defineVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	  Rf_defineVar(Rf_install("map.values"),vvector,R_GlobalEnv);

	  int Rerr;
	  int nread;
	  int reason;

	  //READ AND EXEC LOOP
	  while(TRUE){
	    type = readToKeyValueBuffers(fin,kvector,vvector,max_bytes_to_read,buffer_size,&nread,&reason);
	    if(0 < nread  && nread < buffer_size){
	      //CORNER CASE DID NOT FILL BUFFER
	      //allocate a new vector with appropriate length.
	      //maybe inefficient but should be a corner case at the end of each Mapper.
	      SEXP temp_keys, temp_values;
	      mcount("rhipeInternal","partialFlush",1);
	      PROTECT(temp_keys   = Rf_allocVector(VECSXP,nread));
	      PROTECT(temp_values = Rf_allocVector(VECSXP,nread));
	      shallowCopyVector(kvector, temp_keys,nread);
	      shallowCopyVector(vvector, temp_values,nread);
	      Rf_setVar(Rf_install("map.keys"),temp_keys,R_GlobalEnv);
	      Rf_setVar(Rf_install("map.values"),temp_values,R_GlobalEnv);
	      R_tryEval(runner2,NULL,&Rerr);  //do something with the error?
	      UNPROTECT(2);
#ifdef USEAUTOSHORT
	      if(reason == 1){
		UNPROTECT(2);
		buffer_size = nread;
		mcount("rhipeInternal","bufferShrink",1);
		PROTECT(kvector = Rf_allocVector(VECSXP,buffer_size));
		PROTECT(vvector = Rf_allocVector(VECSXP,buffer_size));
	      }
#endif
	    }else if (nread == buffer_size){
#ifdef USEAUTOSHORT
	      mcount("rhipeInternal","completeFlush",1);
#endif
	      //PRIMARY BUFFER IS USUABLE BY CLIENT
	      Rf_setVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	      Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	      R_tryEval(runner2,NULL,&Rerr);  //do something with the error?
	    }
	    fflush(NULL);
	    if(type == EVAL_CLEANUP_MAP)
	      break;
	    if(type == RHIPE_PIPE_READ_ERROR){
	      merror("Error bad pipe read between RhipeMapReduce and RHMRMapper\n");
	      break;
	    }
	    if(type == RHIPE_PIPE_READ_EMPTY)  // I don't understand how this isn't a race condition, but it was in the original logic.
	      break;
	  }
	  //do we really need to know type == EVAL_CLEANUP_MAP before cleaning up?
	  //I don't think so, so I am moving cleanupMap to execMapperWithCombiner()
	  // if(type == EVAL_CLEANUP_MAP)
	 //	  cleanupMap();
	  UNPROTECT(protect);
	  fflush(NULL);
	  return(0);
}



/*
 * execMapperWithCombiner
 * The WithCombiner is to remind us that we sometimes combine.
 * Most of the combining logic is actually in collect_buffer.
 * We only have setup and cleanup here.
 * input: fin what file to take Map Key,Value pairs from.  Currently not used.
 */

void execMapperWithCombiner(FILE* fin){
	int ret;
	setupCombiner();
	if ((ret = mapper_setup()) != 0) {
		LOGG(12,"FAILURE IN MAP SETUP:%d\n",ret);
		merror("Error while running mapper setup: %d\n", ret);
		return;
	}

	if ((ret = mainMapperLoop(fin)) != 0) {
		LOGG(12,"FAILURE IN MAP RUN:%d\n",ret);
		merror("Error while running mapper: %d\n", ret);
		return;
	}
	cleanupMapper(); //we always run this.  We don't need permission from Java classes.
	cleanupCombiner();


}

/*
 * execReducer
 * Exist to mirror execMapperWithCombiner
 * but the design of the relationship between Java and C in the reducer is involved.
 * The C code basically waits for byte instruction codes from the Java classes.
 */
void execReducer(FILE* fin){
	int ret;
	if ((ret = reducer_run()) != 0) {
			LOGG(12,"FAILURE IN REDUCER:%d\n",ret);
			merror("Error while running reducer: %d\n", ret);
	}

}

// from mapper.cc
const int mapper_setup(void){
  int32_t type = 0;
  SEXP setupm;
  int Rerr;
  type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);

  if(type==EVAL_SETUP_MAP){
    Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.setup")),R_GlobalEnv);
    PROTECT(setupm=rexpress(MAPSETUPS));
    WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),setupm),NULL,&Rerr);
    UNPROTECT(1);
    if(Rerr) return(7);
  }
  else {
    merror("RHIPE ERROR: What command is this for setup: %d ?\n",type);
    return(8);
  }

  return(0);
}

extern "C" {

/*
 *  execMapReduce
 *  Launches the MapReduce logic for Rhipe
 *  author: Jeremiah Rounds
 *  Note: spun out from Saptarshi's original source.
 *
 */
SEXP execMapReduce() {


#ifndef FILEREADER
	char *rhipewhat;
	if ((rhipewhat = getenv("RHIPEWHAT")) != NULL) {
		_STATE_ = (int) strtol(rhipewhat, NULL, 10);
	} else {

		fprintf(stderr,"Appears environment variables are not set for RhipeMapReduce (checked RHIPEWHAT).\n");
		fprintf(stderr,"Shutting Down.\n");
		return (R_NilValue);
	}
#else
	_STATE_=0;
#endif
#ifdef FILEREADER
	FILEIN = fopen("/ln/mapinput","rb");
	if(!FILEIN) {
		printf("Could not not find file /ln/mapinput\n");
		return(R_NilValue);
	}
#endif
	//int uid = geteuid();
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
	rexpress("rhreturn<-function(r) .Call('rh_collect',.Internal(runif(1, 0, 1)),r);");

	rexpress(
			"rhcounter<-function(group,counter='',n=1) .Call('rh_counter',list(group,counter,n))");
	if (!strcmp(getenv("rhipe_outputformat_class"),
			"org.apache.hadoop.mapreduce.lib.output.NullOutputFormat"))
		rexpress("rhcollect<-function(key,value) {}");
	else{
	  rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");
	  rexpress("rhvcollect<-function(key,value) .Call('rh_vcollect',key,value)");
	}


	LOGG(9,"Loaded R Wrappers\n");

	LOGG(10,"STD{IN,OUT,ERR}  in binary \n");

	//int ret = 0;
#ifdef USETIMER
	struct timeval tms;
	long int bstart, bend;
	collect_buffer_total = collect_total = time_in_reval =collect_spill_total = time_in_reduce_reval =0;
#endif
	LOGG(10,"Running in STATE=%d\n",_STATE_);
	google::protobuf::SetLogHandler(&CaptureLog);
	rexpress("Sys.setenv(rhipe_iscombining=0);rhipe_iscombining=FALSE");



	switch (_STATE_) {
	case RHIPEWHAT_MAPPER:
		execMapperWithCombiner(CMMNC->BSTDIN);
		break;
	case RHIPEWHAT_REDUCER:
	case RHIPEWHAT_REDUCER_PERHAPS_UNUSED:
		execReducer(CMMNC->BSTDIN);
		break;
	default:
		fprintf(stderr,"Bad value for RHIPEWHAT: %d\n",_STATE_);
		break;
	}

	// if(CMMNC)
	//   free(CMMNC); //crashes when run in cmd line with STATE=1
	LOGG(10,"Now shutting down log:\n");
#ifdef RHIPEDEBUG
	fclose(LOG);
#endif
	fflush(NULL);
	return (R_NilValue);
}

}//close extern "C"
