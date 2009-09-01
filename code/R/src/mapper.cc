#include "ream.h"
#import "iostream"
const char* MAPSETUPS = "unserialize(charToRaw(Sys.getenv('rhipe_setup_map')))";
const char* MAPRUNNERS = "unserialize(charToRaw(Sys.getenv('rhipe_map')))";
const char* MAPCLEANS = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_map')))";

const int mapper_run(void){

  int32_t type=0,ret=0;
  SEXP key,value;
  SEXP runner1,runner2,cleaner;

  PROTECT(runner1=rexpress(MAPRUNNERS));
  PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));

  if(runner2==NILSXP){
    merror("RHIPE ERROR: Could not create mapper\n");
    UNPROTECT(2);
    return(1);
  }

  for(;;){
    type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
    switch(type){
    case 0:
      UNPROTECT(2);
      return(0);
    case EVAL_CLEANUP_MAP:
      {
	PROTECT(cleaner=rexpress(MAPCLEANS));
	Rf_eval(Rf_lang2(Rf_install("eval"),cleaner),R_GlobalEnv);
	UNPROTECT(1);
	break;
      }
    default:
      {
	PROTECT(key = readFromHadoop(type));
	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
	PROTECT(value = readFromHadoop(type));
	Rf_defineVar(Rf_install("map.key"),key,R_GlobalEnv);
	Rf_defineVar(Rf_install("map.value"),value,R_GlobalEnv);
	Rf_eval(runner2 ,R_GlobalEnv);
	UNPROTECT(2);
	break;
      }
    }
  }
  UNPROTECT(2);
  return(ret);
}

const int mapper_setup(void){
  int32_t type = 0;
  SEXP setupm;

  type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);

  if(type==EVAL_SETUP_MAP){
    PROTECT(setupm=rexpress(MAPSETUPS));
    Rf_eval(Rf_lang2(Rf_install("eval"),setupm),R_GlobalEnv);
    UNPROTECT(1);
  }
  else {
    merror("RHIPE ERROR: What command is this for setup: %d ?\n",type);
    return(1);
  }
  return(0);
}

