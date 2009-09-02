#include "ream.h"
#import "iostream"

const char* REDUCESETUP = "unserialize(charToRaw(Sys.getenv('rhipe_setup_reduce')))";
const char* REDUCEPREKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_prekey')))";
const char* REDUCE = "unserialize(charToRaw(Sys.getenv('rhipe_reduce')))";
const char* REDUCEPOSTKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_postkey')))";
const char* REDUCECLEANUP = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_reduce')))";

const int reducer_run(void){

  int32_t type=0,ret=0;
  SEXP key,value;
  SEXP prekey0,prekey,reduce0,reduce,postkey0,postkey,reducesetup0,reducesetup;
    
  PROTECT(prekey0=rexpress(REDUCEPREKEY));
  PROTECT(prekey=Rf_lang2(Rf_install("eval"),prekey0));

  PROTECT(reduce0=rexpress(REDUCE));
  PROTECT(reduce=Rf_lang2(Rf_install("eval"),reduce0));

  PROTECT(postkey0=rexpress(REDUCEPOSTKEY));
  PROTECT(postkey=Rf_lang2(Rf_install("eval"),postkey0));



  for(;;){
    type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
    switch(type){
    case 0:
      UNPROTECT(6);
      return(0);
    case -10:
      fflush(CMMNC->BSTDOUT);
      fflush(NULL);
      break;
    case EVAL_SETUP_REDUCE:
      {
	SEXP reducesetup;
	LOGG(9,"Got reduce setup\n");
	PROTECT(reducesetup=rexpress(REDUCESETUP));
	Rf_eval(Rf_lang2(Rf_install("eval"),reducesetup),R_GlobalEnv);
	UNPROTECT(1);
      }
      break;
    case EVAL_CLEANUP_REDUCE:
      {
	SEXP reduceclean;
	PROTECT(reduceclean=rexpress(REDUCECLEANUP));
	Rf_eval(Rf_lang2(Rf_install("eval"),reduceclean),R_GlobalEnv);
	UNPROTECT(1);
      }
      break;
    case EVAL_REDUCE_THEKEY:
      type = readVInt64FromFileDescriptor(CMMNC->BSTDIN); //read in size of key
      PROTECT(key = readFromHadoop(type));
      Rf_defineVar(Rf_install("reduce.key"),key,R_GlobalEnv);
      UNPROTECT(1);
      break;
    case EVAL_REDUCE_PREKEY:
      Rf_eval(prekey ,R_GlobalEnv);
      break;
    case EVAL_REDUCE_POSTKEY:
      Rf_eval(postkey ,R_GlobalEnv);
      break;
    default:
      //incoming values
      PROTECT(value = readFromHadoop(type));
      Rf_defineVar(Rf_install("reduce.value"),value,R_GlobalEnv);
      Rf_eval(reduce ,R_GlobalEnv);
      UNPROTECT(1);
      break;
    }
  }
  UNPROTECT(6);
  return(ret);
}

// const int reducer_setup(void){
//   int32_t type = 0;
//   SEXP reducesetup;

//   type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);

//   if(type==EVAL_SETUP_REDUCE){
//     PROTECT(reducesetup=rexpress(REDUCESETUP));
//     Rf_eval(Rf_lang2(Rf_install("eval"),reducesetup),R_GlobalEnv);
//     UNPROTECT(1);
//     LOGG(10,"Ran reduce setup\n");
//   }
//   else {
//     merror("RHIPE ERROR: What command is this for reduce setup(EVAL_SETUP_REDUCE=%d): %d ?\n",EVAL_SETUP_REDUCE,type);
//     return(1);
//   }
//   return(0);
// }

