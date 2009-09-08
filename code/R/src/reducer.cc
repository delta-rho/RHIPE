#include "ream.h"
#import "iostream"
const int32_t EVAL_SETUP_REDUCE = -1;
const int32_t EVAL_REDUCE_PREKEY = -2;
const int32_t EVAL_REDUCE_POSTKEY = -3;
const int32_t EVAL_REDUCE_THEKEY = -4;
const int32_t EVAL_CLEANUP_REDUCE = -5;
const char* REDUCESETUP = "unserialize(charToRaw(Sys.getenv('rhipe_setup_reduce')))";
const char* REDUCEPREKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_prekey')))";
const char* REDUCE = "unserialize(charToRaw(Sys.getenv('rhipe_reduce')))";
const char* REDUCEPOSTKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_postkey')))";
const char* REDUCECLEANUP = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_reduce')))";

const int reducer_run(void){

  int32_t type=0;
  int32_t redbuf_cnt=0,REDBUFFER=0;
  SEXP key;
  SEXP prekey0,prekey,reduce0,reduce,postkey0,postkey,vvector;
    
  char * redbustr;

  if ((redbustr=getenv("rhipe_reduce_buff_size"))){
    REDBUFFER = (int)strtol(redbustr,NULL,10);
  }
  else{
    REDBUFFER = 100;
  }
  PROTECT(prekey0=rexpress(REDUCEPREKEY));
  PROTECT(prekey=Rf_lang2(Rf_install("eval"),prekey0));

  PROTECT(reduce0=rexpress(REDUCE));
  PROTECT(reduce=Rf_lang2(Rf_install("eval"),reduce0));

  PROTECT(postkey0=rexpress(REDUCEPOSTKEY));
  PROTECT(postkey=Rf_lang2(Rf_install("eval"),postkey0));


  PROTECT(vvector = Rf_allocVector(VECSXP,REDBUFFER));
  Rf_defineVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);

  int err,Rerr;


  for(;;){
    type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
    switch(type){
    case 0:
      fflush(NULL);
      UNPROTECT(7);
      return(0);
    case -10:
      fflush(NULL);
      break;
    case EVAL_SETUP_REDUCE:
      {
	SEXP reducesetup;
	LOGG(9,"Got reduce setup\n");
	PROTECT(reducesetup=rexpress(REDUCESETUP));
	// Rf_eval(Rf_lang2(Rf_install("eval"),reducesetup),R_GlobalEnv);
	R_tryEval(Rf_lang2(Rf_install("eval"),reducesetup),NULL,&Rerr);
	UNPROTECT(1);
      }
      break;
    case EVAL_CLEANUP_REDUCE:
      {
	SEXP reduceclean;
	PROTECT(reduceclean=rexpress(REDUCECLEANUP));
	R_tryEval(Rf_lang2(Rf_install("eval"),reduceclean),NULL, &Rerr);
	UNPROTECT(1);
      }
      break;
    case EVAL_REDUCE_THEKEY:
      type = readVInt64FromFileDescriptor(CMMNC->BSTDIN); //read in size of key
      PROTECT(key = readFromHadoop(type,&err));
      Rf_defineVar(Rf_install("reduce.key"),key,R_GlobalEnv);
      UNPROTECT(1);
      redbuf_cnt=0;
      break;
    case EVAL_REDUCE_PREKEY:
      R_tryEval(prekey,NULL,&Rerr);
      break;
    case EVAL_REDUCE_POSTKEY:
      if(redbuf_cnt >0){
	if(redbuf_cnt < REDBUFFER){
	  SEXP t1;
	  PROTECT(t1 = Rf_allocVector(VECSXP,redbuf_cnt));	  
	  for(int i=0;i<redbuf_cnt;i++){
	    SET_VECTOR_ELT(t1,i, VECTOR_ELT(vvector,i));
	  }
	  Rf_setVar(Rf_install("reduce.values"),t1,R_GlobalEnv);
	  R_tryEval(reduce,NULL, &Rerr);
	  UNPROTECT(1);
	}else{
	  Rf_setVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);
	  R_tryEval(reduce,NULL, &Rerr);
	}
      }
      R_tryEval(postkey ,NULL, &Rerr);
      break;
    default:
      if(redbuf_cnt == REDBUFFER){
	Rf_setVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);
	R_tryEval(reduce,NULL, &Rerr);
	redbuf_cnt=0;
      }
      SEXP v;
      PROTECT(v= readFromHadoop(type,&err));
      if(err) {
	UNPROTECT(8);
	return(0);
      }
      SET_VECTOR_ELT(vvector, redbuf_cnt, v);
      UNPROTECT(1);
      redbuf_cnt++;
    }
  }
}

