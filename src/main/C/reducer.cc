#include "ream.h"
#include <iostream>

// const char* REDUCESETUP = "unserialize(charToRaw(Sys.getenv('rhipe_setup_reduce')))";
// const char* REDUCEPREKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_prekey')))";
// const char* REDUCE = "unserialize(charToRaw(Sys.getenv('rhipe_reduce')))";
// const char* REDUCEPOSTKEY = "unserialize(charToRaw(Sys.getenv('rhipe_reduce_postkey')))";
// const char* REDUCECLEANUP = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_reduce')))";
const char* REDUCESETUP = "{ff <- Sys.getenv('rhipe_setup_reduce'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* REDUCEPREKEY = "{ff <- Sys.getenv('rhipe_reduce_prekey'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* REDUCE = "{ff <- Sys.getenv('rhipe_reduce'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* REDUCEPOSTKEY = "{ff <- Sys.getenv('rhipe_reduce_postkey'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";
const char* REDUCECLEANUP = "{ff <- Sys.getenv('rhipe_cleanup_reduce'); tmpf <- readChar(ff, file.info(ff)$size); unserialize(charToRaw(tmpf))}";

const int reducer_run(void){

  int32_t type=0;
  int32_t redbuf_cnt=0,REDBUFFER=0;
  SEXP key;
  SEXP prekey0,prekey,reduce0,reduce,postkey0,postkey,vvector;
  unsigned int REDBUFFER_MAXBYTES;
  char * redbustr;

  if ((redbustr=getenv("rhipe_reduce_buff_size"))){
    REDBUFFER = (int)strtol(redbustr,NULL,10);
  }
  else{
    REDBUFFER = 100;
  }
  if ((redbustr=getenv("rhipe_reduce_bytes_read")))
    REDBUFFER_MAXBYTES= (unsigned int)strtol(redbustr,NULL,10);
  //gcc warns about the above being possibly not initialized, but R will make it so. Or rather RHIPE will make it so

  PROTECT(prekey0=rexpress(REDUCEPREKEY));
  PROTECT(prekey=Rf_lang2(Rf_install("eval"),prekey0));

  PROTECT(reduce0=rexpress(REDUCE));
  PROTECT(reduce=Rf_lang2(Rf_install("eval"),reduce0));

  PROTECT(postkey0=rexpress(REDUCEPOSTKEY));
  PROTECT(postkey=Rf_lang2(Rf_install("eval"),postkey0));


  PROTECT(vvector = Rf_allocVector(VECSXP,REDBUFFER));
  Rf_defineVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);

  int err,Rerr;
  unsigned int bytes_read = 0;

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
	Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("reduce.setup")),R_GlobalEnv);

	PROTECT(reducesetup=rexpress(REDUCESETUP));
	// Rf_eval(Rf_lang2(Rf_install("eval"),reducesetup),R_GlobalEnv);
	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),reducesetup),NULL,&Rerr);
	UNPROTECT(1);
      }
      break;
    case EVAL_CLEANUP_REDUCE:
      {
	SEXP reduceclean;
	Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("reduce.cleanup")),R_GlobalEnv);
	PROTECT(reduceclean=rexpress(REDUCECLEANUP));
	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),reduceclean),NULL, &Rerr);
	UNPROTECT(1);
      }
      break;
    case EVAL_REDUCE_THEKEY:
      bytes_read = 0;
      type = readVInt64FromFileDescriptor(CMMNC->BSTDIN); //read in size of key
      PROTECT(key = readFromHadoop(type,&err));
      Rf_defineVar(Rf_install("reduce.key"),key,R_GlobalEnv);
      Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("reduce")),R_GlobalEnv);
      UNPROTECT(1);
      redbuf_cnt=0;
      break;
    case EVAL_REDUCE_PREKEY:
      WRAP_R_EVAL(prekey,NULL,&Rerr);
      fflush(NULL);
      break;
    case EVAL_REDUCE_POSTKEY:
      if(redbuf_cnt >0){
	if(redbuf_cnt < REDBUFFER){
	  // Need to copy to avoid having a reduce.values of length N
	  // yet only n(<N) filled and thus remaining are nulls
	  SEXP t1;
	  PROTECT(t1 = Rf_allocVector(VECSXP,redbuf_cnt));	  
	  for(int i=0;i<redbuf_cnt;i++){
	    SET_VECTOR_ELT(t1,i, (VECTOR_ELT(vvector,i)));
	  }
	  Rf_setVar(Rf_install("reduce.values"),t1,R_GlobalEnv);

	  WRAP_R_EVAL(reduce,NULL, &Rerr);
	  UNPROTECT(1);
	}else{
	  Rf_setVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);
	  WRAP_R_EVAL(reduce,NULL, &Rerr);
	}
      }
      WRAP_R_EVAL(postkey ,NULL, &Rerr);
      fflush(NULL);
      break;
    default:
      // We need to read another element for reduce.values
      // However, since reduce is vectorized
      // We call reduce$post on reduce.values of length rhipe_reduce_buff_size
      // OR if the bytes read is just greater than REDBUFFER_MAXBYTES
      if(redbuf_cnt == REDBUFFER){
	// Case 1. The reduce.values has reached the critical length
	// for evaluation
	Rf_setVar(Rf_install("reduce.values"),vvector,R_GlobalEnv);
	WRAP_R_EVAL(reduce,NULL, &Rerr);
	redbuf_cnt=0;
	bytes_read = 0;
      }else if( bytes_read >= REDBUFFER_MAXBYTES ){
	//Case 2. The size of reduce.values as reached the critical cut off
	//however, the number of elements (initialized to REDBUFFER)
	//could well be greater than the actual number read.
	//in that case, reduce the length
	if(redbuf_cnt < REDBUFFER){
	  SEXP t1;
	  PROTECT(t1 = Rf_allocVector(VECSXP,redbuf_cnt));	  
	  for(int i=0;i<redbuf_cnt;i++){
	    SET_VECTOR_ELT(t1,i, (VECTOR_ELT(vvector,i)));
	  }
	  Rf_setVar(Rf_install("reduce.values"),t1,R_GlobalEnv);
	  WRAP_R_EVAL(reduce,NULL, &Rerr);
	  UNPROTECT(1);
	  redbuf_cnt=0;
	  bytes_read = 0;
	}
      }
      SEXP v;
      PROTECT(v= readFromHadoop(type,&err));
      bytes_read += type;
      if(err) {
	UNPROTECT(8);
	return(10);
      }
      SET_VECTOR_ELT(vvector, redbuf_cnt, v);
      UNPROTECT(1);
      redbuf_cnt++;
      fflush(NULL);
    }
  }
}

