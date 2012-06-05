#include "ream.h"
#include <iostream>



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
const char* MAPSETUPS = "unserialize(charToRaw(Sys.getenv('rhipe_setup_map')))";
const char* MAPRUNNERS = "unserialize(charToRaw(Sys.getenv('rhipe_map')))";
const char* MAPCLEANS = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_map')))";
#endif


/*********************************************************
* CONTENTS OF THIS FILE ARE BASICALLY NO LONGER USED 
* SEE mapreduce.cc
*********************************************************/

const int old_mapper_run2(void){

  int32_t type=0;
  int32_t mapbuf_cnt=0,MAPBUFFER=0;
  SEXP runner1,runner2,cleaner,kvector,vvector;
  char * mapbustr;
  
  PROTECT(runner1=rexpress(MAPRUNNERS));
  PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
  

  if(runner2==NILSXP){
    merror("RHIPE ERROR: Could not create mapper\n");
    UNPROTECT(2);
    return(4);
  }
  
  if ((mapbustr=getenv("rhipe_map_buff_size"))){
    MAPBUFFER = (int)strtol(mapbustr,NULL,10);
  }
  else{
    MAPBUFFER = 10000;
  }
 

  PROTECT(kvector = Rf_allocVector(VECSXP,MAPBUFFER));
  PROTECT(vvector = Rf_allocVector(VECSXP,MAPBUFFER));
  Rf_defineVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
  Rf_defineVar(Rf_install("map.values"),vvector,R_GlobalEnv);

  int Rerr;
	  
  for(;;){
#ifdef FILEREADER
    type=readJavaInt(FILEIN);
#else
    type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
#endif
    switch(type){
    case 0:
#ifdef FILEREADER
      printf("Finished Reading,going to cleanup\n");
      goto filereader_end;
#endif
      UNPROTECT(4);
      fflush(NULL);
      return(0);
    case -10:
      fflush(NULL);
      break;
    case EVAL_CLEANUP_MAP:
      {
#ifdef FILEREADER
      filereader_end:
	printf("In cleanup\n");
#endif
	if(mapbuf_cnt >0){
	  if(mapbuf_cnt < MAPBUFFER){
#ifdef FILEREADER
	    printf("Non Exact Number %d, running leftovers\n",mapbuf_cnt);
#endif

	    SEXP t1,t2;
	    PROTECT(t1 = Rf_allocVector(VECSXP,mapbuf_cnt));
	    PROTECT(t2 = Rf_allocVector(VECSXP,mapbuf_cnt));
	    
	    for(int i=0;i<mapbuf_cnt;i++){
	      // SET_VECTOR_ELT(t2,i, Rf_duplicate(VECTOR_ELT(vvector,i)));
	      // SET_VECTOR_ELT(t1,i, Rf_duplicate(VECTOR_ELT(kvector,i)));
	      SET_VECTOR_ELT(t2,i, VECTOR_ELT(vvector,i));
	      SET_VECTOR_ELT(t1,i, VECTOR_ELT(kvector,i));
	    }

	    Rf_setVar(Rf_install("map.keys"),t1,R_GlobalEnv);
	    Rf_setVar(Rf_install("map.values"),t2,R_GlobalEnv);
	    do_unser();
#ifdef FILEREADER
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#else
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#endif
	    UNPROTECT(2);
	  }else{
#ifdef FILEREADER
	    printf("Exact number %d, running leftovers\n",mapbuf_cnt);
#endif
	    Rf_setVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	    Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	    //JEREMIAH THIS STATE LOOKS LIKE AN ERROR TO ME SO I AM COMMENTING IT OUT.
	    //Rf_setVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.setup")),R_GlobalEnv);

	    do_unser();
#ifdef FILEREADER
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#else
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#endif
	  }
	}
	Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.cleanup")),R_GlobalEnv);
	PROTECT(cleaner=rexpress(MAPCLEANS));
	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),cleaner),NULL,&Rerr);
	UNPROTECT(1);
	fflush(NULL);
#ifdef FILEREADER
	return(0);
#endif
	break;
      }
    default:
      {
	if(mapbuf_cnt == MAPBUFFER){
	  Rf_setVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	  Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	  // do_unser();
#ifdef FILEREADER
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#else
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
#endif
	    fflush(NULL);
	    mapbuf_cnt=0;
	}
	SEXP k=R_NilValue,v=R_NilValue;
	// int fre=0;
//	PROTECT(k=Rf_allocVector(RAWSXP,type));
// 	fre=fread(RAW(k),type,1,
// #ifdef FILEREADER
// 		  FILEIN
// #else 
// 		  CMMNC->BSTDIN
// #endif		  
// 		  );
// 	if(fre <= 0){
// 	  UNPROTECT(5);
// 	  return(5);
// 	}
// #ifdef FILEREADER
// 	type = readJavaInt(FILEIN);
// #else
// 	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
// #endif
// 	PROTECT(v=Rf_allocVector(RAWSXP,type));
// 	fre=fread(RAW(v),type,1,
// #ifdef FILEREADER
// 		  FILEIN
// #else 
// 		  CMMNC->BSTDIN
// #endif		  
// 		  );
// 	if(fre<=0){
// 	  UNPROTECT(6);
// 	  return(6);
// 	}
	int err;
	PROTECT(k = readFromHadoop(type,&err));
	if(err){UNPROTECT(5); return(5);}
#ifdef FILEREADER
	type = readJavaInt(FILEIN);
#else
	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
#endif

	PROTECT(v = readFromHadoop(type,&err));
	if(err){UNPROTECT(6); return(6);}
	SET_VECTOR_ELT(vvector, mapbuf_cnt, v);
	SET_VECTOR_ELT(kvector, mapbuf_cnt, k);
	UNPROTECT(2);
	mapbuf_cnt++;
#ifdef FILEREADER
	_counter_++;
	if( _counter_ % 500 == 0 | _counter_==1)
	  LOGG(12,"Counter has reached %d\n",_counter_);
#endif
	break;
      }
    }
  }
}



const int mapper_run_no_key(void){

  int32_t type=0;
  int32_t mapbuf_cnt=0,MAPBUFFER=0;
  SEXP runner1,runner2,cleaner,kvector,vvector;
  char * mapbustr;
  PROTECT(runner1=rexpress(MAPRUNNERS));
  PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
  if(runner2==NILSXP){
    merror("RHIPE ERROR: Could not create mapper\n");
    UNPROTECT(2);
    return(4);
  }
  if ((mapbustr=getenv("rhipe_map_buff_size"))){
    MAPBUFFER = (int)strtol(mapbustr,NULL,10);
  }
  else{
    MAPBUFFER = 10000;
  }
  PROTECT(kvector = Rf_allocVector(VECSXP,MAPBUFFER));
  PROTECT(vvector = Rf_allocVector(VECSXP,MAPBUFFER));
  Rf_defineVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
  Rf_defineVar(Rf_install("map.values"),vvector,R_GlobalEnv);

  int Rerr;
	  
  for(;;){
    type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
    switch(type){
    case 0:
      UNPROTECT(4);
      fflush(NULL);
      return(0);
    case -10:
      fflush(NULL);
      break;
    case EVAL_CLEANUP_MAP:
      {
	if(mapbuf_cnt >0){
	  if(mapbuf_cnt < MAPBUFFER){
	    SEXP t2;
	    PROTECT(t2 = Rf_allocVector(VECSXP,mapbuf_cnt));
	    for(int i=0;i<mapbuf_cnt;i++){
	      SET_VECTOR_ELT(t2,i, VECTOR_ELT(vvector,i));
	    }
	    Rf_setVar(Rf_install("map.values"),t2,R_GlobalEnv);
	    // do_unser();
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
	    UNPROTECT(2);
	  }else{
	    Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	    //do_unser();
	    WRAP_R_EVAL(runner2,NULL,&Rerr);
	  }
	}
	PROTECT(cleaner=rexpress(MAPCLEANS));
	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),cleaner),NULL,&Rerr);
	UNPROTECT(1);
	fflush(NULL);
	break;
      }
    default:
      {
	if(mapbuf_cnt == MAPBUFFER){
	  Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	  WRAP_R_EVAL(runner2,NULL,&Rerr);
	  fflush(NULL);
	  mapbuf_cnt=0;
	}
	SEXP v=R_NilValue;
	int err;
	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
	PROTECT(v = readFromHadoop(type,&err));
	if(err){UNPROTECT(5); return(6);}
	SET_VECTOR_ELT(vvector, mapbuf_cnt, v);
	UNPROTECT(1);
	mapbuf_cnt++;
	break;
      }
    }
  }
}





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



void do_unser(void){
  // LOGG(12,"Wrote something home\n");
  // rexpress("map.keys   <- lapply(map.keys,function(r)   .Call('rh_uz',r)) ");
  // rexpress("map.values <- lapply(map.values,function(r) .Call('rh_uz',r))");
}

