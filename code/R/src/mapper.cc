#include "ream.h"
#import "iostream"
const char* MAPSETUPS = "unserialize(charToRaw(Sys.getenv('rhipe_setup_map')))";
const char* MAPRUNNERS = "unserialize(charToRaw(Sys.getenv('rhipe_map')))";
const char* MAPCLEANS = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_map')))";
const int32_t  EVAL_SETUP_MAP =   -1;
const int32_t  EVAL_CLEANUP_MAP = -2;


// const int mapper_run2(void){

//   int32_t type=0,ret=0;
//   SEXP key,value;
//   SEXP runner1,runner2,cleaner;
//   uint8_t** mapkbuf, **mapvbuf;
//   uint32_t* mapkbuf_l,*mapvbuf_l;
//   int MAPBUFFER;
//   char * mapbustr;
//   uint8_t *keycon,*vcon;
//   int mapbuf_cnt=0;

//   PROTECT(runner1=rexpress(MAPRUNNERS));
//   PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
//   if(runner2==NILSXP){
//     merror("RHIPE ERROR: Could not create mapper\n");
//     UNPROTECT(2);
//     return(1);
//   }
  
//   if (mapbustr=getenv("rhipe_map_buff_size")){
//     MAPBUFFER = (int)strtol(mapbustr,NULL,10);
//   }
//   else{
//     MAPBUFFER = 1000;
//   }
//   mapkbuf=(uint8_t**) malloc(sizeof(uint8_t*)*MAPBUFFER);
//   mapvbuf=(uint8_t**) malloc(sizeof(uint8_t*)*MAPBUFFER);

//   mapkbuf_l=(uint32_t*) malloc(sizeof(uint32_t)*MAPBUFFER);
//   mapvbuf_l=(uint32_t*) malloc(sizeof(uint32_t)*MAPBUFFER);


  
//   for(;;){
//     type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
//     switch(type){
//     case 0:
//       UNPROTECT(2);
//       return(0);
//     case EVAL_CLEANUP_MAP:
//       {
// 	if(mapbuf_cnt> 0){
// 	  SEXP mapvalues,mapkeys;
// 	  PROTECT(mapvalues=Rf_allocVector(VECSXP,mapbuf_cnt));
// 	  PROTECT(mapkeys=  Rf_allocVector(VECSXP,mapbuf_cnt));
// 	  for (int i = 0; i < mapbuf_cnt; i++)
// 	    {
// 	      SEXP k,v;
// 	      PROTECT(k = readFromMem(mapkbuf[i],mapkbuf_l[i]));
// 	      PROTECT(v = readFromMem(mapvbuf[i],mapvbuf_l[i]));
// 	      SET_VECTOR_ELT(mapkeys,i, k);
// 	      SET_VECTOR_ELT(mapvalues,i,v);
// 	      UNPROTECT(2);
// 	      free(mapkbuf[i]);
// 	      free(mapvbuf[i]);
// 	    }
// 	  Rf_defineVar(Rf_install("map.keys"),mapkeys,R_GlobalEnv);
// 	  Rf_defineVar(Rf_install("map.values"),mapvalues,R_GlobalEnv);
// 	  UNPROTECT(2);
// 	  Rf_eval(runner2 ,R_GlobalEnv);
// 	}
	
// 	PROTECT(cleaner=rexpress(MAPCLEANS));
// 	Rf_eval(Rf_lang2(Rf_install("eval"),cleaner),R_GlobalEnv);
// 	UNPROTECT(1);
// 	break;
//       }
//     default:
//       {
// 	if(mapbuf_cnt == MAPBUFFER){
// 	  SEXP mapvalues,mapkeys;
// 	  PROTECT(mapvalues=Rf_allocVector(VECSXP,mapbuf_cnt));
// 	  PROTECT(mapkeys=  Rf_allocVector(VECSXP,mapbuf_cnt));
// 	  for (int i = 0; i < mapbuf_cnt; i++)
// 	    {
// 	      SEXP k,v;
// 	      PROTECT(k = readFromMem(mapkbuf[i],mapkbuf_l[i]));
// 	      PROTECT(v = readFromMem(mapvbuf[i],mapvbuf_l[i]));
// 	      SET_VECTOR_ELT(mapkeys,i, k);
// 	      SET_VECTOR_ELT(mapvalues,i,v);
// 	      UNPROTECT(2);
// 	      free(mapkbuf[i]);
// 	      free(mapvbuf[i]);
// 	    }
// 	  Rf_defineVar(Rf_install("map.keys"),mapkeys,R_GlobalEnv);
// 	  Rf_defineVar(Rf_install("map.values"),mapvalues,R_GlobalEnv);
// 	  UNPROTECT(2);
// 	  Rf_eval(runner2 ,R_GlobalEnv);
// 	  mapbuf_cnt = 0;
// 	  LOGG(9,"Spilling\n");
// 	}
// 	keycon = (uint8_t*)malloc(type);
// 	mapkbuf_l[mapbuf_cnt]=type;
// 	if(fread(keycon, type,1, CMMNC->BSTDIN )<=0) {
// 	  free(mapkbuf);free(mapvbuf);free(mapkbuf_l);free(mapvbuf_l);
// 	  UNPROTECT(2);
// 	  return(0);
// 	}
// 	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
// 	vcon = (uint8_t*)malloc(type);
// 	mapvbuf_l[mapbuf_cnt]=type;
// 	if(fread(vcon, type,1,CMMNC->BSTDIN) <=0) {
// 	  free(mapkbuf);free(mapvbuf);free(mapkbuf_l);free(mapvbuf_l);
// 	  UNPROTECT(2);
// 	  return(0);
// 	}
// 	mapkbuf[mapbuf_cnt]=keycon;
// 	mapvbuf[mapbuf_cnt]=vcon;
// 	mapbuf_cnt++;
// 	break;
//       }
//     }
//   }
// }


const int mapper_run2(void){

  int32_t type=0;
  int32_t mapbuf_cnt=0,MAPBUFFER=0;
  SEXP runner1,runner2,cleaner,kvector,vvector;
  char * mapbustr;

  
  PROTECT(runner1=rexpress(MAPRUNNERS));
  PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));


  if(runner2==NILSXP){
    merror("RHIPE ERROR: Could not create mapper\n");
    UNPROTECT(2);
    return(1009);
  }
  
  if ((mapbustr=getenv("rhipe_map_buff_size"))){
    MAPBUFFER = (int)strtol(mapbustr,NULL,10);
  }
  else{
    MAPBUFFER = 1000;
  }
 

  PROTECT(kvector = Rf_allocVector(VECSXP,MAPBUFFER));
  PROTECT(vvector = Rf_allocVector(VECSXP,MAPBUFFER));
  Rf_defineVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
  Rf_defineVar(Rf_install("map.values"),vvector,R_GlobalEnv);

  int err=0,Rerr;
	  
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
	    SEXP t1,t2;
	    PROTECT(t1 = Rf_allocVector(VECSXP,mapbuf_cnt));
	    PROTECT(t2 = Rf_allocVector(VECSXP,mapbuf_cnt));
	    
	    for(int i=0;i<mapbuf_cnt;i++){
	      SET_VECTOR_ELT(t2,i, VECTOR_ELT(vvector,i));
	      SET_VECTOR_ELT(t1,i, VECTOR_ELT(kvector,i));
	    }
	    Rf_setVar(Rf_install("map.keys"),t1,R_GlobalEnv);
	    Rf_setVar(Rf_install("map.values"),t2,R_GlobalEnv);
	    // Rf_eval(runner2 ,R_GlobalEnv);
	    R_tryEval(runner2,NULL,&Rerr);
	    UNPROTECT(2);
	  }else{
	    Rf_setVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	    Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	    R_tryEval(runner2,NULL,&Rerr);

	  }
	}
	PROTECT(cleaner=rexpress(MAPCLEANS));
	R_tryEval(Rf_lang2(Rf_install("eval"),cleaner),NULL,&Rerr);
	UNPROTECT(1);
	break;
      }
    default:
      {
	if(mapbuf_cnt == MAPBUFFER){
	  Rf_setVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
	  Rf_setVar(Rf_install("map.values"),vvector,R_GlobalEnv);
	  R_tryEval(runner2,NULL,&Rerr);
	  mapbuf_cnt=0;
	}
	SEXP k,v;
	PROTECT(k= readFromHadoop(type,&err));
	if(err) {
	  UNPROTECT(5);
	  return(1);
	}
	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
	PROTECT( v = readFromHadoop(type,&err));
	if(err) {
	  UNPROTECT(6);
	  return(1);
	}

	SET_VECTOR_ELT(vvector, mapbuf_cnt, v);
	SET_VECTOR_ELT(kvector, mapbuf_cnt, k);
	UNPROTECT(2);
	mapbuf_cnt++;

	break;
      }
    }
  }
}











// const int mapper_run(void){

//   int32_t type=0,ret=0;
//   SEXP key,value;
//   SEXP runner1,runner2,cleaner;
//   SEXP mapbuf,maplist;
//   int bufcount = 0;
  
//   PROTECT(runner1=rexpress(MAPRUNNERS));
//   PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
//   if(runner2==NILSXP){
//     merror("RHIPE ERROR: Could not create mapper\n");
//     UNPROTECT(2);
//     return(1);
//   }
  
//   // PROTCET(mapbuf = Rf_allocVector(VECSXP,MAPBUFFER));
  
//   for(;;){
//     type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
//     switch(type){
//     case 0:
//       UNPROTECT(2);
//       return(0);
//     case EVAL_CLEANUP_MAP:
//       {
// 	PROTECT(cleaner=rexpress(MAPCLEANS));
// 	Rf_eval(Rf_lang2(Rf_install("eval"),cleaner),R_GlobalEnv);
// 	UNPROTECT(1);
// 	break;
//       }
//     default:
//       {
// 	PROTECT(key = readFromHadoop(type));
// 	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);//type is length
// 	PROTECT(value = readFromHadoop(type));
// 	Rf_defineVar(Rf_install("map.key"),key,R_GlobalEnv);
// 	Rf_defineVar(Rf_install("map.value"),value,R_GlobalEnv);
// 	Rf_eval(runner2 ,R_GlobalEnv);

// 	// iff(bufcount == MAPBUFFER){
// 	//   //spill
// 	//   Rf_defineVar(Rf_install("map.info"),mapbuf,R_GlobalEnv);
// 	// }
	

// 	// SEXP pair = Rf_allocVector(VECSXP,2);
// 	// VECTOR_SET_ELT(pair,0, key);
// 	// VECTOR_SET_ELT(pair,1,value);
// 	// bufcount++;
	  
// 	UNPROTECT(2);
// 	break;
//       }
//     }
//   }
//   UNPROTECT(2);
//   return(ret);
// }

const int mapper_setup(void){
  int32_t type = 0;
  SEXP setupm;
  int Rerr;
  type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);

  if(type==EVAL_SETUP_MAP){
    PROTECT(setupm=rexpress(MAPSETUPS));
    R_tryEval(Rf_lang2(Rf_install("eval"),setupm),NULL,&Rerr);
    UNPROTECT(1);
    if(Rerr) return(Rerr+2000);
  }
  else {
    merror("RHIPE ERROR: What command is this for setup: %d ?\n",type);
    return(102);
  }
  return(0);
}

