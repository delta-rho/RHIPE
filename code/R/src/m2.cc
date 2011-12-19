// #include "map2.h"
// #include "ream.h"

// const char* MAPSETUPS  = "unserialize(charToRaw(Sys.getenv('rhipe_setup_map')))";
// const char* MAPRUNNERS = "unserialize(charToRaw(Sys.getenv('rhipe_map')))";
// const char* MAPCLEANS  = "unserialize(charToRaw(Sys.getenv('rhipe_cleanup_map')))";

// const int32_t  EVAL_SETUP_MAP =   -1;
// const int32_t  EVAL_CLEANUP_MAP = -2;

// const int mapper_run2(void){

//   int32_t type=0;
//   int32_t MAPBUFFER=0;
//   SEXP runner1,runner2,cleaner;
//   char * mapbustr;
  

//   PROTECT(runner1=rexpress(MAPRUNNERS));
//   PROTECT(runner2=Rf_lang2(Rf_install("eval"),runner1));
  

//   if(runner2==NILSXP){
//     merror("RHIPE ERROR: Could not create mapper\n");
//     UNPROTECT(2);
//     return(4);
//   }
  
//   if ((mapbustr=getenv("rhipe_map_buff_mb"))){
//     MAPBUFFER = (int)strtol(mapbustr,NULL,10)*1024*1024;
//   }
//   else{
//     MAPBUFFER = 1024*1024*64;
//   }
 
//   MapReader mrd = MapReader(MAPBUFFER,runner2);

//   int Rerr;
	  
//   for(;;){
//     type=readVInt64FromFileDescriptor(CMMNC->BSTDIN);
//     switch(type){
//     case 0:
//       fflush(NULL);
//       return(0);
//     case -10:
//       fflush(NULL);
//       break;
//     case EVAL_CLEANUP_MAP:
//       {
// 	if(mrd.spill()){
// 	   mrd.run_map_code();
// 	   fflush(NULL);
// 	}
// 	Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.cleanup")),R_GlobalEnv);
// 	PROTECT(cleaner=rexpress(MAPCLEANS));
// 	WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),cleaner),NULL,&Rerr);
// 	UNPROTECT(1);
// 	fflush(NULL);
// 	break;
//       }
//     default:
//       {
// 	if(mrd.spill()){
// 	  mrd.run_map_code();
// 	  fflush(NULL);
// 	}
// 	bool err;
// 	err = mrd.readObject(type);
// 	if(err) return(5); // i can't be bothered about the stack, going to quit anyway
// 	type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);
// 	err = mrd.readObject(type);
// 	if(err) return(5);
// 	break;
//       }
//     }
//   }
// }







// const int mapper_setup(void){
//   int32_t type = 0;
//   SEXP setupm;
//   int Rerr;
//   type = readVInt64FromFileDescriptor(CMMNC->BSTDIN);

//   if(type==EVAL_SETUP_MAP){
//     Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map.setup")),R_GlobalEnv);
//     PROTECT(setupm=rexpress(MAPSETUPS));
//     WRAP_R_EVAL(Rf_lang2(Rf_install("eval"),setupm),NULL,&Rerr);
//     UNPROTECT(1);
//     if(Rerr) return(7);
//   }
//   else {
//     merror("RHIPE ERROR: What command is this for setup: %d ?\n",type);
//     return(8);
//   }
//   Rf_defineVar(Rf_install(".rhipe.current.state"),Rf_ScalarString(Rf_mkChar("map")),R_GlobalEnv);

//   return(0);
// }



