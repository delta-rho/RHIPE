#include "ream.h"
#include <jni.h>
#include <iostream>
#include <Rinternals.h>
R_CallMethodDef callMethods [] = {
  {"rh_spool_kv",(DL_FUNC) rh_spool_kv,3},
  {"rh_combine_kv",(DL_FUNC) rh_combine_kv,3},
  {"rh_status",(DL_FUNC) rh_status,1},
  {"rh_counter",(DL_FUNC) rh_counter,2},
  {NULL, NULL, 0}
};
#define R_INTERFACE_PTRS 1
#define CSTACK_DEFNS 1
JMessages *stored_jv_messages;
extern uintptr_t R_CStackLimit; 
using namespace google::protobuf;


extern "C" {
  void Re_ResetConsole()
  {
  }
  void Re_FlushConsole()
  {
  }
  void Re_ClearerrConsole()
  {
  }
  void Re_ShowMessage(const char* mess){
    Re_WriteConsoleEx(mess,strlen(mess),0);
  }
  
  void Re_WriteConsoleEx(const char *buf1, int len, int oType){
    if(oType == 0){
      JNU_system_print(buf1,len);
    }else{
      JNU_throw_R_exception(buf1);
    }
  }


  int voidevalR(const char* cmd)
  {
    SEXP cmdSexp, cmdexpr, ans = R_NilValue;
    int i;
    ParseStatus status;
    PROTECT(cmdSexp = Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(cmdSexp, 0, Rf_mkCharCE(cmd,CE_UTF8));
    PROTECT(cmdexpr = R_ParseVector(cmdSexp, -1, &status, R_NilValue));
    if (status != PARSE_OK) {
      UNPROTECT(2);
      char *p="R Parse Error:";
      char * m = (char*)malloc(strlen(cmd)+strlen(p)+2);
      sprintf(m,"%s %s \n",p,cmd);
      Re_WriteConsoleEx(m,strlen(m),1);
      // free(m); not sure why i'm not freeing it.
      // but it doesn't really matter. Program will end now.
      return(-2);
    }
    int Rerr=0, imax=Rf_length(cmdexpr);
    for(i = 0; i < imax; i++){
      ans =R_tryEval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv,&Rerr);
      if(Rerr>0) {
	break;
      }
    }
    UNPROTECT(2);
    return(Rerr);
  }

  void exitR(void){
    R_RunExitFinalizers();
    Rf_KillAllDevices();
    R_CleanTempDir();
   }

  int embedR(int argc0,char **argv0){
    char *argv[] = {"--silent","--slave","--vanilla",0};
    int argc = 3;
    structRstart rp;
    Rstart Rp = &rp;
    google::protobuf::SetLogHandler(&CaptureLog);
    GOOGLE_PROTOBUF_VERIFY_VERSION;  //need to check return value
    R_DefParams(Rp);
    Rp->NoRenviron = 0;
    R_SetParams(Rp);
    R_SignalHandlers=0;
    int stat= Rf_initialize_R(argc, argv);
    if (stat<0) {
      printf("Rf_initialize Error %s:%d\n",__FUNCTION__,__LINE__);
      return(stat);
    }
    R_SignalHandlers=0;

    R_CStackLimit = (uintptr_t)-1;
    R_Outputfile = NULL;
    R_Consolefile = NULL;
    R_Interactive = (Rboolean)1;
    ptr_R_ShowMessage = Re_ShowMessage;

    ptr_R_WriteConsole = NULL;
    ptr_R_WriteConsoleEx =Re_WriteConsoleEx;
    
    ptr_R_ReadConsole = NULL;
    ptr_R_ResetConsole = Re_ResetConsole;;
    ptr_R_FlushConsole = Re_FlushConsole;
    ptr_R_ClearerrConsole = Re_ClearerrConsole;

    ptr_R_Busy = NULL;
    ptr_R_ShowFiles = NULL;
    ptr_R_ChooseFile = NULL;
    ptr_R_loadhistory = NULL;
    ptr_R_savehistory = NULL;
    setup_Rmainloop();
    DllInfo *info = R_getEmbeddingDllInfo();
    R_registerRoutines(info, NULL, callMethods, NULL, NULL);
    return(0);
  }
}

  // void jri_checkExceptions(JNIEnv *env)
  // {
  //   jthrowable t=(env)->ExceptionOccurred();
  //   if (t) {
  //     (env)->ExceptionDescribe();
  //     (env)->ExceptionClear();
  //   }
  // }

