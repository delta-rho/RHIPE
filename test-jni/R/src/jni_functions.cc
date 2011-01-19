#include "ream.h"
#include "org_godhuli_rhipe_RHMRHelper.h"
#include <rexp.pb.h>
#include <jni.h>
#include <google/protobuf/io/coded_stream.h>
using namespace google::protobuf::io;

JavaVM *cached_jvm =NULL;
extern "C" {
  JNIEXPORT jint JNICALL JNI_OnLoad
  (JavaVM *jvm, void *reserved)
  {
    cached_jvm = jvm;
    return JNI_VERSION_1_2;
  }
  JNIEnv *JNU_GetEnv() {
    JNIEnv *env; 
    (cached_jvm)->GetEnv((void **)&env, JNI_VERSION_1_2);
    return env;
  }

  void JNU_ThrowByName(JNIEnv *env, const char *name, const char *msg) 
  {
    jclass cls = (env)->FindClass(name); /* if cls is NULL, an exception has already been thrown */ 
    if (cls != NULL) {
      (env)->ThrowNew( cls, msg);
    } /* free the local ref */ 
    (env)->DeleteLocalRef(cls);
  }
  void JNU_throw_general_exception(const char *name, const char *msg) 
  {
  JNIEnv *jenv = JNU_GetEnv();
  jclass cls = (jenv)->FindClass(name); 
  /* if cls is NULL, an exception has already been thrown */ 
  if (cls != NULL) {
    (jenv)->ThrowNew( cls, msg);
  }  
  else fprintf(stderr, "(Could not %s) %s - %s", name, name, msg);
  /* free the local ref */
  (jenv)->DeleteLocalRef(cls);
  }

  void JNU_throw_R_exception(const char* msg)
  {
    JNU_throw_general_exception("org/godhuli/rhipe/RHIPEException",msg);
  }

  void JNU_system_print(const char *f,int k){
  JNIEnv *jenv = JNU_GetEnv();
  static jclass _print_cls =NULL ; 
  static jmethodID _print_mid = NULL; 
  if(_print_cls == NULL){
    _print_cls = (jenv)->FindClass("org/godhuli/rhipe/RHMRHelper");
  }
  if(_print_mid == NULL){
    _print_mid = (jenv)->GetStaticMethodID(_print_cls, "print", "(Ljava/lang/String;)V");
  }
  if (_print_mid == 0){
    printf("No method 'print' in org/godhuli/rhipe/RHMRHelper: (%s)",f);
    return;
  }
  jstring msg = jenv->NewStringUTF(f);
  (jenv)->CallStaticVoidMethod(_print_cls, _print_mid, msg);
  }

  void JNU_mcount(const char* a, const char* v, int c){
    JNIEnv *jenv = JNU_GetEnv();
    static jclass _count_cls =NULL ; 
    static jmethodID _count_mid = NULL; 
    if(_count_cls == NULL){
      _count_cls = (jenv)->FindClass("org/godhuli/rhipe/RHMRHelper");
    }
    if(_count_mid == NULL){
      _count_mid = (jenv)->GetStaticMethodID(_count_cls, "counter", "(Ljava/lang/String;Ljava/lang/String;I)V");
    }
    if (_count_mid == 0){
      fprintf(stderr,"No method 'counter' in org/godhuli/rhipe/RHMRHelper\n");
      return;
    }
    jstring g = jenv->NewStringUTF(a);
    jstring f = jenv->NewStringUTF(v);
    (jenv)->CallStaticVoidMethod(_count_cls, _count_mid, g,f, c);
  }

  void JNU_mstatus(const char *a){
    JNIEnv *jenv = JNU_GetEnv();
    static jclass _status_cls =NULL ; 
    static jmethodID _status_mid = NULL; 
    if(_status_cls == NULL){
      _status_cls = (jenv)->FindClass("org/godhuli/rhipe/RHMRHelper");
    }
    if(_status_mid == NULL){
      _status_mid = (jenv)->GetStaticMethodID(_status_cls, "counter", "(Ljava/lang/String;)V");
    }
    if (_status_mid == 0){
      fprintf(stderr,"No method 'status' in org/godhuli/rhipe/RHMRHelper\n");
      return;
    }
    jstring g = jenv->NewStringUTF(a);
    (jenv)->CallStaticVoidMethod(_status_cls, _status_mid,g);
  }

  JNIEXPORT jint JNICALL Java_org_godhuli_rhipe_RHMRHelper_embedR
  (JNIEnv *env, jobject jobj, jstring rhome,jobjectArray a,jint iospillsize)
  {
    // REXP *rhome = new REXP(),*args = new REXP();
    // jstring2REXP(env,jstr,rhome);
    // jstring2REXP(env,a,args);
    // // taken from Simon Urbanek's JRI
    // char **argv;
    // int argc=0;
    // int len = args->stringvalue_size();
    // argv = (char**)malloc(len);
    // for(int i=0;i < len;i++){
    //   const char* c= args->stringvalue(i).strval().c_str();
    //   argv[i]=(char*) malloc(strlen(c)+1);
    //   strcpy(argv[i],c);
    // }
    // argc = len;
    const char *rhomec; 
    rhomec = (const char*)(env)->GetStringUTFChars(rhome, NULL);
    if(rhomec == NULL) return(1);
    setenv("R_HOME",rhomec,1);
    jint r= embedR(0,NULL);
    stored_jv_messages = new JMessages(jobj);
    stored_jv_messages->combiner_spill_size = iospillsize;
    return(r);
  }
    

  JNIEXPORT jint JNICALL Java_org_godhuli_rhipe_RHMRHelper_evalR
  (JNIEnv *env, jobject jobj, jstring s)
  {
    const char *sc; 
    sc = (const char*)(env)->GetStringUTFChars(s, NULL);    
    jint result=0;
    if(sc != NULL){
      result = (jint) voidevalR(sc);
      (env)->ReleaseStringUTFChars(s,sc);
    }
    else {
      result=-1;
    }
    return(result);
  }


  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_exitR
  (JNIEnv *env, jobject jobj){
    exitR();
  }


  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_createOpts
  (JNIEnv *env, jobject o, jbyteArray b,jbyteArray c){
    REXP *s0 = new REXP(), *s1 = new REXP();
    jstring2REXP(env,b,s0);
    jstring2REXP(env,c,s1);
    SEXP sl;
    PROTECT(sl = message2rexp(*s0));
    Rf_setVar(Rf_install(  s1->stringvalue(0).strval().c_str()  ),sl,R_GlobalEnv);
    delete s0;
    delete s1;
    UNPROTECT(1);
  }

  JNIEXPORT jobject JNICALL Java_org_godhuli_rhipe_RHMRHelper_create_1backing_1store
  (JNIEnv *env, jobject obj, jint f)
  {
    if(stored_jv_messages!=NULL){
      jobject p = stored_jv_messages->create_storage(f);
      return p;
    }else return NULL;
  }

  JNIEXPORT jobject JNICALL Java_org_godhuli_rhipe_RHMRHelper_create_1reduce_1key_1store
  (JNIEnv *env, jobject obj, jint f)
  {
    if(stored_jv_messages!=NULL){
      jobject p = stored_jv_messages->create_reduce_key_storage(f);
      return p;
    }else return NULL;
  }

  JNIEXPORT jint JNICALL Java_org_godhuli_rhipe_RHMRHelper_get_1num_1to_1flush_1from_1c
  (JNIEnv *env, jobject obj)
  {
    int a= stored_jv_messages->count_kv_stage_buffer_out;
    stored_jv_messages->count_kv_stage_buffer_out= 0;
    return a;
  }

  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_remove_1r2j_1wrappers
  (JNIEnv *jenv, jobject jobj)
  {
    // if(stored_jv_messages->cdo_kv_stage_buffer_out)
    //   delete stored_jv_messages->cdo_kv_stage_buffer_out;
    // if(stored_jv_messages->z_kv_stage_buffer_out)
    //   delete stored_jv_messages->z_kv_stage_buffer_out;
  }

  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_rewind_1kv_1stage
  (JNIEnv *env, jobject obj, jint totalsize,jint absolutemax)
  {
    // totalsize will be less (could be equal) to the into C buffer size
    // fprintf(stderr, "VREATINGE NEW\n");
    stored_jv_messages->z_kv_stage_buffer_out = 
      new ArrayOutputStream(stored_jv_messages->staging_backingstore,
  			    absolutemax);
    stored_jv_messages->cdo_kv_stage_buffer_out = 
      new CodedOutputStream(stored_jv_messages->z_kv_stage_buffer_out);
    stored_jv_messages->size_kv_stage_buffer_out = (uint32_t) totalsize;
    stored_jv_messages->count_kv_stage_buffer_out = 0;
    // don't forget to free these !!!
    // fprintf(stderr, "VREATINGE NEW %p\n",stored_jv_messages->cdo_kv_stage_buffer_out );

  }
  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_deserialize_1v
  (JNIEnv *env, jobject obj, jint f, jint tbytes){
    if(stored_jv_messages==NULL){
      JNU_throw_R_exception("WHY THE HELL IS stored_jv_messages NULL(deserialize_1v)?\n");
      return;
    }
    SEXP v,ans=R_NilValue;
    PROTECT(v = Rf_allocVector(VECSXP,f));
    CodedInputStream cds((uint8_t*)stored_jv_messages->staging_backingstore,tbytes);
    cds.SetTotalBytesLimit(REXP_MAX_SIZE,REXP_MAX_SIZE-1024*1024*10);
    int unpr = 1;
    uint32_t size;
    char * err;
    for(int i=0; i< f; i++){
      int a_;
      cds.ReadVarint32(&size);
      a_ = cds.PushLimit(size);
      stored_jv_messages->rexp_container->Clear();
      if(stored_jv_messages->rexp_container->ParseFromCodedStream(&cds)){
	PROTECT(ans = message2rexp(*(stored_jv_messages->rexp_container)));
	SET_VECTOR_ELT(v,i,ans);
	UNPROTECT(1);
      } else {
	err= "Error deserializing value";
	goto ERRS;
      }
      cds.PopLimit(a_);
    }
    Rf_setVar(Rf_install("reduce.values"),v,R_GlobalEnv);
    UNPROTECT(unpr);
    return;
    ERRS:
    UNPROTECT(unpr);
    if(! env->ExceptionCheck()) //already an exception
      JNU_throw_R_exception(err);
  }

  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_deserialize_1kv
  (JNIEnv *env, jobject obj, jint f, jint tbytes){
    if(stored_jv_messages==NULL){
      JNU_throw_R_exception("WHY THE HELL IS stored_jv_messages NULL?\n");
      return;
    }
    SEXP k,v,ans=R_NilValue;
    PROTECT(k = Rf_allocVector(VECSXP,f));
    PROTECT(v = Rf_allocVector(VECSXP,f));
    CodedInputStream cds((uint8_t*)stored_jv_messages->staging_backingstore,tbytes);
    cds.SetTotalBytesLimit(REXP_MAX_SIZE,REXP_MAX_SIZE-1024*1024*10);
    int unpr = 2;
    uint32_t size;
    char * err;
    for(int i=0; i< f; i++){
      int a_;
      cds.ReadVarint32(&size);
      a_ = cds.PushLimit(size);
      stored_jv_messages->rexp_container->Clear();
      if(stored_jv_messages->rexp_container->ParseFromCodedStream(&cds)){
	PROTECT(ans = message2rexp(*(stored_jv_messages->rexp_container)));
	SET_VECTOR_ELT(k,i,ans);
	UNPROTECT(1);
      } else {
	err= "Error deserializing key";
	goto ERRS;
      }
      cds.PopLimit(a_);
      cds.ReadVarint32(&size);
      a_ = cds.PushLimit(size);
      stored_jv_messages->rexp_container->Clear();
      if(stored_jv_messages->rexp_container->ParseFromCodedStream(&cds)){
	PROTECT(ans = message2rexp(*(stored_jv_messages->rexp_container)));
	SET_VECTOR_ELT(v,i,ans);
	UNPROTECT(1);
      } else {
	err = "Error deserializing value";
	goto ERRS;
      }
      cds.PopLimit(a_);      
      stored_jv_messages->rexp_container->Clear();
    }
    Rf_setVar(Rf_install("map.keys"),k,R_GlobalEnv);
    Rf_setVar(Rf_install("map.values"),v,R_GlobalEnv);
    UNPROTECT(unpr);
    return;
    ERRS:
    UNPROTECT(unpr);
    if(! env->ExceptionCheck()) //already an exception
      JNU_throw_R_exception(err);
  }
 
  JNIEXPORT void JNICALL Java_org_godhuli_rhipe_RHMRHelper_assign_1reduce_1key
  (JNIEnv *env, jobject obj, jstring name, jint bytes)  {
    CodedInputStream cds((uint8_t*)stored_jv_messages->reduce_key_backingstore,bytes);
    cds.SetTotalBytesLimit(REXP_MAX_SIZE,REXP_MAX_SIZE-1024*1024*10);
    SEXP k=R_NilValue;
    stored_jv_messages->rexp_container->Clear();
    if(stored_jv_messages->rexp_container->ParseFromCodedStream(&cds)){
      PROTECT(k = message2rexp(*(stored_jv_messages->rexp_container)));
    }else{
      if(! env->ExceptionCheck()) //already an exception
	JNU_throw_R_exception("Could not deparse reduce key");
      return;
    }
    Rf_setVar(Rf_install("reduce.key"), k,R_GlobalEnv);
    UNPROTECT(1);
  }
}

