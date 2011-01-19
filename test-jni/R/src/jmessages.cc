#include "ream.h"
#include <jni.h>


JMessages::JMessages(jobject ob){
  JNIEnv *jenv = JNU_GetEnv();
  engineObject =jenv->NewGlobalRef(ob);
  engineClass=jenv->NewGlobalRef(jenv->GetObjectClass(engineObject));
  rexp_container = new REXP();
  // spill_method_class = (jenv)->FindClass("org/godhuli/rhipe/RHMRHelper");
  spill_method_id = (jenv)->GetMethodID( (jclass)engineClass, "send_kv", "(I)V");
  JNU_system_print("[JMESSAGES] New JMessage\n",0);
  z_kv_stage_buffer_out = NULL;
  cdo_kv_stage_buffer_out = NULL;
}
JMessages::~JMessages(){
  JNU_system_print("[JMESSAGES] Deleting JMessage\n",0);
  // JNIEnv *jenv = JNU_GetEnv();
  // if(staging_buffer)
  //   (jenv)->DeleteLocalRef( staging_buffer);
  // if(staging_backingstore)
  //   free(staging_backingstore);
  // if(reduce_key_buffer)
  //   (jenv)->DeleteLocalRef( reduce_key_buffer);
  // if(reduce_key_backingstore)
  //   free(staging_backingstore);

  delete rexp_container;
}

jobject JMessages::create_storage(jint size){
  JNU_system_print("[JMESSAGES] New Storage\n",0);
  JNIEnv *jenv = JNU_GetEnv();
  staging_bytes = size;
  staging_backingstore = malloc( staging_bytes);
  if(staging_backingstore == NULL)
    JNU_throw_R_exception("Could not allocate map buffer backing store for keys");
  staging_buffer = (jenv)->NewDirectByteBuffer(staging_backingstore,  staging_bytes);
  return staging_buffer;
}
jobject JMessages::create_reduce_key_storage(jint size){
  JNU_system_print("[JMESSAGES] New Reduce Key Storage\n",0);
  JNIEnv *jenv = JNU_GetEnv();
  reduce_key_bytes = size;
  reduce_key_backingstore = malloc( reduce_key_bytes);
  if(reduce_key_backingstore == NULL)
    JNU_throw_R_exception("Could not allocate reduce key buffer backing store");
  reduce_key_buffer = (jenv)->NewDirectByteBuffer(reduce_key_backingstore,  reduce_key_bytes);
  return reduce_key_buffer;
}


void JMessages::cdo_staged_writer(SEXP k,SEXP v){
  JNIEnv *jenv = JNU_GetEnv();
  int message_size;
  this->rexp_container->Clear();
  rexp2message(this->rexp_container,k);
  message_size = this->rexp_container->ByteSize();
  this->cdo_kv_stage_buffer_out->WriteVarint32(message_size);
  if(!this->rexp_container->SerializeToCodedStream(this->cdo_kv_stage_buffer_out)){
    delete cdo_kv_stage_buffer_out;
    delete z_kv_stage_buffer_out;
    if(! jenv->ExceptionCheck())
      JNU_throw_R_exception("Could not serialize key to coded output stream");
    return;
  }

  rexp_container->Clear();
  rexp2message(this->rexp_container,v);
  message_size = rexp_container->ByteSize();
  this->cdo_kv_stage_buffer_out->WriteVarint32(message_size);
  if(!rexp_container->SerializeToCodedStream(this->cdo_kv_stage_buffer_out)){
    delete cdo_kv_stage_buffer_out;
    delete  z_kv_stage_buffer_out;
    if(! jenv->ExceptionCheck())
      JNU_throw_R_exception("Could not serialize value to coded output stream");
    return;
  }
  this->count_kv_stage_buffer_out ++;
  if (cdo_kv_stage_buffer_out->ByteCount() >= this->size_kv_stage_buffer_out){
    // CALL JAVA SIDE SPOOL
    // delete cdo_kv_stage_buffer_out;
    // delete z_kv_stage_buffer_out;
    // z_kv_stage_buffer_out = NULL;
    // cdo_kv_stage_buffer_out = NULL;
    (jenv)->CallVoidMethod(engineObject,
			    spill_method_id, (jint) this->count_kv_stage_buffer_out);
    this->count_kv_stage_buffer_out= 0;
  }
}
