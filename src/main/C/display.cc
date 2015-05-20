#include "ream.h"
#include <time.h>
#include <locale.h>
#include <langinfo.h>
#include <arpa/inet.h>
#include <google/protobuf/io/coded_stream.h>

using namespace google::protobuf::io;

#include <iostream>
#define PSIZE 4096
static uint8_t ERROR_MSG = 0x00;
static uint8_t PRINT_MSG = 0x01;
static uint8_t SET_STATUS = 0x02;
static uint8_t SET_COUNTER = 0x03;
uint32_t total_count;
using namespace std;
void sendToHadoop(SEXP);
uint32_t BSIZE= 32768;
class OutputInfo{
public:
	REXP *rxp;
	string *rxp_s;
	void* inputbuffer;
	OutputInfo(void){
		rxp = new REXP();
		rxp_s = new string();
		inputbuffer = (void*)malloc(BSIZE);
	}

	~OutputInfo(){
		delete rxp;
		delete rxp_s;
		free(inputbuffer);
	}
};



OutputInfo oiinfo;// = new OutputInfo();


void Re_ShowMessage(const char* mess){
	Re_WriteConsoleEx(mess,strlen(mess),0);
}

void Re_WriteConsoleEx(const char *buf1, int len, int oType){
#ifndef FILEREADER
	switch(oType){
	case 0:
	  {
		fwrite(&PRINT_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
		int len_rev =  reverseUInt(len);
		fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
		fwrite(buf1,len,1,CMMNC->BSTDERR);
		return;
	  }
	case 1:
	  {
	    const char* state =CHAR(STRING_ELT( Rf_findVar(Rf_install(".rhipe.current.state"), R_GlobalEnv),0));
	    char buffme[4096];
	    snprintf(buffme,4096,"\nR ERROR BEGIN (%s):\n=============\n\n%s\nR ERROR END\n===========\n",state,buf1);
	    int k = strlen(buffme);
	    fwrite(&ERROR_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
	    int len_rev =  reverseUInt(k);
	    fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
	    fwrite(buffme,k,1,CMMNC->BSTDERR);
	    return;
	  }
	}
#endif
	fwrite(buf1,len,1,CMMNC->BSTDERR);
}

void logg(int level,const char *fmt, ...)
{
	if (level >= DLEVEL)
	{
		char s[100];
		size_t i;
		struct tm tim;
		time_t now;
		now = time(NULL);
		tim = *(localtime(&now));
		i = strftime(s,30,"%b %d, %Y; %H:%M:%S",&tim);
		s[i]='\0';
		va_list args;
		va_start(args,fmt);
		fprintf(LOG,"%s[%02d]<%d>: ",s,level,_STATE_);
		vfprintf(LOG,fmt,args);
		va_end(args);
		fflush(LOG);
	}
}

void mcount(const char *g1, const char* g2, uint32_t n){
  SEXP g,s1,s2,s3;
  PROTECT(g = Rf_allocVector(VECSXP,3));

  PROTECT(s1 = Rf_allocVector(STRSXP,1));
  PROTECT(s2 = Rf_allocVector(STRSXP,1));
  PROTECT(s3 = Rf_allocVector(REALSXP,1));

  SET_STRING_ELT(s1,0,Rf_mkChar(g1));
  SET_VECTOR_ELT(g,0,s1);

  SET_STRING_ELT(s2,0,Rf_mkChar(g2));
  SET_VECTOR_ELT(g,1,s2);

  REAL(s3)[0] = (double)n;
  SET_VECTOR_ELT(g,2,s3);
  
  UNPROTECT(3);

  counter(g);
  UNPROTECT(1);

}
void merror(const char *fmt, ...)
{
	va_list args;
	char errmsg[512];
	va_start(args,fmt);
	vsnprintf(errmsg,sizeof(errmsg),fmt,args);
	va_end(args);
	Re_WriteConsoleEx(errmsg,strlen(errmsg),1);
}

void mmessage(char *fmt, ...)
{
	va_list args;
	char errmsg[512];
	va_start(args,fmt);
	vsnprintf(errmsg,sizeof(errmsg),fmt,args);
	va_end(args);
	Re_WriteConsoleEx(errmsg,strlen(errmsg),0);
}

extern "C" {
  SEXP counter(SEXP listmoo){
	REXP *rxp = new REXP();
	SEXP result;
	rxp->Clear();
	sexpToRexp(rxp,listmoo);
	int size = rxp->ByteSize();
	PROTECT(result = Rf_allocVector(RAWSXP,size));
	if(result != R_NilValue){
		fwrite(&SET_COUNTER,sizeof(uint8_t),1,CMMNC->BSTDERR);
		writeVInt64ToFileDescriptor( size , CMMNC->BSTDERR);
		rxp->SerializeWithCachedSizesToArray(RAW(result));
		fwrite(RAW(result), size,1,CMMNC->BSTDERR);
	}
	UNPROTECT(1);
	delete rxp;
	return(R_NilValue);
  }
}

extern "C" {
  SEXP status(SEXP mess){
    if(TYPEOF(mess)!=STRSXP){
		Rf_error("Must give a string");
		return(R_NilValue);
    }
    char *status = (char*)CHAR(STRING_ELT( mess , 0));
    fwrite(&SET_STATUS,sizeof(uint8_t),1,CMMNC->BSTDERR);
    uint32_t stle = strlen(status);
    uint32_t len_rev =  reverseUInt(stle);
    fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
    fwrite(status,stle,1,CMMNC->BSTDERR);
    return(R_NilValue);
  }
}

extern "C" {
  SEXP collect(SEXP k,SEXP v){
    // So not thread safe
#ifdef USETIMER
    struct timeval tms;
    long int bstart, bend;
    gettimeofday(&tms,NULL);
    bstart = tms.tv_sec*1000000 + tms.tv_usec;
#endif
    
#ifndef FILEREADER
    sendToHadoop(k);
    sendToHadoop(v);
#endif
    
#ifdef USETIMER
    gettimeofday(&tms,NULL);
    bend = tms.tv_sec*1000000 + tms.tv_usec;
    collect_total += (bend - bstart);
#endif
    
    return(R_NilValue);
  }
}

SEXP collectList(SEXP k, SEXP v){
  //if I have to do this operation very often I will write  a zipped
  //list apply (from Python).
  if(!Rf_isNewList(k) || !Rf_isNewList(v)) 
    Rf_error("Argument must be a list.");
  if(Rf_isNull(k) || Rf_isNull(v))
    Rf_error("Argument must not be NULL");  //Turns out NULL is a list.
  if(LENGTH(k) != LENGTH(v))
    Rf_error("Key list must be same length as Value list.");
  R_len_t len = LENGTH(k);
  for(R_len_t i = 0; i < len; ++i){
    sendToHadoop(VECTOR_ELT(k,i));
    sendToHadoop(VECTOR_ELT(v,i));
  }
  return R_NilValue;
}


static inline uint32_t tobytes(SEXP x,std::string* result){
	REXP r = REXP();
	sexpToRexp(&r,x);
	uint32_t size = r.ByteSize();
	r.SerializeToString(result);
	return(size);
}

void spill_to_reducer(void){
#ifdef USETIMER
  struct timeval tms;
  long int bstart, bend;
  gettimeofday(&tms,NULL);
  bstart = tms.tv_sec*1000000 + tms.tv_usec;
  // char x[512];
  // sprintf(x,"Timers-%s-%d",getenv("mapred.task.id"),getpid());
  // mcount(x,"Num Spills to Reducer",1);
#endif
  // rexpress("rhcounter('combiner','spill_to_reducer',1)");
  // uint32_t bytes_received = 0;  
	SEXP comb_pre_red,comb_do_red,comb_post_red;
	rexpress(".rhipe.current.state<-'map.combine';rhcollect<-function(key,value) .Call('rh_collect',key,value)");
	SEXP dummy1,dummy2,dummy3;
	PROTECT(dummy1=rexpress(REDUCEPREKEY));
	PROTECT(comb_pre_red=Rf_lang2(Rf_install("eval"),dummy1));

	PROTECT(dummy2=rexpress(REDUCE));
	PROTECT(comb_do_red=Rf_lang2(Rf_install("eval"),dummy2));
	
	PROTECT(dummy3=rexpress(REDUCEPOSTKEY));
	PROTECT(comb_post_red=Rf_lang2(Rf_install("eval"),dummy3));

	map<string, vector<string> >::iterator it;
	for(it=map_output_buffer.begin(); it!=map_output_buffer.end(); it++){
		string key = (*it).first;
		//we'll create reduce.key and reduce.values
		SEXP rkey, rvalues;
		int Rerr=0;

		REXP r = REXP();
		r.ParseFromArray((void*)key.data(),key.length());
		// bytes_received+=key.length();
		PROTECT(rkey = rexpToSexp(r));
		Rf_defineVar(Rf_install("reduce.key"),rkey,R_GlobalEnv);
		WRAP_R_EVAL_FOR_REDUCE(comb_pre_red,NULL,&Rerr);
		fflush(NULL);
      		int i;
		vector<string> values = (*it).second;
		vector<string>::iterator itvalue;
		PROTECT(rvalues =  Rf_allocVector(VECSXP,values.size()));

		for (i=0, itvalue=values.begin(); itvalue !=values.end(); itvalue++,i++ ){
			REXP v;
			string aval = (*itvalue);
			v.ParseFromArray((void*)aval.data(),aval.length());
			// bytes_received+=aval.length();
			// mmessage("The values are for %s ==%s", r.DebugString().c_str(),v.DebugString().c_str());
			SET_VECTOR_ELT(rvalues, i, rexpToSexp(v));
		}
		Rf_defineVar(Rf_install("reduce.values"),rvalues,R_GlobalEnv);
		WRAP_R_EVAL_FOR_REDUCE(comb_do_red,NULL, &Rerr);
		WRAP_R_EVAL_FOR_REDUCE(comb_post_red ,NULL, &Rerr);
		fflush(NULL);
      		UNPROTECT(2);
		fflush(NULL);
	}
	UNPROTECT(6);
	rexpress("rhcollect<-function(key,value) .Call('rh_collect_buffer',key,value)");
#ifdef USETIMER
  gettimeofday(&tms,NULL);
  bend = tms.tv_sec*1000000 + tms.tv_usec;
  collect_spill_total += (bend - bstart);
#endif

}

extern "C" {
  SEXP collect_buffer(SEXP k,SEXP v){
    
    static bool once = false;
    static std::string *ks;
    static std::string *vs;
    uint32_t ksize=0,vsize=0;
    if(!once){
      ks = new std::string();
      vs = new std::string();
      once = true;
    }
    ks->clear();vs->clear();
    ksize=tobytes(k,ks);
    vsize=tobytes(v,vs);
    total_count += ksize+vsize;
    map_output_buffer[*ks].push_back(*vs);
    if( total_count >=  spill_size) {
      mcount("combiner","bytesent",total_count);
      spill_to_reducer();
      total_count = 0;
      map_output_buffer.clear();
    }// else{
    //   map_output_buffer[*ks].push_back(*vs);
    // }
    // delete(ks);delete(vs);
#ifdef USETIMER
    gettimeofday(&tms,NULL);
    bend = tms.tv_sec*1000000 + tms.tv_usec;
    collect_buffer_total += (bend - bstart);
#endif
    
    return(R_NilValue);
  }
}


void sendToHadoop(SEXP k){
  
	int size;
	oiinfo.rxp->Clear();
	sexpToRexp(oiinfo.rxp,k);
	size = oiinfo.rxp->ByteSize();
	
	// uint32_t nsize = htonl(size);
	writeVInt64ToFileDescriptor( size , CMMNC->BSTDOUT);
	// fwrite(&nsize, sizeof(nsize),1,CMMNC->BSTDOUT);
	// if (size < PSIZE){
	oiinfo.rxp_s->clear();
	oiinfo.rxp->SerializeToString(oiinfo.rxp_s);
	fwrite( oiinfo.rxp_s->data(), size,1,CMMNC->BSTDOUT);
	// }else{
	//   oiinfo.rxp->SerializeToFileDescriptor(fileno(CMMNC->BSTDOUT));
	// }
	// fflush(CMMNC->BSTDOUT);
}




SEXP readFromHadoop(const uint32_t nbytes,int *err){
	SEXP r = R_NilValue;
	oiinfo.rxp->Clear();
	if (nbytes > BSIZE)
	{
		oiinfo.inputbuffer=realloc(oiinfo.inputbuffer,nbytes+1024);
		if (!oiinfo.inputbuffer){
			merror("Memory Exhausted, could not realloc buffer in readFromHadoop\n");
			return(R_NilValue);
		}
		BSIZE=nbytes+1024;
	}
	*err=0;
	if(fread(oiinfo.inputbuffer,nbytes,1,CMMNC->BSTDIN)<=0){
		*err=1;
		return(R_NilValue);
	}
	CodedInputStream cds((uint8_t*)(oiinfo.inputbuffer),nbytes);
	cds.SetTotalBytesLimit(256*1024*1024,256*1024*1024);
	if (oiinfo.rxp->ParseFromCodedStream(&cds)){
		// if (oiinfo.rxp->ParseFromArray(oiinfo.inputbuffer,nbytes)){
		PROTECT(r = rexpToSexp(*(oiinfo.rxp)));
		UNPROTECT(1);
	}
	// a positive value in err is error
	return(r);
}



SEXP readFromMem(void * array,uint32_t nbytes){
	SEXP r = R_NilValue;
	oiinfo.rxp->Clear();
	if (nbytes > BSIZE)
	{
		oiinfo.inputbuffer=realloc(oiinfo.inputbuffer,nbytes+1024);
		if (!oiinfo.inputbuffer){
			merror("Memory Exhausted, could not realloc buffer in readFromHadoop\n");
			return(R_NilValue);
		}
		BSIZE=nbytes+1024;
	}
	if (oiinfo.rxp->ParseFromArray(array,nbytes)){
		PROTECT(r = rexpToSexp(*(oiinfo.rxp)));
		UNPROTECT(1);
	}
	return(r);
}

int32_t readJavaInt(FILE* fp){
	int32_t t = 0,rt=0;
	fread(&t,sizeof(int32_t),1,fp);
	rt = reverseUInt(t); //something not good here, rt is int32_t
	return(rt);
}
    
SEXP persUnser(SEXP robj)
{
	SEXP ans  = R_NilValue;
	REXP *rexp_container = new REXP();
	CodedInputStream cds(RAW(robj),LENGTH(robj));
	cds.SetTotalBytesLimit(256*1024*1024,256*1024*1024);
	if(rexp_container->ParseFromCodedStream(&cds)){
	  PROTECT(ans = rexpToSexp(*rexp_container));
	  UNPROTECT(1);
	}
	delete(rexp_container);
	return(ans);
}

SEXP persSer(SEXP robj){
  REXP *rexp_container = new REXP();
  rexp_container->Clear();
  sexpToRexp(rexp_container, robj);
  int bs = rexp_container->ByteSize();
  SEXP result = R_NilValue;
  PROTECT(result = Rf_allocVector(RAWSXP,bs));
  rexp_container->SerializeWithCachedSizesToArray(RAW(result));
  UNPROTECT(1);
  delete (rexp_container);
  return (result);
}


SEXP dbgstr(SEXP robj)
{
	SEXP ans  = R_NilValue;
	REXP *rexp_container = new REXP();
	rexp_container->ParseFromArray(RAW(robj),LENGTH(robj));
	PROTECT(ans = Rf_allocVector(STRSXP,1));
	SET_STRING_ELT(ans,0,Rf_mkChar(rexp_container->DebugString().c_str()));    
	delete(rexp_container);
	UNPROTECT(1);
	return(ans);
}
