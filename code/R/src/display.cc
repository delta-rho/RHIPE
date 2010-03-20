#include "ream.h"
#include <time.h>
#include <locale.h>
#include <langinfo.h>
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



OutputInfo * oiinfo = new OutputInfo();


void Re_ShowMessage(const char* mess){
	Re_WriteConsoleEx(mess,strlen(mess),0);
}

void Re_WriteConsoleEx(const char *buf1, int len, int oType){
#ifndef FILEREADER
	switch(oType){
	case 0:
		fwrite(&PRINT_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
		break;
	case 1:
		fwrite(&ERROR_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
	}
	int len_rev =  reverseUInt(len);
	fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
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

SEXP counter(SEXP listmoo){
	REXP *rxp = new REXP();
	SEXP result;
	rxp->Clear();
	rexp2message(rxp,listmoo);
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

SEXP collect(SEXP k,SEXP v){
	// So not thread safe
#ifndef FILEREADER
	sendToHadoop(k);
	sendToHadoop(v);
#endif
	return(R_NilValue);
}

static inline void tobytes(SEXP x,std::string* result,uint32_t& size){
	REXP r = REXP();
	rexp2message(&r,x);
	size = r.ByteSize();
	r.SerializeToString(result);
}

void spill_to_reducer(void){
	SEXP comb_pre_red,comb_do_red,comb_post_red;
	rexpress("rhcollect<-function(key,value) .Call('rh_collect',key,value)");
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
		PROTECT(rkey = message2rexp(r));
		Rf_defineVar(Rf_install("reduce.key"),rkey,R_GlobalEnv);
		R_tryEval(comb_pre_red,NULL,&Rerr);
		fflush(NULL);
      		int i;
		vector<string> values = (*it).second;
		vector<string>::iterator itvalue;
		PROTECT(rvalues =  Rf_allocVector(VECSXP,values.size()));

		for (i=0, itvalue=values.begin(); itvalue !=values.end(); itvalue++,i++ ){
			REXP v;
			string aval = (*itvalue);
			v.ParseFromArray((void*)aval.data(),aval.length());
			// mmessage("The values are for %s ==%s", r.DebugString().c_str(),v.DebugString().c_str());
			SET_VECTOR_ELT(rvalues, i, message2rexp(v));
		}
		Rf_defineVar(Rf_install("reduce.values"),rvalues,R_GlobalEnv);
		R_tryEval(comb_do_red,NULL, &Rerr);
		R_tryEval(comb_post_red ,NULL, &Rerr);
		fflush(NULL);
      		UNPROTECT(2);
		fflush(NULL);
	}
	UNPROTECT(6);
	rexpress("rhcollect<-function(key,value) .Call('rh_collect_buffer',key,value)");
}

SEXP collect_buffer(SEXP k,SEXP v){
  static bool once = false;
  static std::string *ks;
  static std::string *vs;
  static uint32_t combiner_count;
  uint32_t ksize=0,vsize=0;

  if(!once){
    ks = new std::string();
    vs = new std::string();
    combiner_count = 0;
    once = true;
  }
  ks->clear();vs->clear();
  tobytes(k,ks,ksize);
  tobytes(v,vs,vsize);
  total_count += ksize+vsize;
  if( total_count >=  spill_size) {
    spill_to_reducer();
    total_count = 0;
    map_output_buffer.clear();
  }else{
    map_output_buffer[*ks].push_back(*vs);
  }
  // delete(ks);delete(vs);
  return(R_NilValue);
}


void sendToHadoop(SEXP k){

	int size;
	oiinfo->rxp->Clear();
	rexp2message(oiinfo->rxp,k);
	size = oiinfo->rxp->ByteSize();
	writeVInt64ToFileDescriptor( size , CMMNC->BSTDOUT);
	// if (size < PSIZE){
	oiinfo->rxp_s->clear();
	oiinfo->rxp->SerializeToString(oiinfo->rxp_s);
	fwrite( oiinfo->rxp_s->data(), size,1,CMMNC->BSTDOUT);
	// }else{
	//   oiinfo->rxp->SerializeToFileDescriptor(fileno(CMMNC->BSTDOUT));
	// }
	// fflush(CMMNC->BSTDOUT);
}

// SEXP readFromHadoop(const uint32_t nbytes,int *err){
//   SEXP r = R_NilValue;
//   SEXP rv ;
//   PROTECT(rv = Rf_allocVector(RAWSXP, nbytes));
//   if(fread(RAW(rv),nbytes,1,CMMNC->BSTDIN)<=0){
//     *err=1;
//     UNPROTECT(1);
//     return(R_NilValue);
//   }
//   REXP *rxp = new REXP();
//   if (rxp->ParseFromArray(RAW(rv),LENGTH(rv))){
//     LOGG(1,"%s\n", rxp->DebugString().c_str());

//     PROTECT(r = message2rexp(*rxp));
//     UNPROTECT(1);
//   }
//   UNPROTECT(1);
//   delete rxp;
//   return(r);
// }


SEXP readFromHadoop(const uint32_t nbytes,int *err){
	SEXP r = R_NilValue;
	oiinfo->rxp->Clear();
	if (nbytes > BSIZE)
	{
		oiinfo->inputbuffer=realloc(oiinfo->inputbuffer,nbytes+1024);
		if (!oiinfo->inputbuffer){
			merror("Memory Exhausted, could not realloc buffer in readFromHadoop\n");
			return(R_NilValue);
		}
		BSIZE=nbytes+1024;
	}
	*err=0;
	if(fread(oiinfo->inputbuffer,nbytes,1,CMMNC->BSTDIN)<=0){
		*err=1;
		return(R_NilValue);
	}
	CodedInputStream cds((uint8_t*)(oiinfo->inputbuffer),nbytes);
	cds.SetTotalBytesLimit(256*1024*1024,256*1024*1024);
	if (oiinfo->rxp->ParseFromCodedStream(&cds)){
		// if (oiinfo->rxp->ParseFromArray(oiinfo->inputbuffer,nbytes)){
		PROTECT(r = message2rexp(*(oiinfo->rxp)));
		UNPROTECT(1);
	}
	// a positive value in err is error
	return(r);
}



SEXP readFromMem(void * array,uint32_t nbytes){
	SEXP r = R_NilValue;
	oiinfo->rxp->Clear();
	if (nbytes > BSIZE)
	{
		oiinfo->inputbuffer=realloc(oiinfo->inputbuffer,nbytes+1024);
		if (!oiinfo->inputbuffer){
			merror("Memory Exhausted, could not realloc buffer in readFromHadoop\n");
			return(R_NilValue);
		}
		BSIZE=nbytes+1024;
	}
	if (oiinfo->rxp->ParseFromArray(array,nbytes)){
		PROTECT(r = message2rexp(*(oiinfo->rxp)));
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
		// if(rexp_container->ParseFromArray(RAW(robj),LENGTH(robj))){
		PROTECT(ans = message2rexp(*rexp_container));
		UNPROTECT(1);
	}
	delete(rexp_container);
	return(ans);
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
