#include "ream.h"
#include <time.h>
#include <locale.h>
#include <langinfo.h>

#import "iostream"
#define PSIZE 4096

using namespace std;
void sendToHadoop(SEXP);
int BSIZE= 32768;
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
  switch(oType){
  case 0:
    fwrite(&PRINT_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
    break;
  case 1:
    fwrite(&ERROR_MSG,sizeof(uint8_t),1,CMMNC->BSTDERR);
  }
  int len_rev =  reverseUInt(len);
  fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
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

void merror(char *fmt, ...)
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

void counter(SEXP grouppattern){
  char *group = (char*)CHAR(STRING_ELT( grouppattern , 0));
  fwrite(&SET_COUNTER,sizeof(uint8_t),1,CMMNC->BSTDERR);
  uint32_t stle = strlen(group);
  uint32_t len_rev =  reverseUInt(stle);
  fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
  fwrite(group,stle,1,CMMNC->BSTDERR);
}

void status(SEXP mess){
  if(TYPEOF(mess)!=STRSXP){
    Rf_error("Must give a string");
    return;
  }
  char *status = (char*)CHAR(STRING_ELT( mess , 0));
  fwrite(&SET_STATUS,sizeof(uint8_t),1,CMMNC->BSTDERR);
  uint32_t stle = strlen(status);
  uint32_t len_rev =  reverseUInt(stle);
  fwrite(&len_rev,sizeof(uint32_t),1,CMMNC->BSTDERR);
  fwrite(status,stle,1,CMMNC->BSTDERR);
}

void collect(SEXP k,SEXP v){
  // So not thread safe
  sendToHadoop(k);
  sendToHadoop(v);

}

void sendToHadoop(SEXP k){

  int size;
  oiinfo->rxp->Clear();
  rexp2message(oiinfo->rxp,k);
  size = oiinfo->rxp->ByteSize();
  writeVInt64ToFileDescriptor( size , CMMNC->BSTDOUT);
  if (size < PSIZE){
    oiinfo->rxp_s->clear();
    oiinfo->rxp->SerializeToString(oiinfo->rxp_s);
    fwrite( oiinfo->rxp_s->data(), size,1,CMMNC->BSTDOUT);
  }else{
    oiinfo->rxp->SerializeToFileDescriptor(fileno(CMMNC->BSTDOUT));
  }
}

SEXP readFromHadoop(uint32_t nbytes){
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
  fread(oiinfo->inputbuffer,nbytes,1,CMMNC->BSTDIN);
  if (oiinfo->rxp->ParseFromArray(oiinfo->inputbuffer,nbytes)){
    PROTECT(r = message2rexp(*(oiinfo->rxp)));
    UNPROTECT(1);
  }
  return(r);
}
