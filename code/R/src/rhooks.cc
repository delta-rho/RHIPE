#include "ream.h"
#include <vector>
#include <iostream>
using namespace std;


static REXP *rexp_container = new REXP();

extern "C" {
  //Neither of these are thread safe...
  SEXP serializeUsingPB(SEXP robj)
  {
    rexp_container->Clear();
    rexp2message(rexp_container,robj);  
    int bs = rexp_container->ByteSize();
    SEXP result ;
    PROTECT(result = Rf_allocVector(RAWSXP,bs));
    rexp_container->SerializeWithCachedSizesToArray(RAW(result));
    UNPROTECT(1);
    return(result);
  }
  
  SEXP unserializeUsingPB(SEXP robj)
  {
    if (TYPEOF(robj)!=RAWSXP)
      Rf_error("Must pass a raw vector");
    SEXP ans;
    // REXP *rexp = new REXP();
    rexp_container->Clear();
    rexp_container->ParseFromArray(RAW(robj),LENGTH(robj));
    PROTECT(ans = message2rexp(*rexp_container));
    UNPROTECT(1);
    return(ans);
  }
  


  void readVInt_from_R(const unsigned char *data, int *vint){
    /**
     * Given the RAW vector in data
     * vint[0] is the number of bytes it takes up
     * vint[1] is the actual value
     **/
    
    int8_t firstByte = (int8_t)data[0];
    uint32_t len = decodeVIntSize(firstByte);
    vint[0]=len;
    if (len == 1) {
      vint[1] =  (int8_t)firstByte;
      return;
    }
    int counter=1;
    int64_t i = 0;
    int32_t idx;
    for (idx = 0; idx < len-1; idx++) {
      int8_t b = (int8_t) data[counter];
      counter++;
      i = i << 8;
      i = i | (b & 0xFF);
    }
    vint[1] = (int32_t)(isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  

  SEXP returnBytesForVInt(SEXP n0){
    int i = INTEGER(n0)[0];
    SEXP r ;
    int nm = getVIntSize(i);
    PROTECT(r = Rf_allocVector(RAWSXP,nm));
    char x ;
    if (i >= -112 && i <= 127) {
      x=(char)i;
      /* writen(fd,&x,sizeof(x)); */
      RAW(r)[0] = x;
      UNPROTECT(1);
      return(r);
    }
    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }
    long long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }
    x=(char)len;
    RAW(r)[0]=x;
    len = (len < -120) ? -(len + 120) : -(len + 112);
    int idx,counter;
    for (idx = len,counter=1; idx != 0; idx--,counter++) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      x = (char)((i & mask) >> shiftbits);
      RAW(r)[counter]=x;
    }
    UNPROTECT(1);
    return(r);
  }
 

  static SEXP NewList(void)
  {
    SEXP s = Rf_cons(R_NilValue, R_NilValue);
    SETCAR(s, s);
    return s;
  }
  
  /* Add a new element at the end of a stretchy list */
  
  static SEXP GrowList(SEXP l, SEXP s)
  {
    SEXP tmp;
    PROTECT(s);
    tmp = Rf_cons(s, R_NilValue);
    UNPROTECT(1);
    SETCDR(CAR(l), tmp);
    SETCAR(l, tmp);
    return l;
  }
  
  /* Insert a new element at the head of a stretchy list */
  
  // static SEXP Insert(SEXP l, SEXP s)
  // {
  //   SEXP tmp;
  //   PROTECT(s);
  //   tmp = Rf_cons(s, CDR(l));
  //   UNPROTECT(1);
  //   SETCDR(l, tmp);
  //   return l;
  // }
  

  SEXP readBinaryFile(SEXP filename0, SEXP max0,SEXP bf){
    SEXP rv = R_NilValue;
    int max = INTEGER(max0)[0];
    char *filename =  (char*)CHAR(STRING_ELT( filename0 , 0));

    FILE *fp = fopen(filename,"rb");
    if(!fp) Rf_error("Could not open filename, error=%d",errno);

    int buffsize = INTEGER(bf)[0];
    if(buffsize == 0)
      buffsize = BUFSIZ;
    int w=64*1024;
    Rf_warning("Using read buffer size:%d, data store:%d",buffsize,w);
    char* buffer = (char*)malloc(buffsize*sizeof(char));
    setbuffer(fp,buffer, buffsize);

    int32_t kvlength;
    int32_t count=0;
    void * kvhold = (void*)malloc(w);
    REXP *rexp = new REXP();
    SEXP k,v,l;
    

    PROTECT( rv = NewList());

    while(true){
      PROTECT(l = Rf_allocVector(VECSXP,2));
      // read key
      fread(&kvlength, sizeof(int32_t),1,fp);
      kvlength = reverseUInt(kvlength);
      if( kvlength > w){
	kvhold = realloc( kvhold, kvlength + 2048);
	w= kvlength+2048;
      }
      int d_;
      if( (d_=fread(kvhold, kvlength, 1, fp)) <= 0){
	if(d_<0) Rf_warning("There was an issue reading this file:%d",errno);
	UNPROTECT(1);
	break;
      }
      rexp->Clear();
      rexp->ParseFromArray( kvhold, kvlength );
      PROTECT(k = message2rexp(*rexp));
      SET_VECTOR_ELT( l, 0, k);
      // Rf_PrintValue(k);
      // read value
      fread(&kvlength, sizeof(int32_t),1,fp);
      kvlength = reverseUInt(kvlength);

      if( kvlength > w){
	kvhold = realloc( kvhold, kvlength + 2048);
	w = kvlength+2048;
      }
       if( (d_=fread(kvhold, kvlength, 1, fp)) <= 0){
	if(d_<0) Rf_warning("There was an issue reading this file:%d",errno);
	UNPROTECT(2);
	break;
      }
      rexp->Clear();
      rexp->ParseFromArray( kvhold, kvlength );
      PROTECT(v = message2rexp(*rexp));
      // Rf_PrintValue(v);

      SET_VECTOR_ELT( l, 1, v);
      UNPROTECT(3);
      rv = GrowList(rv, l);

      count++;
      R_CheckUserInterrupt();

      if(max >=0 && count >= max) break;
    }

    rv = CDR(rv);
    SEXP rval;
    PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
    for (int n = 0 ; n < LENGTH(rval) ; n++, rv = CDR(rv))
      SET_VECTOR_ELT(rval, n, CAR(rv));
    UNPROTECT(2);

    delete(rexp);
    free(kvhold);
    free(buffer);
    fclose(fp);
    return(rval);
  }
 
}
