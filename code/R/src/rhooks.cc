#include "ream.h"
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
    uint32_t idx;
    for (idx = 0; idx < len-1; idx++) {
      int8_t b = data[counter];
      counter++;
      i = i << 8;
    i = i | (b & 0xFF);
    }
    vint[1] = (uint32_t)(isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  

SEXP returnBytesForVInt(SEXP n0){
  int i = INTEGER(n0)[0];
  SEXP r ;
  int nm = getVIntSize(i);
  PROTECT(r = Rf_allocVector(RAWSXP,nm));
  int n2;
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
  long tmp = i;
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
  
}
