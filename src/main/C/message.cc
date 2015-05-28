#include <iostream>
#include <rexp.pb.h>
#include "ream.h"

using namespace std;

#if R_VERSION < R_Version(2,7,0)                                                                                                                                                                                                          
#define mkCharUTF8(X) Rf_mkChar(X)
#else                                                                                                                                                                                                                                     
#define mkCharUTF8(X) Rf_mkCharCE(X, CE_UTF8)
#endif  

SEXP rexpress(const char* cmd)
{
  SEXP cmdSexp, cmdexpr, ans = R_NilValue;
  int i;
  ParseStatus status;
  PROTECT(cmdSexp = Rf_allocVector(STRSXP, 1));
  SET_STRING_ELT(cmdSexp, 0, Rf_mkChar(cmd));
  cmdexpr = PROTECT(R_ParseVector(cmdSexp, -1, &status, R_NilValue));
  if (status != PARSE_OK) {
    UNPROTECT(2);
    Rf_error("invalid call: %s", cmd);
    return(R_NilValue);
  }
  for(i = 0; i < Rf_length(cmdexpr); i++)
    ans = Rf_eval(VECTOR_ELT(cmdexpr, i), R_GlobalEnv);
  UNPROTECT(2);
  return(ans);
}

// call (void)Rf_PrintValue(robj) in gdb



SEXP rexpToSexp(const REXP& rexp){
  SEXP s = R_NilValue;
  int length;
  static int convertLogical[3]={0,1,NA_LOGICAL};
  switch(rexp.rclass()){

  case REXP::NULLTYPE:
    return(R_NilValue);
  case REXP::LOGICAL:
    length = rexp.booleanvalue_size();
    PROTECT(s = Rf_allocVector(LGLSXP,length));
    for (int i = 0; i<length; i++)
      {
  	REXP::RBOOLEAN v = rexp.booleanvalue(i);
  	LOGICAL(s)[i] = convertLogical[1*v];
      }
    break;
  case REXP::INTEGER:
    length = rexp.intvalue_size();
    PROTECT(s = Rf_allocVector(INTSXP,length));
    for (int i = 0; i<length; i++)
      INTEGER(s)[i] = rexp.intvalue(i);
    break;
  case REXP::REAL:
    length = rexp.realvalue_size();
    PROTECT(s = Rf_allocVector(REALSXP,length));
    for (int i = 0; i<length; i++)
      REAL(s)[i] = rexp.realvalue(i);
    break;
  case REXP::RAW:
    {
      const string& r = rexp.rawvalue();
      length = r.size();
      PROTECT(s = Rf_allocVector(RAWSXP,length));
      memcpy(RAW(s),r.data(),length);
      break;
    }
  case REXP::COMPLEX:
    length = rexp.complexvalue_size();
    PROTECT(s = Rf_allocVector(CPLXSXP,length));
    for (int i = 0; i<length; i++){
      COMPLEX(s)[i].r = rexp.complexvalue(i).real();
      COMPLEX(s)[i].i = rexp.complexvalue(i).imag();
    }
    break;
  case REXP::STRING:
    {
      length = rexp.stringvalue_size();
      PROTECT(s = Rf_allocVector(STRSXP,length));
      STRING st;
      for (int i = 0; i<length; i++){
      	st= rexp.stringvalue(i);
      	if (st.isna())
      	  SET_STRING_ELT(s,i,R_NaString);
      	else{
	  SEXP y=  mkCharUTF8(st.strval().c_str());
      	  SET_STRING_ELT(s,i,y);
	}
      }
      break;
    }
  case REXP::LIST:
    length = rexp.rexpvalue_size();
    PROTECT(s = Rf_allocVector(VECSXP,length));
    for (int i = 0; i< length; i++){
      // SEXP ik;
      SET_VECTOR_ELT(s, i, rexpToSexp(rexp.rexpvalue(i)) );
    }
    break;
  case REXP::ENVIRONMENT:
    length = rexp.envvalue_size();
    PROTECT(s = Rf_allocSExp(ENVSXP));
    ENV e;
    for(int i=0; i< length;i++){
      e = rexp.envvalue(i);
      const char * name = e.key().c_str();
      SEXP v = rexpToSexp(e.value());
      Rf_defineVar(Rf_install(name),v ,s);
    }
    break;
  }
  int atlength = rexp.attrname_size();
  // int typ = TYPEOF(s);
  if (atlength>0  )
    {
      for (int j=0; j<atlength; j++)
  	{
	  Rf_setAttrib(s,
	  	       Rf_install(rexp.attrname(j).c_str()), 
	  	       rexpToSexp(rexp.attrvalue(j)));


  	}
    }
  UNPROTECT(1);
  return(s); //Rf_duplicate(s)); //iff not forthis things crash, dont know why.
}


void sexpToRexp(REXP* rxp,const SEXP robj){
  fill_rexp(rxp,robj);
}



void fill_rexp(REXP* rexp,const SEXP robj){
  
  SEXP xx =   ATTRIB(robj);
  if (xx!=R_NilValue)
    {
      for (SEXP s = ATTRIB(robj); s != R_NilValue; s = CDR(s))
	{
	  // Rf_PrintValue(s);
	  rexp->add_attrname(CHAR(PRINTNAME(TAG(s))));
	  fill_rexp(rexp->add_attrvalue(),
	  	    CAR(s));
	}
    }
  switch(TYPEOF(robj)){
  case LGLSXP:
    rexp->set_rclass(REXP::LOGICAL);
    for (int i = 0; i< LENGTH(robj); i++)
      {
	int d = LOGICAL(robj)[i];
	    switch(d){
	    case 0:
	      rexp->add_booleanvalue(REXP::F);
	      break;
	    case 1:
	      rexp->add_booleanvalue(REXP::T);
	      break;
	    default:
	      rexp->add_booleanvalue(REXP::NA);
	      break;
	    }
      }
    break;
  case INTSXP:
    rexp->set_rclass(REXP::INTEGER);
    for (int i=0; i<LENGTH(robj); i++)
      rexp->add_intvalue(INTEGER(robj)[i]);
    break;
  case REALSXP:
    rexp->set_rclass(REXP::REAL);
    for (int i=0; i<LENGTH(robj); i++)
      rexp->add_realvalue(REAL(robj)[i]);
    break;
  case RAWSXP:{
    rexp->set_rclass(REXP::RAW);
    int l = LENGTH(robj);
    rexp->set_rawvalue((const char*)RAW(robj),l);
    break;
  }
  case CPLXSXP:{
    rexp->set_rclass(REXP::COMPLEX);
    for (int i = 0; i<LENGTH(robj); i++)
      {
	CMPLX *mp = rexp->add_complexvalue();
	mp->set_real(COMPLEX(robj)[i].r);
	mp->set_imag(COMPLEX(robj)[i].i);
      }
    break;
  }
  case NILSXP:{
    rexp->set_rclass(REXP::NULLTYPE);
    break;
  }
  case STRSXP:{
    rexp->set_rclass(REXP::STRING);
    for (int i=0; i<LENGTH(robj); i++){
      STRING* cm = rexp->add_stringvalue();
      if (STRING_ELT(robj,i)==NA_STRING)
	cm->set_isna(true);
      else
	cm->set_strval(CHAR(STRING_ELT(robj,i)));
    }
    break;
  }
  case VECSXP:{
    rexp->set_rclass(REXP::LIST);
    for (int i = 0; i<LENGTH(robj); i++)
  	fill_rexp(rexp->add_rexpvalue(),VECTOR_ELT(robj,i));
    break;
  }
  case ENVSXP:{
    if(R_IsPackageEnv(robj) || R_IsNamespaceEnv(robj)) break;
    rexp->set_rclass(REXP::ENVIRONMENT);
    SEXP fieldnames = R_lsInternal(robj,TRUE);
    for(int i = 0; i< LENGTH(fieldnames);i++){
      ENV* entry = rexp->add_envvalue();
      const char* name = CHAR(STRING_ELT(fieldnames,i));
      entry->set_key(name);
      fill_rexp(entry->mutable_value(), Rf_findVar(Rf_install(name), robj));
    }
    break;
  }
 default:
   rexp->set_rclass(REXP::NULLTYPE);
   break;
  }
 
}
  

/*
 * writeSEXP64
 * Converts an R object (not a list or vector of objects or other container type) and writes it to stream.
 * Based on contents of sendToHadoop written by saptarshi
 * input: fout file to output bytes
 * input: pbyte_buffer buffer to write serialized rexp object too.
 * input: pbuffer_size max allocated length of byte_buffer
 * input: prexp_buffer rexp object to use for converting obj
 * input: object to be written.
 * Writes the serialized bytes to stream WITH a prepend of byte length.
 * SIDE EFFECTS: WILL ATTEMPT TO REALLOCATE pbyte_buffer to be larger if needed.
 * SIDE EFFECTS: WILL CHANGE THE VALUE OF pbuffer_length
 * SIDE EFFECTS: Everything in prexp_buffer is lost.
 * SIDE EFFECTS: Everything in pbyte_buffer is lost.
 *
 * Difference between 64 and 32 versions is the way the byte size is written to stream.
 */
void writeSexp64(FILE* fout, REXP* prexp_buffer, SEXP obj){
	int size;
	string buffer; //faster to use a string buffer outside of this of fixed length?  Does it matter?
	prexp_buffer->Clear();
	sexpToRexp(prexp_buffer,obj);
	size = prexp_buffer->ByteSize();
	writeVInt64ToFileDescriptor( size , fout);
	buffer.clear();
	prexp_buffer->SerializeToString(&buffer);
	fwrite(buffer.data(), size,1,fout);

	/*
	 * OLDER REWRITE THAT char BUFFERS
	prexp_buffer->Clear();
	sexpToRexp(prexp_buffer, obj);
	uint32_t bs = prexp_buffer->ByteSize();
	if (bs > *pbuffer_size) {
		*pbyte_buffer = (uint8_t *) realloc(*pbyte_buffer, 2*bs);
		*pbuffer_size = 2*bs;
	}
	prexp_buffer->SerializeWithCachedSizesToArray(*pbyte_buffer);
	writeVInt64ToFileDescriptor(bs, fout);
	fwrite(*pbyte_buffer, bs, 1, fout);
	*/

}
/*
 * writeSEXP32
 * Converts an R object (not a list or vector of objects or other container type) and writes it to stream.
 * input: fout file to output bytes
 * input: pbyte_buffer buffer to write serialized rexp object too.
 * input: pbuffer_size max allocated length of byte_buffer
 * input: prexp_buffer rexp object to use for converting obj
 * input: object to be written.
 * Writes the serialized bytes to stream WITH a prepend of byte length.
 * SIDE EFFECTS: WILL ATTEMPT TO REALLOCATE pbyte_buffer to be larger if needed.
 * SIDE EFFECTS: WILL CHANGE THE VALUE OF pbuffer_length
 * SIDE EFFECTS: Everything in prexp_buffer is lost.
 * SIDE EFFECTS: Everything in pbyte_buffer is lost.
 *
 * Difference between 64 and 32 versions is the way the byte size is written to stream.
 */
void writeSexp32(FILE* fout, REXP* prexp_buffer, SEXP obj){
	int size;
	string buffer; //faster to use a string buffer outside of this of fixed length?  Does it matter?
	prexp_buffer->Clear();
	sexpToRexp(prexp_buffer, obj);
	size = prexp_buffer->ByteSize();
	writeUInt32( fout,size);
	buffer.clear();
	prexp_buffer->SerializeToString(&buffer);
	fwrite(buffer.data(), size,1,fout);

}


  
