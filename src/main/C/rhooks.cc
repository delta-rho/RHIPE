#include "ream.h"
#include <vector>
#include <iostream>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <unistd.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdio.h>
#include <errno.h>

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::io;

// static REXP *rexp_container = new REXP();
extern uintptr_t R_CStackLimit;

void CaptureLogInLibrary(LogLevel level, const char* filename, int line,
		const string& message) {
  static const char* pb_log_level[] = { "LOGLEVEL_INFO", "LOGLEVEL_WARNING",
			"LOGLEVEL_ERROR", "LOGLEVEL_FATAL", "LOGLEVEL_DFATAL" };
	Rf_error("PB ERROR[%s](%s:%d) %s", pb_log_level[level], filename, line,
			message.c_str());
}

extern "C" {
void R_init_Rhipe(DllInfo *info) {
	R_CStackLimit = (uintptr_t) -1;
	google::protobuf::SetLogHandler(&CaptureLogInLibrary);
}

//Neither of these are thread safe...
SEXP serializeUsingPB(SEXP robj) {
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

SEXP unserializeUsingPB(SEXP robj) {
	if (TYPEOF(robj) != RAWSXP)
		Rf_error("Must pass a raw vector");
	SEXP ans = R_NilValue;
	// REXP *rexp = new REXP();
	REXP *rexp_container = new REXP();
	//     rexp_container->Clear();
	CodedInputStream cds(RAW(robj), LENGTH(robj));
	// rexp_container->ParseFromArray(RAW(robj),LENGTH(robj));
	cds.SetTotalBytesLimit(512*1024 * 1024, 250*1024 * 1024);
	rexp_container->ParseFromCodedStream(&cds);
	PROTECT(ans = rexpToSexp(*rexp_container));
	UNPROTECT(1);
	delete (rexp_container);
	return (ans);
}

void readVInt_from_R(const unsigned char *data, int *vint) {
	/**
	 * Given the RAW vector in data
	 * vint[0] is the number of bytes it takes up
	 * vint[1] is the actual value
	 **/

	int8_t firstByte = (int8_t) data[0];
	uint32_t len = decodeVIntSize(firstByte);
	vint[0] = len;
	if (len == 1) {
		vint[1] = (int8_t) firstByte;
		return;
	}
	int counter = 1;
	int64_t i = 0;
	uint32_t idx;
	for (idx = 0; idx < len - 1; idx++) {
		int8_t b = (int8_t) data[counter];
		counter++;
		i = i << 8;
		i = i | (b & 0xFF);
	}
	vint[1] = (int32_t) (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
}

SEXP returnBytesForVIntx(SEXP n0) {
	int i = INTEGER(n0)[0];
	SEXP r;
	int nm = getVIntSize(i);
	PROTECT(r = Rf_allocVector(RAWSXP,nm));
	char x;
	if (i >= -112 && i <= 127) {
		x = (char) i;
		/* writen(fd,&x,sizeof(x)); */
		RAW(r)[0] = x;
		UNPROTECT(1);
		return (r);
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
	x = (char) len;
	RAW(r)[0] = x;
	len = (len < -120) ? -(len + 120) : -(len + 112);
	int idx, counter;
	for (idx = len, counter = 1; idx != 0; idx--, counter++) {
		int shiftbits = (idx - 1) * 8;
		long mask = 0xFFL << shiftbits;
		x = (char) ((i & mask) >> shiftbits);
		RAW(r)[counter] = x;
	}
	UNPROTECT(1);
	return (r);
}

static SEXP NewList(void) {
	SEXP s = Rf_cons(R_NilValue, R_NilValue);
	SETCAR(s, s);
	return s;
}

/* Add a new element at the end of a stretchy list */

static SEXP GrowList(SEXP l, SEXP s) {
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

SEXP kk_(char *d, int n) {
	SEXP k;
	REXP *rexp_container = new REXP();

	//     rexp_container->Clear();
	rexp_container->ParseFromArray(d, n);
	PROTECT(k = rexpToSexp(*rexp_container));
	UNPROTECT(1);
	delete (rexp_container);
	//     Rf_PrintValue(k);
	return (k);

}
/**
 library(Rhipe)
 d=rhreadBin("/tmp/smry")
 ##where tmpsmr is say a part-r-0000 file
 ##from connection summaryzes
 ## u=list(); u=append(u,d) - crashes

 n=10
 z=rhsz(d[28231])
 f=rhuz(z)
 e=lapply(1:100000,function(r) rhuz(z))
 u=list(); u=append(u,e)


 **/
SEXP returnListOfKV(SEXP raw, SEXP numread) {

	if (TYPEOF(raw) != RAWSXP) {
		return (R_NilValue);
	}
	SEXP rval;
	int num = INTEGER(numread)[0];
	char *rawdata = (char*) RAW(raw);
	int r;
	char *x = rawdata;
	PROTECT(rval = Rf_allocVector(VECSXP, num));
	for (int i = 0; i < num; i++) {
		SEXP k, v, l;
		PROTECT(l = Rf_allocVector(VECSXP, 2));
		r = reverseUInt(*((int*) x));
		x += 4;

		//       PROTECT(k= kk_(x,r));
		PROTECT(k = Rf_allocVector(RAWSXP, r));
		memcpy(RAW(k), x, r);
		x += r;
		//       SET_VECTOR_ELT(rval, 2*i,k);
		//       Rf_PrintValue(k);
		SET_VECTOR_ELT(l, 0, k);
		UNPROTECT(1);

		r = reverseUInt(*((int*) x));
		x += 4;
		//       PROTECT(v= kk_(x,r));
		PROTECT(v = Rf_allocVector(RAWSXP, r));
		memcpy(RAW(v), x, r);

		x += r;
		//       SET_VECTOR_ELT(rval,2*i+1,v);
		SET_VECTOR_ELT(l, 1, v);
		UNPROTECT(1);
		SET_VECTOR_ELT(rval, i, l);
		UNPROTECT(1);
	}
	UNPROTECT(1);
	return (rval);
}

//   SEXP returnListOfKV(SEXP raw,SEXP numread){

//    //  SEXP raw ;
// //     PROTECT(raw = Rf_duplicate(raw0));
//     SEXP rval,rv,l;
//     int num = INTEGER(numread)[0];
//     char *rawdata = (char*)RAW(raw);
//     int r;
//     char *x = rawdata;
//     //yeah, why a grow list when i know numread?
//     PROTECT(rv = NewList());
//     while(true){
//       SEXP k,v;
//       r = reverseUInt(*((int*) x  ));
//       if(r<0) break;
//       PROTECT(l = Rf_allocVector(VECSXP,2));
//       x+=4;
//       PROTECT(k= kk_(x,r));
//       x+= r;
//       SET_VECTOR_ELT( l, 0, k);

//       r = reverseUInt(*((int*) x));
//       x+=4;
//       PROTECT(v= kk_(x,r));
//       SET_VECTOR_ELT( l, 1, v);
//       x+=r;

//       UNPROTECT(3);
//       rv = GrowList(rv, l);
//     }

//      rv = CDR(rv);

//      PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
//      for (int n = 0 ; n < LENGTH(rval) ; n++, rv = CDR(rv))
//        SET_VECTOR_ELT(rval, n, CAR(rv));
//      UNPROTECT(2);

//     return(rval);
//   }


SEXP readBinaryFile(SEXP filename0, SEXP max0, SEXP bf, SEXP vb) {
	SEXP rv = R_NilValue;
	int max = INTEGER(max0)[0];
	char *filename = (char*) CHAR(STRING_ELT( filename0 , 0));

	FILE *fp = fopen(filename, "rb");
	if (!fp)
		Rf_error("Could not open filename, error=%d", errno);

	int buffsize = INTEGER(bf)[0];
	if (buffsize == 0)
		buffsize = BUFSIZ;
	int w = 2 * 1024 * 1024;
	if (LOGICAL(vb)[0])
		Rf_warning("Using read buffer size:%d, data store:%d", buffsize, w);
	char* buffer = (char*) malloc(buffsize * sizeof(char));
	setbuffer(fp, buffer, buffsize);

	int32_t kvlength;
	int32_t count = 0;
	void * kvhold = (void*) malloc(w);
	//     REXP *rexp = new REXP();
	SEXP k, v, l;

	PROTECT( rv = NewList());

	while (true) {
		PROTECT(l = Rf_allocVector(VECSXP,2));
		// read key
		fread(&kvlength, sizeof(int32_t), 1, fp);
		kvlength = reverseUInt(kvlength);
		if (kvlength > w) {
			kvhold = realloc(kvhold, kvlength + 2048);
			w = kvlength + 2048;
		}
		int d_;
		if ((d_ = fread(kvhold, kvlength, 1, fp)) <= 0) {
			if (d_ < 0)
				Rf_warning("There was an issue reading this file:%d", errno);
			UNPROTECT(1);
			break;
		}
		//       rexp->Clear();
		//       rexp->ParseFromArray( kvhold, kvlength );
		//       PROTECT(k = message2rexp(*rexp));

		PROTECT(k = Rf_allocVector(RAWSXP,kvlength));
		memcpy(RAW(k), kvhold, kvlength);
		SET_VECTOR_ELT(l, 0, k);
		// Rf_PrintValue(k);
		// read value
		fread(&kvlength, sizeof(int32_t), 1, fp);
		kvlength = reverseUInt(kvlength);

		if (kvlength > w) {
			kvhold = realloc(kvhold, kvlength + 2048);
			w = kvlength + 2048;
		}
		if ((d_ = fread(kvhold, kvlength, 1, fp)) <= 0) {
			if (d_ < 0)
				Rf_warning("There was an issue reading this file:%d", errno);
			UNPROTECT(2);
			break;
		}
		// //       rexp->Clear();
		//       rexp->ParseFromArray( kvhold, kvlength );
		//       PROTECT(v = message2rexp(*rexp));
		//       Rf_PrintValue(v);


		PROTECT(v = Rf_allocVector(RAWSXP,kvlength));
		memcpy(RAW(v), kvhold, kvlength);
		SET_VECTOR_ELT(l, 1, v);
		UNPROTECT(3);
		rv = GrowList(rv, l);

		count++;
		// R_ProcessEvents();
		R_CheckUserInterrupt();

		if (max >= 0 && count >= max)
			break;
	}

	rv = CDR(rv);
	SEXP rval;
	PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
	for (int n = 0; n < LENGTH(rval); n++, rv = CDR(rv)) {
		//       SEXP r;
		//       PROTECT(r = Rf_duplicate(CAR(rv)));
		SET_VECTOR_ELT(rval, n, CAR(rv));
		//       UNPROTECT(1);
	}
	UNPROTECT(2);

	//     delete(rexp);
	free(kvhold);
	free(buffer);
	fclose(fp);
	return (rval);
}




/*
 * writeKeyValues64
 * This was intended to be just a reuse of writeKeyValues32.
 * However the format that comes from Java writes out the byte lengths as signed 64 bits.
 * Where as the writeKeyValues32 does not...
 * Arguments otherwise the same
 */
void writeKeyValues64(FILE* fout, SEXP vkeyvalues, uint32_t buffer_size){
	REXP *rexp_container = new REXP();
	uint8_t *buffer = (uint8_t*) malloc(buffer_size);
	for (int i = 0; i < LENGTH(vkeyvalues); i++) {
		SEXP a = VECTOR_ELT(vkeyvalues, i);
		SEXP k = VECTOR_ELT(a, 0);
		SEXP v = VECTOR_ELT(a, 1);
		writeSexp64(fout, rexp_container, k);
		writeSexp64(fout, rexp_container, v);
	}
	delete (rexp_container);
	free(buffer);
}
void writeKeyValues32(FILE* fout, SEXP vkeyvalues, uint32_t buffer_size){
	REXP *rexp_container = new REXP();
	uint8_t *buffer = (uint8_t*) malloc(buffer_size);
	for (int i = 0; i < LENGTH(vkeyvalues); i++) {
		SEXP a = VECTOR_ELT(vkeyvalues, i);
		SEXP k = VECTOR_ELT(a, 0);
		SEXP v = VECTOR_ELT(a, 1);
		writeSexp32(fout, rexp_container, k);
		writeSexp32(fout, rexp_container, v);
	}
	delete (rexp_container);
	free(buffer);
}

/*
 * writeBinaryFile
 * Saptarshi's original C code with the logic torn out to be generalized.
 * It writes a 32 bit length before each Key,Value because that is what Saptarshi's code did.
 */
SEXP writeBinaryFile(SEXP vkeyvalues, SEXP sfilename, SEXP nbuffer_size) {
	char *filename = (char*) CHAR(STRING_ELT( sfilename , 0));
	uint32_t buffer_size = INTEGER(nbuffer_size)[0];
	FILE *fp = fopen(filename, "w");
	setvbuf(fp, NULL, _IOFBF, 0);
	writeKeyValues32(fp,vkeyvalues,buffer_size);
	fclose(fp);
	return (R_NilValue);
}


/*
 * writeMapUnitTestInput
 * Based on writeBinaryFile
 * Writes a binary file that should be a good input to a RhipeMapReduce.
 * Appropriate environment variables must be set to get RhipeMapReduce to work.
 * Basically the major difference is that it writes two bytes to the front and back that
 * are byte codes for setup and cleanup
 * author: Jeremiah Rounds


SEXP writeUnitTestMapInputFile(SEXP vkeyvalues, SEXP sfilename){
	char *filename = (char*) CHAR(STRING_ELT( sfilename , 0));
	uint32_t buffer_size = 4000; //INTEGER(nbuffer_size)[0], m = buffer_size;
	FILE *fp = fopen(filename, "w");
	setvbuf(fp, NULL, _IOFBF, 0);
	writeVInt64ToFileDescriptor(EVAL_SETUP_MAP,fp);
	writeKeyValues64(fp,vkeyvalues,buffer_size);
	writeVInt64ToFileDescriptor(EVAL_CLEANUP_MAP,fp);
	fclose(fp);
	return (R_NilValue);

}
*/
SEXP readSQFromPipe(SEXP jcmd, SEXP buf, SEXP verb) {
	FILE* pipe = popen((char*) CHAR(STRING_ELT( jcmd , 0)), "r");
	if (!pipe) {
		Rf_error("Could not run java process: %s",
				(char*) CHAR(STRING_ELT( jcmd , 0)));
		return (R_NilValue);
	}
	// printf("Reading input from %s\n", (char*)CHAR(STRING_ELT( jcmd , 0)));
	// FILE *pipe = fopen((char*)CHAR(STRING_ELT( jcmd , 0)), "r");
	int a = INTEGER(verb)[0];
	uint32_t n0;
	// FileInputStream *fis = new FileInputStream(fileno(pipe), INTEGER(buf)[0]);
	// CodedInputStream* cis = new CodedInputStream(fis);
	// cis->SetTotalBytesLimit(375*1024*1024,375*1024*1024);


	SEXP rv;
	uint32_t countsum = 0;
	PROTECT(rv = NewList());
	uint32_t co = 0;
	while (true) {
		SEXP l, k, v;
		if (a) {
			printf("reading %d key,value pair: ", co);
		}
		// if(!cis->ReadVarint32(&n0)) break;
		n0 = readVInt64FromFileDescriptor(pipe);
		if (n0 == 0)
			break;
		PROTECT(l = Rf_allocVector(VECSXP,2));
		if (a) {
			printf("\tkey: %d,", n0);
			fflush(NULL);
		}
		PROTECT(k = Rf_allocVector(RAWSXP,n0));
		// cis->ReadRaw(RAW(k),n0);
		fread(RAW(k), n0, 1, pipe);
		SET_VECTOR_ELT(l, 0, k);
		countsum += n0;

		// cis->ReadVarint32(&n0);
		n0 = readVInt64FromFileDescriptor(pipe);

		PROTECT(v = Rf_allocVector(RAWSXP,n0));
		if (a) {
			printf("value: %d ", n0);
			fflush(NULL);
		}

		// cis->ReadRaw(RAW(v),n0);
		fread(RAW(v), n0, 1, pipe);

		if (a) {
			printf("DONE\n");
		}

		SET_VECTOR_ELT(l, 1, v);
		countsum += n0;
		rv = GrowList(rv, l);
		UNPROTECT(3);
		co++;
	}
	//http://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c
	pclose(pipe);
	// delete cis;
	// delete fis;
	if (countsum < 12 * 1024)
		printf("About to unserialize %.2f kb, please wait\n",
				((double) countsum) / (1024));
	else
		printf("About to unserialize %.2f mb, please wait\n",
				((double) countsum) / (1024 * 1024));

	rv = CDR(rv);
	SEXP rval;
	PROTECT(rval = Rf_allocVector(VECSXP, Rf_length(rv)));
	for (int n = 0; n < LENGTH(rval); n++, rv = CDR(rv)) {
		SET_VECTOR_ELT(rval, n, CAR(rv));
	}
	UNPROTECT(2);
	return (rval);
}
  SEXP createTempDir(SEXP inDir){
     char *dir = ( char*) CHAR(STRING_ELT( inDir , 0));
     char *newdir = mkdtemp(dir);
     if(!newdir) Rf_error("RHIPE: There was an error creating a temporary folder in %s: (%s)",dir,strerror(errno));
     SEXP s = R_NilValue,y=R_NilValue;
     PROTECT(s = Rf_allocVector(STRSXP,1));
     PROTECT(y = Rf_mkChar(newdir));
     SET_STRING_ELT(s,0,y);
     UNPROTECT(2);
     return(s);
  }

#include "md5.h"

  SEXP md5(SEXP aRaw,SEXP len){
    md5_state_t state;
    md5_byte_t digest[16];
    md5_init(&state);
    md5_append(&state, (const md5_byte_t *)RAW(aRaw), INTEGER(len)[0]);
    md5_finish(&state, digest);
    char hex_output[16*2 + 1];
    for (int di = 0; di < 16; ++di)
      sprintf(hex_output + di * 2, "%02x", digest[di]);
    SEXP s = R_NilValue,y=R_NilValue;
    PROTECT(s = Rf_allocVector(STRSXP,1));
    PROTECT(y = Rf_mkChar(hex_output));
    SET_STRING_ELT(s,0,y);
    UNPROTECT(2);
    return(s);
  }
}

