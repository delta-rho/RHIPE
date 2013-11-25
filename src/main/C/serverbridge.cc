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
#include <string.h>
#include <sys/select.h>

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::io;

#define BUGBUG 0
extern "C" {

SEXP createProcess(SEXP program, SEXP whatclose, SEXP bufsz, SEXP bugbug) {
	int buglevel = INTEGER(bugbug)[0];
	// int errorpipe, fromJ, toJ;
	int pid = fork();
	if (pid == 0) { // child
		fclose(stdin);
		if (INTEGER(whatclose)[0] == 0) {
			int nullend = open("/dev/null", 666);
			dup2(nullend, 2);
		}
		if (INTEGER(whatclose)[1] == 0) {
			int nullend = open("/dev/null", 666);
			dup2(nullend, 1);
		}

		if (buglevel > 1000)
			Rprintf("C call to execl with args %s\n",
					(const char*) CHAR( STRING_ELT(program,0)));

		execl("/bin/sh", "sh", "-c",
				(const char*) CHAR( STRING_ELT(program,0)), (char*) NULL);
		Rprintf("Program faild to run, errno=%d errstr=%s\n", errno,
				strerror(errno));
		_exit(255);
	}

	// SEXP rz =  R_MakeExternalPtr(result, R_NilValue,result);
	// R_RegisterCFinalizerEx(rz, RefObjectFinalizer, (Rboolean)1);
	return (R_NilValue);
}

} //end extern

