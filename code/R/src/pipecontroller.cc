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

using namespace std;
using namespace google::protobuf;
using namespace google::protobuf::io;

SEXP makeErrorCode(int* n,int length){
  SEXP r;
  PROTECT(r = Rf_allocVector(INTSXP,length));
  for(int i=0;i < length;i++){
    INTEGER(r)[i]= n[i];
  }
  UNPROTECT(1);
  return(r);
}

SEXP createProcess(SEXP program,SEXP arguments,SEXP processpipes){
  
  int errorpipe, fromJ, toJ;
  if(mkfifo(STRING_ELT(processpipes,0),S_IRUSR| S_IWUSR)   // writing to child
     | mkfifo(STRING_ELT(processpipes,1),S_IRUSR| S_IWUSR) // reading from child
     | mkfifo(STRING_ELT(processpipes,2),S_IRUSR|S_IWUSR)) // errors from child
    return(makeErrorCode(&errno,1));
  pid = fork();
  if(pid == 0){ // child
    close(stdin);
    char ** m = (char**) malloc(LENGTH(arguments)+1);
    for(int i=0;i< LENGTH(arguments);i++){
      const char *s = CHAR(STRING_ELT(arguments,i));
      m[i] = (char*)malloc(strlen(s)+1);
      strcpy(m[i], s);
    }
    m[LENGTH(arguments)]=0;
    execvp(STRING_ELT(program,0), m);
    // we reach here if program failed. Why bother freeing m?
    fprintf(stderr, "Program faild to run, errno=%d\n",errno);
      _exit(255);
  }
  fromJ = open(STRING_ELT(processpipes,1), O_RDONLY) ;
  errorpipe = open(STRING_ELT(processpipes,2), O_RDONLY) ; 
  char u;
  read(fd_fromworker, &u,1);
  toJ = open(STRING_ELT(processpipes,0),O_WRONLY);
  int r[] = { u, toJ,fromJ,errorpipe};
  return(makeErrorCode(r,4));
}

