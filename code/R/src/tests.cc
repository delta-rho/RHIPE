// #include <iostream>
// #include <rexp.pb.h>
// #include "ream.h"

// using namespace std;

// void doTests(void){
//   char* trial = (char*)malloc(sizeof(char)*100);
//   int rep=1000000,which=0;
  
//   if (trial = getenv("trial"))
//     rep = atoi(trial);
  
//   if (trial = getenv("which"))
//     which = atoi(trial);
  
//   if (!(trial = getenv("rexpress")))
//     strcpy(trial,"list(x=1,y=2,z=runif(125))");
  
//   fprintf(stderr,"Trial=%d Which=%d Express=%s\n",rep,which,trial);
  
//   // free(trial);
  
//   switch(which){
//   case 0:
//     doTest_Serialize2String(trial,rep);
//     break;
//   case 1:
//     doTest_Serialize2Char(trial,rep);
//   case 2:
//     doTest_Serialize2FD(trial,rep);
//   }
// }

// void doTest_Serialize2String(char *msg,const int repeat){
//   //
//   SEXP ans = rexpress(msg);
//   REXP *rexp = new REXP();
//   SEXP result;
//   FILE* FSTDOUT;

//   string *serialdata = new string();

//   if (!(FSTDOUT = freopen(NULL, "wb", stdout))){
//     fprintf(stderr,"ERROR: Could not reopen standard output in binary mode\n");
//     return;
//   }

//   setvbuf(FSTDOUT, 0, _IOFBF,  32*1024);

//   for (int i=0; i<repeat; i++)
//     {
//       rexp->Clear();
//       rexp2message(rexp,ans);   

//       int sz = rexp->ByteSize();
      
//       rexp->SerializeToString(serialdata);
//       // int sz = serialdata->length()
//       fwrite(serialdata->data(),sz,1,FSTDOUT);
//       // result = message2rexp(*rexp);
//     }
//   // cout<< rexp->DebugString();
//   free(rexp);
//   free(serialdata);
// }


// void doTest_Serialize2Char(char *msg,const int repeat){
//   //
//   SEXP ans = rexpress(msg);
//   REXP *rexp = new REXP();
//   SEXP result;
//   FILE* FSTDOUT;

//   int sizes = 8192;
//   unsigned char *serialdata = (unsigned char*)malloc(sizes);

//   if (!(FSTDOUT = freopen(NULL, "wb", stdout))){
//     fprintf(stderr,"ERROR: Could not reopen standard output in binary mode\n");
//     return;
//   }

//   setvbuf(FSTDOUT, 0, _IOFBF,  32*1024);

//   for (int i=0; i<repeat; i++)
//     {
//       rexp->Clear();
//       rexp2message(rexp,ans);   

//       int sz = rexp->ByteSize();
      
//       if (sz>sizes)
// 	{
// 	  sizes=sz+512;
// 	  serialdata = (unsigned char*)realloc(serialdata,sizes);
// 	  if (!serialdata)
// 	    {
// 	      fprintf(stderr,"Could not resize buffer\n");
// 	      return;
// 	    }
// 	}
//       rexp->SerializeWithCachedSizesToArray(serialdata);
//       fwrite(serialdata,sz,1,FSTDOUT);
//       // result = message2rexp(*rexp);
//     }
//   // cout<< rexp->DebugString();
//   free(rexp);
//   free(serialdata);
// }

// void doTest_Serialize2FD(char *msg,const int repeat){
//   //
//   SEXP ans = rexpress(msg);
//   REXP *rexp = new REXP();
//   SEXP result;
//   FILE* FSTDOUT;


//   if (!(FSTDOUT = freopen(NULL, "wb", stdout))){
//     fprintf(stderr,"ERROR: Could not reopen standard output in binary mode\n");
//     return;
//   }
//   int fn  = fileno(FSTDOUT);

//   for (int i=0; i<repeat; i++)
//     {
//       rexp->Clear();
//       rexp2message(rexp,ans);   

//       int sz = rexp->ByteSize();
      
//       rexp->SerializeToFileDescriptor(fn);
//     }
//   free(rexp);
// }
