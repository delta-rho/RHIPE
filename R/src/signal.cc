#include "ream.h"
#include <netinet/in.h>
#include <google/protobuf/stubs/common.h>
#include <unistd.h>
#include <time.h>

using namespace std;
const int i___ = 1;
#define is_bigendian() ( (*(char*)&i___) == 0 )


#ifdef USETIMER
SEXP TIMER_R_tryEval(SEXP ex, SEXP en, int *err){
  struct timeval tms;
  long int bstart, bend;
  gettimeofday(&tms,NULL);
  bstart = tms.tv_sec*1000000 + tms.tv_usec;
  SEXP f = R_tryEval(ex,en,err);
  gettimeofday(&tms,NULL);
  bend = tms.tv_sec*1000000 + tms.tv_usec;
  time_in_reval += (bend - bstart);
  return(f);
}

SEXP TIMER_REDUCE_R_tryEval(SEXP ex, SEXP en, int *err){
  struct timeval tms;
  long int bstart, bend;
  gettimeofday(&tms,NULL);
  bstart = tms.tv_sec*1000000 + tms.tv_usec;
  SEXP f = R_tryEval(ex,en,err);
  gettimeofday(&tms,NULL);
  bend = tms.tv_sec*1000000 + tms.tv_usec;
  time_in_reduce_reval += (bend - bstart);
  return(f);
}
#endif








/*****************************
 *
 * Signal Handlers
 * blindly taken from Stevens
 *
 ***************************/
void sig_chld(int signo)
{
        pid_t   pid;
        int             stat;
        while ( (pid = waitpid(-1, &stat, WNOHANG)) > 0);
        return;
}

Sigfunc *signal_ours(int signo, Sigfunc *func)
{
  struct sigaction      act, oact;
  act.sa_handler = func;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  if (signo == SIGALRM) {
#ifdef  SA_INTERRUPT
    act.sa_flags |= SA_INTERRUPT;       /* SunOS 4.x */
#endif
  } else {
#ifdef  SA_RESTART
    act.sa_flags |= SA_RESTART;         /* SVR4, 44BSD */
#endif
  }
  if (sigaction(signo, &act, &oact) < 0)
    return(SIG_ERR);
  return(oact.sa_handler);
}
Sigfunc * Signal(int signo, Sigfunc *func)      /* for our signal() function */
{
  Sigfunc *sigfunc;
  if ( (sigfunc = signal_ours(signo, func)) == SIG_ERR)
    fprintf(stderr,"signal error");
  return(sigfunc);
}

void sigHandler(int i) {
  pid_t pid;
  int  stat;
  // if (i==SIGTERM || i==SIGHUP  || i==SIGINT || i==SIGQUIT)
  //   __active__=0;
  while ( (pid = waitpid(-1, &stat, WNOHANG)) > 0);
  // merror("sighandler: %d\n", i);
  // return;
  sleep(10);
  LOGG(9,"SIGHANDLE %d\n",i);
  exit(0);
  // return;
}





