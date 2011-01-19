# Process this file with autoconf to produce a configure script.
AC_INIT(Rhipe, 0.5, sguha@purdue.edu)


# find R home and set CC/CFLAGS
: ${R_HOME=`R RHOME`}
if test -z "${R_HOME}"; then
  echo "could not determine R_HOME"
  exit 1
fi
RBIN="${R_HOME}/bin/R"
CC=`"${RBIN}" CMD config CC`;
CFLAGS=`"${RBIN}" CMD config CFLAGS`
LIBS="${PKG_LIBS}"
AC_SUBST(R_HOME)

RLD=`"${RBIN}" CMD config --ldflags 2>/dev/null`
has_R_shlib=no
if test -n "$RLD"; then
   has_R_shlib=yes
fi

AC_MSG_CHECKING([Java support in R])
R_JAVA_HOME=`"${RBIN}" CMD config JAVA_HOME`
: ${JAVA_HOME="${R_JAVA_HOME}"}
if test -z "${JAVA_HOME}"; then
   AC_MSG_ERROR([absent
R was configured without Java support. Please run
R CMD javareconf
as root to add Java support to R.
If you don't have root privileges, run
R CMD javareconf -e
to set all Java-related variables and then install rJava.
(works with R 2.5.0 only)])
fi

: ${JAR=`"${RBIN}" CMD config JAR|sed 's/ERROR:.*//'`}
: ${JAVA=`"${RBIN}" CMD config JAVA|sed 's/ERROR:.*//'`}
: ${JAVAC=`"${RBIN}" CMD config JAVAC|sed 's/ERROR:.*//'`}
: ${JAVAH=`"${RBIN}" CMD config JAVAH|sed 's/ERROR:.*//'`}
: ${JAVA_CPPFLAGS=`"${RBIN}" CMD config JAVA_CPPFLAGS|sed 's/ERROR:.*//'`}
: ${JAVA_LIBS=`"${RBIN}" CMD config JAVA_LIBS|sed 's/ERROR:.*//'`}

AC_MSG_RESULT([present:
interpreter : '${JAVA}'
archiver    : '${JAR}'
compiler    : '${JAVAC}'
header prep.: '${JAVAH}'
cpp flags   : '${JAVA_CPPFLAGS}'
java libs   : '${JAVA_LIBS}'])

java_error='One or more Java configuration variables are not set.'
if test -z "${JAVA}"; then
   java_error='Java interpreter is missing or not registered in R'
fi
if test -z "${JAVAC}"; then
   java_error='Java Development Kit (JDK) is missing or not registered in R'
fi

have_all_flags=no
if test -n "${JAVA}" && test -n "${JAVAC}" && test -n "${JAVAH}" && \
   test -n "${JAVA_CPPFLAGS}" && test -n "${JAVA_LIBS}" && test -n "${JAR}"; then
  have_all_flags=yes;
fi
if test "${have_all_flags}" = no; then
  AC_MSG_ERROR([${java_error}
Make sure R is configured with full Java support (including JDK). Run
R CMD javareconf
as root to add Java support to R.

If you don't have root privileges, run
R CMD javareconf -e
to set all Java-related variables and then install rJava.
])
fi

if test `echo foo | sed -e 's:foo:bar:'` = bar; then
   JAVA_CPPFLAGS0=`echo ${JAVA_CPPFLAGS} | sed -e 's:$(JAVA_HOME):'${JAVA_HOME}':g'`
   JAVA_LIBS0=`echo ${JAVA_LIBS} | sed -e 's:$(JAVA_HOME):'${JAVA_HOME}':g'`
   JAVA_LD_LIBRARY_PATH0=`echo ${JAVA_LD_LIBRARY_PATH} | sed -e 's:$(JAVA_HOME):'${JAVA_HOME}':g'`
else
   AC_MSG_WARN([sed is not working properly - the configuration may fail])
   JAVA_CPPFLAGS0="${JAVA_CPPFLAGS}"
   JAVA_LIBS0="${JAVA_LIBS}"
   JAVA_LD_LIBRARY_PATH0="${JAVA_LD_LIBRARY_PATH}"
fi

LIBS="${LIBS} ${JAVA_LIBS0}"
CFLAGS="${CFLAGS} ${JAVA_CPPFLAGS0}"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${JAVA_LD_LIBRARY_PATH0}"

AC_MSG_CHECKING([whether JNI programs can be compiled])
AC_LINK_IFELSE([
#include <jni.h>
int main(void) {
    jobject o;
    JNI_CreateJavaVM(0, 0, 0);
    return 0;
}
	],[AC_MSG_RESULT(yes)],
	[AC_MSG_ERROR([Cannot compile a simple JNI program. See config.log for details.

Make sure you have Java Development Kit installed and correctly registered in R.
If in doubt, re-run "R CMD javareconf" as root.
])])

AC_MSG_CHECKING([JNI data types])
AC_TRY_RUN(
	[
#include <jni.h>
int main(void) {
  return (sizeof(int)==sizeof(jint) && sizeof(long)==sizeof(long) && sizeof(jbyte)==sizeof(char) && sizeof(jshort)==sizeof(short) && sizeof(jfloat)==sizeof(float) && sizeof(jdouble)==sizeof(double))?0:1;
}
	],
	[AC_MSG_RESULT([ok])],
	[AC_MSG_ERROR([One or more JNI types differ from the corresponding native type. You may need to use non-standard compiler flags or a different compiler in order to fix this.])],
	[AC_MSG_RESULT([don't know (cross-compiling)])])
AC_SUBST(JAVA_LIBS)
AC_SUBST(JAVA_CPPFLAGS)
AC_SUBST(JAVA_HOME)
AC_SUBST(JAVA)
AC_SUBST(JAVAC)
AC_SUBST(JAVAH)

# Checks for programs.
AC_PROG_CC

# Checks for libraries.


AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
