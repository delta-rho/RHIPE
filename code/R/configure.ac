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





# Checks for programs.
AC_PROG_CC

# Checks for libraries.


AC_CONFIG_FILES([src/Makevars])
AC_OUTPUT
