#!/bin/bash
# ONLY APPROPRIATE TO RUN THIS FROM AN ARCHIVE CREATED WITH bashRhipeArchive()
# EVERYONE ELSE SHOULD JUST CALL:
# R CMD RhipeMapReduce --slave --silent --vanilla
# OR WRITE A NEW RUNNER SCRIPT.
bin=`dirname "$0"`
RHIPE_BIN=`cd "$bin"; pwd`


#ASSUME THIS IS RAN FROM AN ARCHIVE CREATED BY bashRhipeArchive()
export R_HOME=$RHIPE_BIN/../../..
export R_HOME_DIR=$R_HOME
export R_SHARE_DIR=$R_HOME/library
export R_INCLUDE_DIR=$R_HOME/include
export R_DOC_DIR=$R_HOME/doc
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$R_HOME/lib
chmod -R 777 $R_HOME
. "${R_HOME}/etc${R_ARCH}/ldpaths"
exec $R_HOME/bin/Rcmd $RHIPE_BIN/RhipeMapReduce --slave --silent --vanilla

