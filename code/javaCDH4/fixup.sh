#!/bin/sh
PATH=$PATH:/usr/local/mvn/bin
rsync -avP --exclude=RHMR.java ../java/org/godhuli/rhipe/ src/main/java/org/godhuli/rhipe/
echo COMPILING
mvn compile
mvn package
echo COPYING
cp target/Rhipe-cdh4.jar ../R/inst/java/RhipeCDH4.jar


