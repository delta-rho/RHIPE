#!/bin/sh
PATH=$PATH:/usr/local/mvn/bin
rsync -avP --exclude=../java/org/godhuli/rhipe/RHMR.java ../java/org/godhuli/rhipe/ src/main/java/org/godhuli/rhipe/
mvn compile
mvn package
cp target/Rhipe-cdh4.jar ../R/inst/java/RhipeCDH4.jar


