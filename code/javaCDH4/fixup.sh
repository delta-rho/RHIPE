#!/bin/sh
cp -r ../java/org/godhuli/rhipe/!(RHMR.java) src/main/java/org/godhuli/rhipe/
mvn compile
mvn package
cp target/Rhipe-cdh4.jar ../R/inst/java/RhipeCDH4.jar


