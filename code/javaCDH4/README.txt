# Currently, the only source file that needs to be changed to work with CDH4 is RHMR.java.  In this directory is that one file and the pom for building.

# To build:
# (working directory is javaCDH4)

# first, temporarily copy all .java files except RHMR.java over to the src/main/java/org/godhuli/rhipe directory

mvn compile
mvn package

cp target/Rhipe-cdh4.jar ../R/inst/java/RhipeCDH4.jar
