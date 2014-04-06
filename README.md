# Introduction #
The following are instructions to get Rhipe up and running on a local machine (as opposed to a cluster) for testing
and development.  The instructions are specific to a Mac although they can be adapted to Linux

## Pre-reqs ##
* Mac/Linux
* Java 1.6+
* Apache Ant - latest
* Apache Maven - latest
* R - latest
* pkg-config - if not installed either:
    * download from http://macpkg.sourceforge.net/
    * build from source: http://cgit.freedesktop.org/pkg-config/
    * Use "homebrew" - http://brew.sh/
        * -> brew install pkg-config
* ssh service running (for Hadoop)
    * On a Mac go to System Preferences -> Sharing and check "Remote Login"

## Install Hadoop ##

* CDH3
http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDHTarballs/3.25.2013/CDH3-Downloadable-Tarballs/CDH3-Downloadable-Tarballs.html

Download Hadoop from Cloudera - you only need Hadoop - and follow the instructions for installing Hadoop in
pseudo-distributed mode located in the tar ball under docs/single_node_setup.html

* In addition:
Edit conf/core-site.xml
Set the storage directory to something meaningful (where you want hadoop data stored on the local directory)  
```xml
    <property>  
        <name>hadoop.tmp.dir</name>  
        <value>[path on local machine]</value>  
    </property>  
```  
If you have already formatted the namenode then change the tmp dir you will need to reformat the namenode.

## Install Google protocol buffers 2.4.1 ##

On Mac use homebrew
On Linux use package manager
- Or -
Download from:
https://code.google.com/p/protobuf/downloads/list
Follow the instructions in the protobuf archive - but probably:
    ./configure
    make
    make install

## Environment ##

**Set the following environment variables:**
```
HADOOP_HOME=<where you unzipped the hadoop tar ball>  
HADOOP_BIN=<hadoo bin dir>  
HADOOP_CONF_DIR=<hadoop conf dir>  
PKG_CONFIG_PATH=<protobuf pkgconfig dir>  
LD_LIBRARY_PATH=<protobuf lib dir>  
RHIPE_HADOOP_TMP_FOLDER=<location in HDFS space for Rhipe to write temporary files, defaults to /tmp if not specified>
```
**Mac/Linux example:**  
    `export HADOOP_HOME=/Users/perk387/Software/hadoop-0.20.2-cdh3u6`    
    `export HADOOP_BIN=$HADOOP_HOME/bin`    
    `export HADOOP_CONF_DIR=$HADOOP_HOME/conf`    
    `export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/Cellar/protobuf241/2.4.1/lib/pkgconfig`    
    `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Cellar/protobuf241/2.4.1/lib`    
    `export RHIPE_HADOOP_TMP_FOLDER=/tmp`

### R Development Environment ###

* Start R
* Run:  
`install.packages("roxygen2")`

## Rhipe ##

Rhipe is built using Ant and Maven.
To clean, compile, build, install and test Rhipe on a fully configured system run:

`ant build-all`

or to skip the R tests run:

`ant clean build r-install`

For other main Ant targets run  
`ant -p`  

## Hadoop Distros ##
* Rhipe has been successfully built and run on plain Hadoop 1.x from Apache and Cloudera CDH3/CDH4.  However Rhipe must 
be built against the distro dependencies that will be used.

* Rhipe is setup to build against CDH3 & CDH4-mr1 by default.  There are maven profiles setup in the POM that build 
against Apache Hadoop 1.x and 2.x however these have not been integrated with the main Ant build and would need to be 
enabled by the user.  
To do this, edit the ant build file, build.xml, copy the ant target _build-hadoop-1, and customize it for the alternate 
maven profiles.

* YARN - Rhipe successfully builds against YARN but has not been tested

## Debug Options ##
### Experimental ###
An option has been added to Java to save any "last.dump.rda" files created by R if an error occurs in the MR job to 
HDFS at /tmp/map-reduce-error.  
To enable this option in R, add 
`options(error=dump.frames)` 
to the beginning of any R code that will be evaluated by Hadoop (mapper, reducer, etc)
The last.dump.rda file will be stored according to job ID under the $RHIPE_HADOOP_TMP_FOLDER/map-reduce-error directory.
For a usage example see the `test-with-errors.R` script in the `inst/tests` directory of the package.
