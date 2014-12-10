# Introduction #
The following are instructions to build Rhipe and get it up and running on a local machine (as opposed to a cluster) for testing and development.  The instructions are specific to a Mac although they can be adapted to Linux

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
* Google protocol buffers 2.5 (see below)
* Hadoop

## Install Hadoop ##

Hadoop 1 or CDH3 is straightforward to install and setup in pseudo-distributed mode and can be good for testing.  There is nothing in Rhipe preventing it from running on any given Hadoop distro.  But Rhipe does need to be built (compiled) against that particular distro (including Hadoop 1 and CDH3).  See below for more information on building Rhipe for a given distro.

Installing and configuring Hadoop in general is beyond the scope of this readme file.  See the documentation of the distro you wish
to use for this information.

### Basic CDH3 Setup ###

Download Hadoop from Cloudera - you only need Hadoop - and follow the instructions for installing Hadoop in
pseudo-distributed mode located in the tar ball under docs/single_node_setup.html

http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDHTarballs/3.25.2013/CDH3-Downloadable-Tarballs/CDH3-Downloadable-Tarballs.html

* Optional:
Edit conf/core-site.xml
Set the storage directory to something meaningful (where you want hadoop data stored on the local directory)  
```xml
    <property>  
        <name>hadoop.tmp.dir</name>  
        <value>[path on local machine]</value>  
    </property>  
```  
If you have already formatted the namenode then prior to changing the tmp dir you will need to reformat the namenode.

## Install Google protocol buffers 2.5 ##

On Mac use homebrew
On Linux use the package manager
- Or -
Download from:
https://code.google.com/p/protobuf/downloads/list
Follow the instructions in the protobuf archive - but probably:
    ./configure
    make
    make install

## Environment ##

###Set the following environment variables###
```
HADOOP_HOME=<where you unzipped the hadoop tar ball>  
HADOOP_BIN=<hadoo bin dir>  
HADOOP_CONF_DIR=<hadoop conf dir>
HADOOP_LIBS=<hadoop jars see below>
PKG_CONFIG_PATH=<protobuf pkgconfig dir>  
LD_LIBRARY_PATH=<protobuf lib dir>  
RHIPE_HADOOP_TMP_FOLDER=<location in HDFS space for Rhipe to write temporary files, defaults to /tmp if not specified>
```
####HADOOP\_LIBS####

HADOOP\_LIBS lets Rhipe know where to find all the Hadoop jars and their dependencies.  Rhipe adds the jar files from each directory (separated by ":") listed in HADOOP_LIBS non-recursively.  Each directory you wish to be added to the classpath must be explicitly specified.  Each environment is different and your configuration may vary.

**Hadoop 1 (including CDH3):**
`export HADOOP_LIBS=$HADOOP_HOME:$HADOOP_HOME/libs`

**Hadoop 2 (including CDH4 and CDH5):**
Use the command "hadoop classpath" as follows:
     ``export HADOOP_LIBS=`hadoop classpath | tr -d '*'` ``

Hadoop 2 example from classpath command:
```
HADOOP_LIBS=/etc/alternatives/hadoop-conf:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/libexec/../../hadoop/lib/:
/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/libexec/../../hadoop/.//:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/
lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/libexec/../../hadoop-hdfs/lib/:
/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/libexec/../../hadoop-hdfs/.//:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/
lib/hadoop/libexec/../../hadoop-yarn/lib/:/opt/cloudera/parcels/CDH-5.0.0-0.cdh5b2.p0.27/lib/hadoop/libexec/../../hadoop-yarn/.//:
/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/lib/:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/.//
```


**Mac example with CDH3 in psuedo-distributed mode:**  
    `export HADOOP_HOME=/Users/someUser/Software/hadoop-0.20.2-cdh3u6`    
    `export HADOOP_BIN=$HADOOP_HOME/bin`    
    `export HADOOP_CONF_DIR=$HADOOP_HOME/conf`    
    `export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/Cellar/protobuf/2.5.0/lib/pkgconfig`
    `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Cellar/protobuf/2.5.0/lib`
    `export RHIPE_HADOOP_TMP_FOLDER=/tmp`
    `export HADOOP_LIBS=$HADOOP_HOME:$HADOOP_HOME/libs`

### R Development Environment ###

* Start R
* Run:  
`install.packages("roxygen2")`

## Hadoop Distros ##
* Rhipe has been successfully built and run for Apache Hadoop 1.x , Cloudera CDH3, CDH4mr1 and current testing is in processes for CDH5 with YARN.  There are rumors of work being done around EC2.

* Rhipe is setup to build against CDH3 & CDH4-mr1 by default.  There are maven profiles setup in the POM that build against Apache Hadoop 1.x, 2.x,CDH3, CDH4, CDH5, HDP 1,2 & 2.2.   

* YARN - Rhipe successfully builds against YARN but has not been fully tested

## Building Rhipe ##
Rhipe is built using both Ant and Maven.  Maven handles the Java build with the various distro dependencies.  Ant drives the build process including the R packaging, running R commands and testing.

`ant` 

with no targets prints help

to build Rhipe for a specific distro run

`ant build-distro -Dhadoop.version=[hadoop-1,hadoop-2,cdh3,cdh4,cdh5,hdp-1,hdp-2]`

## Debug Options ##
### Experimental ###
An option has been added to Java to save any "last.dump.rda" files created by R if an error occurs in the MR job to 
HDFS at /tmp/map-reduce-error.  
To enable this option in R, add 
`options(error=dump.frames)` 
to the beginning of any R code that will be evaluated by Hadoop (mapper, reducer, etc)
The last.dump.rda file will be stored according to job ID under the $RHIPE_HADOOP_TMP_FOLDER/map-reduce-error directory.
For a usage example see the `test-with-errors.R` script in the `inst/tests` directory of the package.
