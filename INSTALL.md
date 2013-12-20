# Introduction #
The following are instructions to get Rhipe up and running on a local machine (as opposed to a cluster) for testing
and development.  The instructions are specific to a Mac although they can be adapted to Linux

## Pre-reqs ##
* Mac/Linux
* Java 1.6+
* Apache Ant - latest
* Apache Maven - latest
* R - latest
* pkg-config - if not installed either
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

    <property>
        <name>hadoop.tmp.dir</name>
        <value>[path on local machine]</value>
    </property>

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

Set the following environment variables:

`HADOOP_HOME=<where you unzipped the hadoop tar ball>`  
`HADOOP_BIN=<hadoo bin dir>`  
`HADOOP_CONF_DIR=<hadoop conf dir>`  
`PKG_CONFIG_PATH=<protobuf pkgconfig dir>`  
`LD_LIBRARY_PATH=<protobuf lib dir>`  

**Mac/Linux example:**  
    export HADOOP_HOME=/Users/perk387/Software/hadoop-0.20.2-cdh3u6  
    export HADOOP_BIN=$HADOOP_HOME/bin  
    export HADOOP_CONF_DIR=$HADOOP_HOME/conf  
    export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/Cellar/protobuf241/2.4.1/lib/pkgconfig  
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/Cellar/protobuf241/2.4.1/lib  

### R Development Environment ###

* Start R
* Run:  
`install.packages("roxygen2")`

## Rhipe ##

Rhipe is built using Ant and Maven.
To clean, compile, build, install and test Rhipe on a fully configure system run:

`ant build-all`

For other main Ant targets run  
`ant -p`  

