Setting up RHIPE
================
.. highlight:: r
   :linenothreshold: 5

Requirements
------------

1. *Protobuffers*

   RHIPE uses Google's Protobuf library for serialization. This(the C/C++
   libraries) must be installed on *all* machines (master/workers). Get
   Protobuffers from http://code.google.com/p/protobuf/. RHIPE already has the
   protobuf jar file inside it.

   Non Standard Locations
		If installing protobuf to a non standard location, update the
		PKG_CONFIG_PATH variable, e.g 
  		::

			export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$CUSTROOT/lib/pkgconfig/

2. *R* , tested on 2.8




Tested on RHEL Linux, Mac OS 10.5.5 (Leopard).
Does not work on Snow Leopard


Installation
------------
Rhipe requires the following environment variables 

::

	HADOOP=location of Hadoop installation
	HADOOP_LIB=location of lib jar files included with Hadoop, usually
	$HADOOP/lib
	HADOOP_CONF_DIR=location of Hadoop conf folder, (usually $HADOOP/conf)


On every machine

::
	

       R CMD INSTALL Rhipe_VERSION.tar.gz



To load it

::
	
	library(Rhipe)

