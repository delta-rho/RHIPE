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

3. *rJava* The R package needs rJava.


Tested on RHEL Linux, though *may* work on Windows


Installation
------------
On every machine

::
	
	R CMD INSTALL Rhipe_VERSION.tar.gz


To load it

::
	
	library(Rhipe)

