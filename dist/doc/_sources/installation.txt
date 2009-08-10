Setting up RHIPE
================
.. highlight:: r
   :linenothreshold: 5

Requirements
------------

RHIPE requires Java (JDK 1.6, checked only with Sun's Java), and R.
Currently, RHIPE only works on Linux, though one day it might also
work on Windows too.

Installation
------------


1. Install Java and R


2. Install rJava, Simon Urbanek's R package to allow R to call Java libraries.

3. Download the current version of RHIPE, untar and run

::

	make R

This will build and install the R RHIPE package. There is a JAR file
present in the ``lib/`` folder, but should you wish to build the RHIPE
JAR file, set the environment variables ``HADOOP`` should point to the
Hadoop distribution folder. To build the JAR file, run 

::

	make java

To build the docs, install `sphynx <http://sphinx.pocoo.org/>`_

4. Given that Hadoop is running, typing 

::

	[bash] R
	>> library(rhipe)

should get you started.  
