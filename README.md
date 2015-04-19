

# RHIPE: R and Hadoop Integrated Programming Environment #

RHIPE is an R package that provides a way to use Hadoop from R.  It can be used on its own or as part of the [Tessera](tessera.io) environment.

## Installation ##

Installation of RHIPE requires a working Hadoop cluster and several prerequisites.  A comprehensive installation guide, as well as other options to get an environment set up (Vagrant, Amazon Web Services, etc.) are discussed [here](http://tessera.io/#quickstart).

If you are interested in installing and using RHIPE, please see the link just provided.  The remainder of this README will focus on developers who want to build RHIPE from source or contribute to RHIPE development.

## Hadoop Support ##

Our current development efforts are focused on Hadoop 2 (YARN), although code that works with earlier versions of Hadoop is also available in this repository.

### Hadoop 2 ###

The `master` branch of this repository is focused on Hadoop 2 development, and contains code to build RHIPE v0.75.x.  As some aspects of YARN that we have addressed are not backward compatible, packages built from this branch will not work with Hadoop 1.

### Hadoop 1 ###

The `v0.74` branch of this repository is for Hadoop 1.

### Hadoop Distributions ###

There are several Hadoop distributions avaialble.  RHIPE has been successfully built and run for Apache Hadoop 1.x , Cloudera CDH3, CDH4mr1, and CDH5mr2.  There are maven profiles setup in the POM that build against Apache Hadoop 1.x, 2.x,CDH3, CDH4, CDH5, HDP 1,2 & 2.2.

## Building Rhipe ##

Probably the easiest way to build RHIPE is to provision a [Vagrant](https://github.com/tesseradata/install-vagrant) machine that has all the prerequisites configured.  Another option is to set up a local pseudo-distributed Hadoop cluster, for example see [here](https://github.com/hafen/RHIPE/blob/master/cdh5-on-mac.md).

If you set up your own machine, you will need to make sure the following dependencies are met, beyond Hadoop:

* Java 1.6+
* Apache Ant - latest
* Apache Maven - latest
* R - latest
* R `rJava`, `testthat`, and `roxygen2` packages
* Google protocol buffers (v2.5 for Hadoop 2, else v2.4.1)
* pkg-config - if not installed either:
    * download from http://macpkg.sourceforge.net/
    * build from source: http://cgit.freedesktop.org/pkg-config/
    * Use "homebrew" - http://brew.sh/
        * -> brew install pkg-config

Rhipe is built using both Ant and Maven.  Maven handles the Java build with the various distro dependencies.  Ant drives the build process including the R packaging, running R commands and testing.

```
ant
```

with no targets prints help.

To build Rhipe for a specific distro run

```
ant build-distro -Dhadoop.version=[hadoop-1,hadoop-2,cdh3,cdh4,cdh5,hdp-1,hdp-2]
```
