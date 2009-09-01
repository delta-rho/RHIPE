Using RHIPE on EC2
==================

.. highlight:: sh
   :linenothreshold: 5


Introduction
------------

We have release two AMIs(32 and 64bit). Both are based on Fedora 8 and have
Hadoop 0.19.1,R 2.8 and RIPE (latest) installed. `s3sync <http://s3sync.net/wiki>`_ is also present.

32 bit
    ami-4b678122 
64 bit
    ami-9f7492f6 


The following describes the usage of the EC2 scripts.

Usage
-----
* Get an Amazon EC2 account and confirm the ability to start and instance from the command line (using ec2-tools).
* Unzip the rhipe-ec2 distribution (see the downloads page)
* OPTIONS

In ``bin/hadoop-ec2-env.sh`` template there are several options:

AWS_ACCOUNT_ID
    fill this from the Amazon Account Identifiers 
AWS_ACCESS_KEY_ID
    same as above 
AWS_SECRET_ACCESS_KEY
    same as above 
RSOPTS
    options to Rserve, default: 
::

	-max-nsize=1G --max-ppsize=100000 --RS-port 8888 

R_USER_FILE
    a URL to an R script. This file is executed on machine boot up. Useful to install R packages. Read ``bin/hadoop-ec2-env.sh.template`` for details. 

INSTANCE_TYPE
    choose the Amazon machine instance type. For details, go to
    `<http://aws.amazon.com/ec2/instance-types/>`_
  
* Save the file as ``bin/hadoop-ec2-env.sh``
Some launch commands
--------------------

* launch

::

	bin/hadoop-ec2 launch-cluster clustername number-of-workers


Replace clustername with the name of the cluster and number-of-workers with the number of workers. Use Elasticfox to check all the instances are running, this can some time.

* login

::
	
	bin/hadoop-ec2 login clustername

* terminate

::
	
	bin/hadoop-ec2 terminate-cluster clustername

* You can check the status of jobs at masterip:50030 in your web browser.

Useful tools
------------
`s3fox <http://www.s3fox.net/>`_
    A S3 file browser that works within Firefox.
`Elasticfox <http://sourceforge.net/projects/elasticfox/>`_
    EC2 management tools, a Firefox add-on. 




