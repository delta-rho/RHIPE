#!/bin/bash

sudo yum -y update
sudo rpm -Uvh http://mirror.pnl.gov/epel//6/x86_64/epel-release-6-8.noarch.rpm
sudo yum -y update

sudo yum -y install R

wget http://download2.rstudio.org/rstudio-server-0.98.507-x86_64.rpm
wget ftp://195.220.108.108/linux/Mandriva/official/2008.0/x86_64/media/main/release/libgfortran1-4.1.2-1mdv2007.1.x86_64.rpm
sudo yum -y install --nogpgcheck libgfortran1-4.1.2-1mdv2007.1.x86_64.rpm
sudo yum -y install --nogpgcheck rstudio-server-0.98.507-x86_64.rpm
echo "manual" | sudo tee /etc/init/rstudio-server.override
sudo useradd rstudio
echo "rstudio:rstudio" | sudo chpasswd
sudo mkdir /home/rstudio
sudo chmod -R 0777 /home/rstudio

sudo su - -c "R -e \"install.packages('shiny', repos='http://cran.rstudio.com/')\""
wget http://download3.rstudio.org/centos-6.3/x86_64/shiny-server-1.2.0.342-x86_64.rpm
sudo yum -y install --nogpgcheck shiny-server-1.1.0.10000-x86_64.rpm
echo "manual" | sudo tee /etc/init/shiny-server.override

sudo mkdir /srv/shiny-server/examples
sudo cp -R /usr/lib64/R/library/shiny/examples/* /srv/shiny-server/examples/
sudo chown -R shiny:shiny /srv/shiny-server/examples

sudo yum -y install libcurl-devel mongodb java-1.6.0-openjdk-devel.x86_64
sudo su - -c "R -e \"install.packages('rJava', repos='http://cran.rstudio.com/')\""

wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz
tar -xzf protobuf-2.5.0.tar.gz
cd protobuf-2.5.0
./configure
sudo make -j4
sudo make install
sudo ldconfig
cd ..

###can't get this to work as-is - need to login as root "sudo su" then run the commands 
###
###
sudo su
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/lib/pkgconfig
sudo echo export LD_LIBRARY_PATH=$LD_LIBRARY_PATH | sudo tee -a /etc/profile
#sudo -E R CMD INSTALL Rhipe_0.75.0_cdh4mr1.tar.gz
#sudo -E R CMD INSTALL Rhipe_0.75.0_cdh5mr1.tar.gz
sudo -E R CMD INSTALL /vagrant/Rhipe_0.75.0_cdh3.tar.gz
exit

###### STOPPED HERE FOR CDH3 - cant get any further
## devtools
sudo su - -c "R -e \"install.packages('devtools', repos='http://cran.rstudio.com/')\""
## datadr
## REQUIRES MANUAL INSTALL!
sudo su - -c "R -e \"options(repos = 'http://cran.rstudio.com/'); library(devtools); install_github('datadr', 'hafen')\""
## trelliscope
## REQUIRES MANUAL INSTALL!
sudo su - -c "R -e \"options(repos = 'http://cran.rstudio.com/'); library(devtools); install_github('trelliscope', 'hafen')\""

sudo yum -y install git

export JAVA_CPPFLAGS=-I/usr/lib/jvm/java-1.6.0/jre/../include/
export JAVAC=/usr/bin/javac
export JAVA_HOME=/usr/lib/jvm/java-1.6.0/jre
export JAVAH=/usr/bin/javah
export JAVA_LD_LIBRARY_PATH=/usr/lib/jvm/java-1.6.0/jre/lib/amd64/server
export JAVA_LIBS=-L/usr/lib/jvm/java-1.6.0/jre/lib/amd64/server
export JAVA=/usr/bin/java

sudo echo 'export JAVA_CPPFLAGS=-I/usr/lib/jvm/java-1.6.0/jre/../include/' | sudo tee -a /etc/profile
sudo echo 'export JAVAC=/usr/bin/javac' | sudo tee -a /etc/profile
sudo echo 'export JAVA_HOME=/usr/lib/jvm/java-1.6.0/jre' | sudo tee -a /etc/profile
sudo echo 'export JAVAH=/usr/bin/javah' | sudo tee -a /etc/profile
sudo echo 'export JAVA_LD_LIBRARY_PATH=/usr/lib/jvm/java-1.6.0/jre/lib/amd64/server' | sudo tee -a /etc/profile
sudo echo 'export JAVA_LIBS=-L/usr/lib/jvm/java-1.6.0/jre/lib/amd64/server' | sudo tee -a /etc/profile
sudo echo 'export JAVA=/usr/bin/java' | sudo tee -a /etc/profile

## From Cloudera:
## https://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH5-Quick-Start/cdh5qs_mrv1_pseudo.html
##

wget http://archive.cloudera.com/cdh5/one-click-install/redhat/6/x86_64/cloudera-cdh-5-0.x86_64.rpm
sudo yum -y --nogpgcheck localinstall cloudera-cdh-5-0.x86_64.rpm
sudo rpm --import http://archive.cloudera.com/cdh5/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera
sudo yum -y install hadoop-0.20-conf-pseudo
# rpm -ql hadoop-0.20-conf-pseudo 

sudo -u hdfs hdfs namenode -format
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done
sudo -u hdfs hadoop fs -mkdir -p /tmp
sudo -u hdfs hadoop fs -chmod -R 1777 /tmp
sudo -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -u hdfs hadoop fs -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred
# sudo -u hdfs hadoop fs -ls -R /

for x in `cd /etc/init.d ; ls hadoop-0.20-mapreduce-*` ; do sudo service $x start ; done

sudo -u hdfs hadoop fs -mkdir -p /user/vagrant
sudo -u hdfs hadoop fs -chown vagrant /user/vagrant

#run example from cloudera to test install

export HADOOP=/usr/lib/hadoop
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_BIN=$HADOOP/bin
export HADOOP_LIBS=`hadoop classpath | tr -d '*'`
export HADOOP_CONF_DIR=/etc/hadoop/conf

sudo echo export HADOOP=$HADOOP | sudo tee -a /etc/profile
sudo echo export HADOOP_HOME=$HADOOP_HOME | sudo tee -a /etc/profile
sudo echo export HADOOP_BIN=$HADOOP_BIN | sudo tee -a /etc/profile
sudo echo export HADOOP_LIBS=$HADOOP_LIBS | sudo tee -a /etc/profile
sudo echo export HADOOP_CONF_DIR=$HADOOP_CONF_DIR | sudo tee -a /etc/profile

