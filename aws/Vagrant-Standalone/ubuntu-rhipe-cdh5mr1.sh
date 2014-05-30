#!/bin/bash


#### install cdh5mr1
sudo R CMD INSTALL /vagrant/Rhipe_0.75.0_cdh5mr1.tar.gz

wget http://archive.cloudera.com/cdh5/one-click-install/precise/amd64/cdh5-repository_1.0_all.deb
sudo dpkg -i cdh5-repository_1.0_all.deb
curl -s http://archive.cloudera.com/cdh5/ubuntu/precise/amd64/cdh/archive.key | sudo apt-key add -
sudo apt-get  --yes --force-yes update 
sudo apt-get  --yes --force-yes install hadoop-0.20-conf-pseudo
sudo -u hdfs hdfs namenode -format
for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done
sudo -u hdfs hadoop fs -mkdir -p /tmp 
sudo -u hdfs hadoop fs -chmod -R 1777 /tmp
sudo -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
sudo -u hdfs hadoop fs -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred
for x in `cd /etc/init.d ; ls hadoop-0.20-mapreduce-*` ; do sudo service $x start ; done
sudo -u hdfs hadoop fs -mkdir -p /user/vagrant
sudo -u hdfs hadoop fs -chown vagrant /user/vagrant

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

## TODO ##
# setup .Renviron file in /home/rstudio
# install_github("vastChallenge", "hafen", subdir = "package")
