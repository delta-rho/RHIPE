#!/usr/bin/env bash

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size.
################################################################################

################################################################################
# Initialize variables
################################################################################

# Slaves are started after the master, and are told its address by sending a
# modified copy of this file which sets the MASTER_HOST variable. 
# A node  knows if it is the master or not by inspecting the security group
# name. If it is the master then it retrieves its address using instance data.
MASTER_HOST=%MASTER_HOST% # Interpolated before being sent to EC2 node
INSTANCE_TYPE=%INSTANCE_TYPE%
NUMSLAVES=%NUMSLAVES%
export AWS_ACCESS_KEY_ID=%AWS_ACCESS_KEY_ID%
export AWS_SECRET_ACCESS_KEY=%AWS_SECRET_ACCESS_KEY%
RSOPTS="%RSOPTS%"
R_USER_FILE=%R_USER_FILE% 
R_USER_FILE_IS_PUBLIC="%R_USER_FILE_IS_PUBLIC%"
SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`
if [ "$IS_MASTER" == "true" ]; then
 MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
fi

HADOOP_HOME=`ls -d /usr/local/hadoop-*`
################################################################################
# Hadoop configuration
# Modify this section to customize your Hadoop cluster.
################################################################################

case "$INSTANCE_TYPE" in
"m1.large")
  MAX_MAP_TASKS=2
  MAX_REDUCE_TASKS=1
  CHILD_OPTS=-Xmx1024m
  CHILD_ULIMIT=2097152
  ;;
"m1.xlarge")
  MAX_MAP_TASKS=6
  MAX_REDUCE_TASKS=4
  CHILD_OPTS=-Xmx1024m
  CHILD_ULIMIT=1392640
  ;;
"c1.large")
  MAX_MAP_TASKS=4
  MAX_REDUCE_TASKS=2
  CHILD_OPTS=-Xmx550m
  CHILD_ULIMIT=1126400
  ;;
"c1.xlarge")
  MAX_MAP_TASKS=6
  MAX_REDUCE_TASKS=4
  CHILD_OPTS=-Xmx680m
  CHILD_ULIMIT=1392640
  ;;
*)
  # "m1.small
  MAX_MAP_TASKS=2
  MAX_REDUCE_TASKS=1
  CHILD_OPTS=-Xmx550m
  CHILD_ULIMIT=1126400
  ;;
esac
#Copy the fair scheduler
cp $HADOOP_HOME/contrib/fairscheduler/*jar $HADOOP_HOME/lib

cat > $HADOOP_HOME/conf/hadoop-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
<property>
  <name>dfs.block.size</name>
  <value>134217728</value>
  <final>true</final>
</property>
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
</property>
<property>
  <name>dfs.datanode.du.reserved</name>
  <value>1073741824</value>
  <final>true</final>
</property>
<property>
  <name>fs.trash.interval</name>
  <value>1440</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.handler.count</name>
  <value>3</value>
  <final>true</final>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:50001</value>
</property>

<property>
  <name>mapred.job.tracker</name>
  <value>hdfs://$MASTER_HOST:50002</value>
</property>
<property>
  <name>dfs.namenode.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>tasktracker.http.threads</name>
  <value>80</value>
</property>



<property>
  <name>mapred.output.compress</name>
  <value>true</value>
</property>

<property>
  <name>mapred.output.compression.type</name>
  <value>BLOCK</value>
</property>

<property>
  <name>dfs.client.block.write.retries</name>
  <value>3</value>
</property>
<property>
  <name>io.file.buffer.size</name>
  <value>65536</value>
</property>
<property>
  <name>mapred.child.java.opts</name>
  <value>$CHILD_OPTS</value>
</property>
<property>
  <name>mapred.child.ulimit</name>
  <value>$CHILD_ULIMIT</value>
  <final>true</final>
</property>
<property>
  <name>mapred.job.tracker.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>mapred.map.tasks.speculative.execution</name>
  <value>true</value>
</property>
<property>
  <name>mapred.reduce.parallel.copies</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks.speculative.execution</name>
  <value>false</value>
</property>
<property>
  <name>mapred.submit.replication</name>
  <value>10</value>
</property>
<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>$MAX_MAP_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>$MAX_REDUCE_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>tasktracker.http.threads</name>
  <value>46</value>
  <final>true</final>
</property>
<property>
  <name>mapred.output.compression.type</name>
  <value>BLOCK</value>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.default</name>
  <value>org.apache.hadoop.net.StandardSocketFactory</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.ClientProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.JobSubmissionProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>io.compression.codecs</name>
  <value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec</value>
</property>
<property>
  <name>mapred.jobtracker.taskScheduler</name>
  <value>org.apache.hadoop.mapred.FairScheduler</value>
</property>

<property>
  <name>mapred.fairscheduler.assignmultiple</name>
  <value>true</value>
</property>
<property>
  <name>mapred.fairscheduler.sizebasedweight</name>
  <value>true</value>
</property>

<property>
  <name>mapred.fairscheduler.weightadjuster</name>
  <value>org.apache.hadoop.mapred.NewJobWeightBooster</value>
</property>

</configuration>
EOF

# Configure Hadoop for Ganglia
# overwrite hadoop-metrics.properties
cat > $HADOOP_HOME/conf/hadoop-metrics.properties <<EOF

# Ganglia
# we push to the master gmond so hostnames show up properly
dfs.class=org.apache.hadoop.metrics.ganglia.GangliaContext
dfs.period=10
dfs.servers=$MASTER_HOST:8649

mapred.class=org.apache.hadoop.metrics.ganglia.GangliaContext
mapred.period=10
mapred.servers=$MASTER_HOST:8649

jvm.class=org.apache.hadoop.metrics.ganglia.GangliaContext
jvm.period=10
jvm.servers=$MASTER_HOST:8649
EOF

################################################################################
# Start services
################################################################################

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

mkdir -p /mnt/hadoop/logs

# not set on boot
export USER="root"

if [ "$IS_MASTER" == "true" ]; then
  # MASTER
  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\( *mute *=.*\)|  mute = yes|" \
         -e "s|\( *location *=.*\)|  location = \"master-node\"|" \
         /etc/gmond.conf
  mkdir -p /mnt/ganglia/rrds
  chown -R ganglia:ganglia /mnt/ganglia/rrds
  rm -rf /var/lib/ganglia; cd /var/lib; ln -s /mnt/ganglia ganglia; cd
  service gmond start
  service gmetad start
  apachectl start

  # Hadoop
  # only format on first boot
  [ ! -e /mnt/hadoop/dfs ] && "$HADOOP_HOME"/bin/hadoop namenode -format

  "$HADOOP_HOME"/bin/hadoop-daemon.sh start namenode
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start jobtracker
#   "$HADOOP_HOME"/bin/hadoop dfsadmin -safemode wait
#   "$HADOOP_HOME"/bin/hadoop fs -mkdir /tmp
else
  # SLAVE
  # Prep Ganglia
  sed -i -e "s|\( *mcast_join *=.*\)|#\1|" \
         -e "s|\( *bind *=.*\)|#\1|" \
         -e "s|\(udp_send_channel {\)|\1\n  host=$MASTER_HOST|" \
         /etc/gmond.conf
  service gmond start

  # Hadoop
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker
fi
##Install S3Sync
cd /root
wget http://s3.amazonaws.com/ServEdge_pub/s3sync/s3sync.tar.gz
tar -xzvf s3sync.tar.gz 
rm -rf s3sync.tar.gza

##Ideally, should just run in MASTER
##SLAVES copy this user_r.r from MASTER
mkdir -p /opt/rhipe/etc
mkdir -p /opt/rJava
wget -q http://ml.stat.purdue.edu/rhipe/dn/rhipe.tgz -P /opt/rhipe/
wget -q http://www.rforge.net/rJava/snapshot/rJava_0.6-4.tar.gz -P /opt/rJava

R CMD Rserve --no-save --RS-conf /opt/rhipe/conf/rserve.conf $RSOPTS 2>&1 1>/tmp/rserve.log
R CMD INSTALL  /opt/rhipe/rhipe
R CMD INSTALL /opt/rJava/rJava_0.6-4.tar.gz

case "$R_USER_FILE_IS_PUBLIC" in
    "0" )
	cd /root/
	/root/s3sync/s3cmd.rb get $R_USER_FILE /root/user_r.r 
        R CMD BATCH  --no-restore --no-save /root/user_r.r /root/user_r.Rout ;;
    "1" )
	wget --waitretry=5 $R_USER_FILE -O /root/user_r.r 
	R CMD BATCH  --no-restore --no-save /root/user_r.r /root/user_r.Rout ;;
esac

## If this file has sensitive information
# case "$R_USER_FILE_IS_PUBLIC" in
#     "0" )
# 	rm -f /tmp/ec2-user-data.* ;;
# esac


# Run this script on next boot
rm -f /var/ec2/ec2-run-user-data.*
