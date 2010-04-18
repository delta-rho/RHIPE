#!/bin/bash -x

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size after gzip compression.
#
# This script is executed by /etc/init.d/ec2-run-user-data, and output is
# logged to /var/log/messages.
################################################################################

################################################################################
# Initialize variables
################################################################################

# Substitute environment variables passed by the client
export %ENV%

if [ -z "$MASTER_HOST" ]; then
  IS_MASTER=true
  MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`
else
  IS_MASTER=false
fi

REPO=${REPO:-stable}
HADOOP=hadoop${HADOOP_VERSION:+-${HADOOP_VERSION}} # prefix with a dash if set

function register_auto_shutdown() {
  if [ ! -z "$AUTO_SHUTDOWN" ]; then
    shutdown -h +$AUTO_SHUTDOWN >/dev/null &
  fi
}

function update_repo() {
  if which dpkg &> /dev/null; then
    cat > /etc/apt/sources.list.d/cloudera.list <<EOF
deb http://archive.cloudera.com/debian intrepid-$REPO contrib
deb-src http://archive.cloudera.com/debian intrepid-$REPO contrib
EOF
    sudo apt-get update
  elif which rpm &> /dev/null; then
    rm -f /etc/yum.repos.d/cloudera.repo
    cat > /etc/yum.repos.d/cloudera-$REPO.repo <<EOF
[cloudera-$REPO]
name=Cloudera's Distribution for Hadoop ($REPO)
mirrorlist=http://archive.cloudera.com/redhat/cdh/$REPO/mirrors
gpgkey = http://archive.cloudera.com/redhat/cdh/RPM-GPG-KEY-cloudera
gpgcheck = 0
EOF
    yum update -y yum
  fi
}

# Install a list of packages on debian or redhat as appropriate
function install_packages() {
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install $@
  elif which rpm &> /dev/null; then
    yum install -y $@
  else
    echo "No package manager found."
  fi
}

# Install any user packages specified in the USER_PACKAGES environment variable
function install_user_packages() {
  if [ ! -z "$USER_PACKAGES" ]; then
    install_packages $USER_PACKAGES
  fi
}

# Install Hadoop packages and dependencies
function install_hadoop() {
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install $HADOOP
    cp -r /etc/$HADOOP/conf.empty /etc/$HADOOP/conf.dist
    update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf /etc/$HADOOP/conf.dist 90
    apt-get -y install pig${PIG_VERSION:+-${PIG_VERSION}}
    apt-get -y install hive${HIVE_VERSION:+-${HIVE_VERSION}}
    apt-get -y install policykit # http://www.bergek.com/2008/11/24/ubuntu-810-libpolkit-error/
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP
    cp -r /etc/$HADOOP/conf.empty /etc/$HADOOP/conf.dist
    if [ ! -e /etc/alternatives/$HADOOP-conf ]; then # CDH1 RPMs use a different alternatives name
      conf_alternatives_name=hadoop
    else
      conf_alternatives_name=$HADOOP-conf
    fi
    alternatives --install /etc/$HADOOP/conf $conf_alternatives_name /etc/$HADOOP/conf.dist 90
    yum install -y hadoop-pig${PIG_VERSION:+-${PIG_VERSION}}
    yum install -y hadoop-hive${HIVE_VERSION:+-${HIVE_VERSION}}
  fi
}

function prep_disk() {
  mount=$1
  device=$2
  automount=${3:-false}

  echo "warning: ERASING CONTENTS OF $device"
  mkfs.xfs -f $device
  if [ ! -e $mount ]; then
    mkdir $mount
  fi
  mount -o defaults,noatime $device $mount
  if $automount ; then
    echo "$device $mount xfs defaults,noatime 0 0" >> /etc/fstab
  fi
}

function wait_for_mount {
  mount=$1
  device=$2

  mkdir $mount

  i=1
  echo "Attempting to mount $device"
  while true ; do
    sleep 10
    echo -n "$i "
    i=$[$i+1]
    mount -o defaults,noatime $device $mount || continue
    echo " Mounted."
    break;
  done
}

function make_hadoop_dirs {
  for mount in "$@"; do
    if [ ! -e $mount/hadoop ]; then
      mkdir -p $mount/hadoop
      chown hadoop:hadoop $mount/hadoop
    fi
  done
}

# Configure Hadoop by setting up disks and site file
function configure_hadoop() {

  install_packages xfsprogs # needed for XFS

  INSTANCE_TYPE=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-type`

  if [ -n "$EBS_MAPPINGS" ]; then
    # EBS_MAPPINGS is like "/ebs1,/dev/sdj;/ebs2,/dev/sdk"
    DFS_NAME_DIR=''
    FS_CHECKPOINT_DIR=''
    DFS_DATA_DIR=''
    for mapping in $(echo "$EBS_MAPPINGS" | tr ";" "\n"); do
      # Split on the comma (see "Parameter Expansion" in the bash man page)
      mount=${mapping%,*}
      device=${mapping#*,}
      wait_for_mount $mount $device
      DFS_NAME_DIR=${DFS_NAME_DIR},"$mount/hadoop/hdfs/name"
      FS_CHECKPOINT_DIR=${FS_CHECKPOINT_DIR},"$mount/hadoop/hdfs/secondary"
      DFS_DATA_DIR=${DFS_DATA_DIR},"$mount/hadoop/hdfs/data"
      FIRST_MOUNT=${FIRST_MOUNT-$mount}
      make_hadoop_dirs $mount
    done
    # Remove leading commas
    DFS_NAME_DIR=${DFS_NAME_DIR#?}
    FS_CHECKPOINT_DIR=${FS_CHECKPOINT_DIR#?}
    DFS_DATA_DIR=${DFS_DATA_DIR#?}

    DFS_REPLICATION=3 # EBS is internally replicated, but we also use HDFS replication for safety
  else
    case $INSTANCE_TYPE in
    m1.xlarge|c1.xlarge)
      DFS_NAME_DIR=/mnt/hadoop/hdfs/name,/mnt2/hadoop/hdfs/name
      FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary,/mnt2/hadoop/hdfs/secondary
      DFS_DATA_DIR=/mnt/hadoop/hdfs/data,/mnt2/hadoop/hdfs/data,/mnt3/hadoop/hdfs/data,/mnt4/hadoop/hdfs/data
      ;;
    m1.large)
      DFS_NAME_DIR=/mnt/hadoop/hdfs/name,/mnt2/hadoop/hdfs/name
      FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary,/mnt2/hadoop/hdfs/secondary
      DFS_DATA_DIR=/mnt/hadoop/hdfs/data,/mnt2/hadoop/hdfs/data
      ;;
    *)
      # "m1.small" or "c1.medium"
      DFS_NAME_DIR=/mnt/hadoop/hdfs/name
      FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary
      DFS_DATA_DIR=/mnt/hadoop/hdfs/data
      ;;
    esac
    FIRST_MOUNT=/mnt
    DFS_REPLICATION=3
  fi

  case $INSTANCE_TYPE in
  m1.xlarge|c1.xlarge)
    prep_disk /mnt2 /dev/sdc true &
    disk2_pid=$!
    prep_disk /mnt3 /dev/sdd true &
    disk3_pid=$!
    prep_disk /mnt4 /dev/sde true &
    disk4_pid=$!
    wait $disk2_pid $disk3_pid $disk4_pid
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local,/mnt2/hadoop/mapred/local,/mnt3/hadoop/mapred/local,/mnt4/hadoop/mapred/local
    MAX_MAP_TASKS=8
    MAX_REDUCE_TASKS=4
    CHILD_OPTS=-Xmx680m
    CHILD_ULIMIT=1392640
    ;;
  m1.large)
    prep_disk /mnt2 /dev/sdc true
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local,/mnt2/hadoop/mapred/local
    MAX_MAP_TASKS=4
    MAX_REDUCE_TASKS=2
    CHILD_OPTS=-Xmx1024m
    CHILD_ULIMIT=2097152
    ;;
  c1.medium)
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local
    MAX_MAP_TASKS=4
    MAX_REDUCE_TASKS=4
    CHILD_OPTS=-Xmx550m
    CHILD_ULIMIT=1126400
    ;;
  *)
    # "m1.small"
    MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local
    MAX_MAP_TASKS=2
    MAX_REDUCE_TASKS=1
    CHILD_OPTS=-Xmx550m
    CHILD_ULIMIT=1126400
    ;;
  esac

  make_hadoop_dirs `ls -d /mnt*`

  # Create tmp directory
  mkdir /mnt/tmp
  chmod a+rwxt /mnt/tmp

  ##############################################################################
  # Modify this section to customize your Hadoop cluster.
  ##############################################################################
  cat > /etc/$HADOOP/conf.dist/hadoop-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
  <name>dfs.block.size</name>
  <value>134217728</value>
  <final>true</final>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>$DFS_DATA_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.du.reserved</name>
  <value>1073741824</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.handler.count</name>
  <value>3</value>
  <final>true</final>
</property>
<!--property>
  <name>dfs.hosts</name>
  <value>/etc/$HADOOP/conf.dist/dfs.hosts</value>
  <final>true</final>
</property-->
<!--property>
  <name>dfs.hosts.exclude</name>
  <value>/etc/$HADOOP/conf.dist/dfs.hosts.exclude</value>
  <final>true</final>
</property-->
<property>
  <name>dfs.name.dir</name>
  <value>$DFS_NAME_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.namenode.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>dfs.permissions</name>
  <value>true</value>
  <final>true</final>
</property>
<property>
  <name>dfs.replication</name>
  <value>$DFS_REPLICATION</value>
</property>
<property>
  <name>fs.checkpoint.dir</name>
  <value>$FS_CHECKPOINT_DIR</value>
  <final>true</final>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:8020/</value>
</property>
<property>
  <name>fs.trash.interval</name>
  <value>1440</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/tmp/hadoop-\${user.name}</value>
  <final>true</final>
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
  <name>mapred.job.tracker</name>
  <value>$MASTER_HOST:8021</value>
</property>
<property>
  <name>mapred.job.tracker.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>mapred.local.dir</name>
  <value>$MAPRED_LOCAL_DIR</value>
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
  <name>mapred.system.dir</name>
  <value>/hadoop/system/mapred</value>
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
  <name>mapred.jobtracker.taskScheduler</name>
  <value>org.apache.hadoop.mapred.FairScheduler</value>
</property>
<property>
  <name>mapred.fairscheduler.allocation.file</name>
  <value>/etc/$HADOOP/conf.dist/fairscheduler.xml</value>
</property>
<property>
  <name>mapred.compress.map.output</name>
  <value>true</value>
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
  <name>fs.s3.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>
<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>
<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>
<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>
</configuration>
EOF

  cat > /etc/$HADOOP/conf.dist/fairscheduler.xml <<EOF
<?xml version="1.0"?>
<allocations>
</allocations>
EOF

  # Keep PID files in a non-temporary directory
  sed -i -e "s|# export HADOOP_PID_DIR=.*|export HADOOP_PID_DIR=/var/run/hadoop|" \
    /etc/$HADOOP/conf.dist/hadoop-env.sh
  mkdir -p /var/run/hadoop
  chown -R hadoop:hadoop /var/run/hadoop

  # Set SSH options within the cluster
  sed -i -e 's|# export HADOOP_SSH_OPTS=.*|export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no"|' \
    /etc/$HADOOP/conf.dist/hadoop-env.sh

  # Hadoop logs should be on the /mnt partition
  rm -rf /var/log/hadoop
  mkdir /mnt/hadoop/logs
  chown hadoop:hadoop /mnt/hadoop/logs
  ln -s /mnt/hadoop/logs /var/log/hadoop
  chown -R hadoop:hadoop /var/log/hadoop

}

# Sets up small website on cluster.
# TODO(philip): Add links/documentation.
function setup_web() {

  if which dpkg &> /dev/null; then
    apt-get -y install thttpd
    WWW_BASE=/var/www
  elif which rpm &> /dev/null; then
    yum install -y thttpd
    chkconfig --add thttpd
    WWW_BASE=/var/www/thttpd/html
  fi

  cat > $WWW_BASE/index.html << END
<html>
<head>
<title>Hadoop EC2 Cluster</title>
</head>
<body>
<h1>Hadoop EC2 Cluster</h1>
To browse the cluster you need to have a proxy configured.
Start the proxy with <tt>hadoop-ec2 proxy &lt;cluster_name&gt;</tt>,
and point your browser to
<a href="http://cloudera-public.s3.amazonaws.com/ec2/proxy.pac">this Proxy
Auto-Configuration (PAC)</a> file.  To manage multiple proxy configurations,
you may wish to use
<a href="https://addons.mozilla.org/en-US/firefox/addon/2464">FoxyProxy</a>.
<ul>
<li><a href="http://$MASTER_HOST:50070/">NameNode</a>
<li><a href="http://$MASTER_HOST:50030/">JobTracker</a>
</ul>
</body>
</html>
END

  service thttpd start

}

function start_hadoop_master() {

  if which dpkg &> /dev/null; then
    AS_HADOOP="su -s /bin/bash - hadoop -c"
    # Format HDFS
    [ ! -e $FIRST_MOUNT/hadoop/hdfs ] && $AS_HADOOP "$HADOOP namenode -format"
    apt-get -y install $HADOOP-namenode
    apt-get -y install $HADOOP-secondarynamenode
    apt-get -y install $HADOOP-jobtracker
  elif which rpm &> /dev/null; then
    AS_HADOOP="/sbin/runuser -s /bin/bash - hadoop -c"
    # Format HDFS
    [ ! -e $FIRST_MOUNT/hadoop/hdfs ] && $AS_HADOOP "$HADOOP namenode -format"
    chkconfig --add $HADOOP-namenode
    chkconfig --add $HADOOP-secondarynamenode
    chkconfig --add $HADOOP-jobtracker
  fi

  service $HADOOP-namenode start
  service $HADOOP-secondarynamenode start
  service $HADOOP-jobtracker start

  $AS_HADOOP "$HADOOP dfsadmin -safemode wait"
  $AS_HADOOP "/usr/bin/$HADOOP fs -mkdir /user"
  # The following is questionable, as it allows a user to delete another user
  # It's needed to allow users to create their own user directories
  $AS_HADOOP "/usr/bin/$HADOOP fs -chmod +w /user"

  # Create temporary directory for Pig and Hive in HDFS
  $AS_HADOOP "/usr/bin/$HADOOP fs -mkdir /tmp"
  $AS_HADOOP "/usr/bin/$HADOOP fs -chmod +w /tmp"
  $AS_HADOOP "/usr/bin/$HADOOP fs -mkdir /user/hive/warehouse"
  $AS_HADOOP "/usr/bin/$HADOOP fs -chmod +w /user/hive/warehouse"
}

function start_hadoop_slave() {

  if which dpkg &> /dev/null; then
    apt-get -y install $HADOOP-datanode
    apt-get -y install $HADOOP-tasktracker
  elif which rpm &> /dev/null; then
    yum install -y $HADOOP-datanode
    yum install -y $HADOOP-tasktracker
    chkconfig --add $HADOOP-datanode
    chkconfig --add $HADOOP-tasktracker
  fi

  service $HADOOP-datanode start
  service $HADOOP-tasktracker start
}

function install_rhipe_extras(){
    cd /root
    PROT=protobuf-2.3.0
    RHIPE=0.58
    case $INSTANCE_TYPE in
    m1.xlarge|c1.xlarge)
	    MAKE_THREAD=6
	    ;;
    m1.large)
	    MAKE_THREAD=4
	    ;;
    *)
      # "m1.small" or "c1.medium"
	    MAKE_THREAD=1
	    ;;
    esac
    install_packages "R"
    ## Install protobuf
    wget http://protobuf.googlecode.com/files/${PROT}.tar.gz
    tar zxvf ${PROT}.tar.gz
    cd ${PROT}
    ./configure && make -j${MAKE_THREAD} && make install
    echo "export HADOOP=/usr/" >> /root/.bashrc
    ##Install RHIPE
    cd /root
    wget http://www.stat.purdue.edu/~sguha/rhipe/dn/Rhipe_${RHIPE}.tar.gz
    PKG_CONFIG_PATH=/usr/local/lib/pkgconfig R CMD INSTALL Rhipe_${RHIPE}.tar.gz
    echo "export LD_LIBRARY_PATH=/usr/local/lib" >> /root/.bashrc
    echo "export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig" >> /root/.bashrc
    echo "export LD_LIBRARY_PATH=/usr/local/lib" >> /etc/$HADOOP/conf.dist/hadoop-env.sh
    ##Must start like
    ## hadoop-ec2 launch-cluster --env REPO=testing --env HADOOP_VERSION=0.20
    chmod a+rwxt /mnt*/hadoop/mapred/local
}

function run_r_code(){
    cat > /root/users_r_code.r << END
install.packages("yaImpute",dependencies=TRUE,repos='http://cran.r-project.org')
download.file("http://ml.stat.purdue.edu/rpackages/survstl_0.1-1.tar.gz","/root/survstl_0.1-1.tar.gz")
download.file("http://ml.stat.purdue.edu/rpackages/stl2_1.0.tar.gz","/root/stl2_1.0.tar.gz")
install.packages("/root/survstl_0.1-1.tar.gz",repos=NULL,dependenc=TRUE)
install.packages("/root/stl2_1.0.tar.gz",repos=NULL,dependenc=TRUE)
END

R CMD BATCH /root/users_r_code.r
}

register_auto_shutdown
update_repo
install_user_packages
install_hadoop
configure_hadoop
install_rhipe_extras
## uncomment the following line and modify
## run_r_code to install your own packages
 run_r_code

if $IS_MASTER ; then
  setup_web
  start_hadoop_master
else
  start_hadoop_slave
fi
