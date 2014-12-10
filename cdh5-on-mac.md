##Installing CDH5 in Pseudodistributed mode with YARN on a Mac##

Based loosely on the instructions from Cloudera:  
http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH5-Quick-Start/cdh5qs_yarn_pseudo.html

Download CDH5 Hadoop from:  
http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH-Version-and-Packaging-Information/cdhvd_cdh_package_tarball.html  
  
untar the download - this location will be referred to as **hadoop_home**

**Unset any Hadoop related environment variables!**

edit hadoop_home/etc/hadoop/core-site.xml  
Add:
```  
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:8020</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/cdh5</value>
    </property>
```
    
###Start HDFS:###
Format first:  
`hadoop_home/bin/hdfs namenode -format`
  
Then run:  
`hadoop_home/sbin/start-dfs.sh`

Go to http://localhost:50070/ to verify it started

From the Cloudera doc - create these directories:  

```
hadoop_home/bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/staging/history/done_intermediate
hadoop_home/bin/hadoop fs -chmod -R 1777 /tmp 
hadoop_home/bin/hadoop fs -mkdir -p /var/log/hadoop-yarn
```
  
Verify:
`hadoop_home/bin/hadoop fs -ls -R /`

###Start YARN###
`hadoop_home/sbin/start-yarn.sh`

Verify you have one node running:  
http://localhost:8088/cluster/nodes

Create user directory:  
hadoop fs -mkdir /user/<username>`
    
###Run the example:###

**DO NOT SET `HADOOP_MAPRED_HOME` as stated in the Cloudera doc.**  this hosed the Hadoop classpath and did not work (at least for me)

Setup the test directory and data  

```
hadoop_home/bin/hadoop fs -mkdir input
hadoop_home/bin/hadoop fs -put hadoop_home/etc/hadoop/*.xml input
hadoop_home/bin/hadoop fs -ls input
```

Run the example:  
`hadoop_home/bin/hadoop jar hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-examples-{version}-cdh5.0.0.jar grep input output23 'dfs[a-z.]+'`
