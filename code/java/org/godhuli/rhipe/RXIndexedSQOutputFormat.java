/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.FileNotFoundException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileUtil;


import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import java.io.File;

public class RXIndexedSQOutputFormat  extends SequenceFileOutputFormat<RHBytesWritable, RHBytesWritable> 
{
    final static Log LOG = LogFactory.getLog(RXIndexedSQOutputFormat.class);

    public RecordWriter<RHBytesWritable, RHBytesWritable> getRecordWriter(TaskAttemptContext context) 
	throws IOException, InterruptedException 
	{
	    Configuration conf = context.getConfiguration();
	    CompressionCodec codec = null;
	    CompressionType compressionType = CompressionType.NONE;
	    if (getCompressOutput(context)) {
		compressionType = getOutputCompressionType(context);
		Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
		codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	    }
	    Path file = getDefaultWorkFile(context, "");
	    final FileSystem fs = file.getFileSystem(conf);
	    final Path dir = new Path(file.toString());
	    if (!fs.mkdirs(dir)) {
		throw new IOException("RHIPE: Mkdirs in RXIndexedSQOutputFormat failed to create directory " + dir.toString());
	    }
	    Path dataFile = new Path(dir, "data");
	    final SequenceFile.Writer out = SequenceFile.createWriter(fs, conf, dataFile,
								      context.getOutputKeyClass(),
								      context.getOutputValueClass(),
								      compressionType,
								      codec,
								      context);
	    LocalFileSystem lfs = new LocalFileSystem();
	    String unqfile = getUniqueFile(context, "bdb","tmp");
	    if(!lfs.mkdirs(new Path(unqfile)))
		throw new IOException("RHIPE: Mkdirs in RXIndexedSQOutputFormat failed to create directory " + unqfile);
	    
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    final File tempfile = new File(unqfile);
	    // if(!tempfile.mkdirs())
	    final Environment myDbEnvironment = new Environment(tempfile, envConfig);
	    DatabaseConfig dbConfig = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    final Database myDatabase = myDbEnvironment.openDatabase(null, "indices", dbConfig); 
	    
	    return new RecordWriter<RHBytesWritable, RHBytesWritable>() {
		DatabaseEntry theKey = new DatabaseEntry();
		DatabaseEntry theData = new DatabaseEntry();
		byte [] b = new byte[8];
		long counter = 0;
		public byte[] toBytes(long val) {
		    for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		    }
		    b[0] = (byte) val;
		    return b;
		}
		public void write(RHBytesWritable key, RHBytesWritable value)
		    throws IOException {
		    counter += 1;
		    theKey.setData(key.getBytes(),0,key.getLength());
		    theData.setData(toBytes(out.getLength()));
		    myDatabase.put(null, theKey, theData);
		    if(counter % 1000 == 0){
			myDbEnvironment.sync();
			LOG.info("Synced "+counter+ "keys");
		    }
		    out.append(key, value);
		}
		
		public void close(TaskAttemptContext context) throws IOException {
		    myDatabase.close();
		    myDbEnvironment.close();
		    out.close();
		    Path pnew = new Path(dir,"index.db");
		    FileUtil.copy(tempfile, fs,pnew,true,context.getConfiguration());
		}
	    };
	}

    public static CompressionType getOutputCompressionType(JobContext job) {
        String val = job.getConfiguration().get("mapred.output.compression.type", CompressionType.RECORD.toString());
        return CompressionType.valueOf(val);
    }

    public static void setOutputCompressionType(Job job, CompressionType style) {
        setCompressOutput(job, true);
        job.getConfiguration().set("mapred.output.compression.type", style.toString());
    }
}

