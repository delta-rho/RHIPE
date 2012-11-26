// /**
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package org.godhuli.rhipe.index;
// import org.godhuli.rhipe.RHBytesWritable;

// import java.io.*;
// import java.util.List;

// import org.apache.hadoop.fs.*;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.BytesWritable;
// import org.apache.hadoop.io.compress.*;
// import org.apache.hadoop.mapreduce.lib.input.FileSplit;
// import org.apache.hadoop.conf.Configuration;

// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import java.io.IOException;
// import org.apache.hadoop.io.compress.CompressionCodec;
// import org.apache.hadoop.io.compress.CompressionCodecFactory;
// import org.apache.hadoop.mapreduce.InputFormat;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.InputSplit;
// import org.apache.hadoop.mapreduce.JobContext;
// import org.apache.hadoop.mapreduce.RecordReader;
// import org.apache.hadoop.mapreduce.TaskAttemptContext;
// import org.apache.hadoop.io.SequenceFile;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
// import java.lang.StringBuilder;

// public class IndexerInputFormat extends FileInputFormat<RHBytesWritable, BytesWritable> {
//     final static Log LOG = LogFactory.getLog(IndexerInputFormat.class);

//   protected long getFormatMinSplitSize() {
//       return SequenceFile.SYNC_INTERVAL;
//   }

//   protected List<FileStatus> listStatus(JobContext job)throws IOException {
      
//       List<FileStatus> files = super.listStatus(job);
//       int len = files.size();
//       for(int i=0; i < len; ++i) {
// 	  FileStatus file = files.get(i);
// 	  // if (file.isDir()) {     // it's a MapFile
// 	  //     Path p = file.getPath();
// 	  //     FileSystem fs = p.getFileSystem(job.getConfiguration());
// 	  //     files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
// 	  // }
//       }
//       return files;
//   }
  
//     public RecordReader<RHBytesWritable, BytesWritable> 
// 	createRecordReader(InputSplit split,
// 			   TaskAttemptContext context
// 			   ) throws IOException {
// 	return new IndexRecordReader();
//     }

//     class IndexRecordReader extends RecordReader<RHBytesWritable,BytesWritable>{
// 	private SequenceFile.Reader in;
// 	private long start;
// 	private long end;
// 	private boolean more = true;
// 	private RHBytesWritable key = new RHBytesWritable();
// 	// private RHBytesWritable value = null;
// 	protected Configuration conf;
// 	byte[] pathstr;
// 	long pos;
// 	BytesWritable avalue = null;
// 	RHBytesWritable dummy = new RHBytesWritable();
// 	public void initialize(InputSplit split, TaskAttemptContext context)
// 	    throws IOException, InterruptedException {
// 	    FileSplit fileSplit = (FileSplit) split;
// 	    conf = context.getConfiguration();    
// 	    Path path = fileSplit.getPath();
// 	    FileSystem fs = path.getFileSystem(conf);
// 	    this.in = new SequenceFile.Reader(fs, path, conf);
// 	    this.end = fileSplit.getStart() + fileSplit.getLength();
// 	    avalue = new BytesWritable();
// 	    if (fileSplit.getStart() > in.getPosition()) {
// 		in.sync(fileSplit.getStart());                  // sync to start
// 	    }
// 	    this.start = in.getPosition();
// 	    LOG.info("At the beginning, "+path.toString()+" has been synced to:"+start);
// 	    pos = this.start;
// 	    avalue.setCapacity(4096);
// 	    pathstr=path.toString().getBytes("UTF-8");
// 	    more = start < end;
// 	}
// 	public boolean nextKeyValue() 
// 	    throws IOException, InterruptedException {
// 	    if (!more) {
// 		return false;
// 	    }
// 	    long currentposition = in.getPosition();
// 	    if(currentposition > pos)
// 		pos = currentposition;
// 	    boolean gots = in.next(key);
// 	    if (!gots || (pos >= end && in.syncSeen())) {
// 		// LOG.info("Stopping because:"+!gots+" pos:"+pos+" end:"+end+"sync:"+in.syncSeen());
// 		more = false;
// 		key = null;
// 		avalue = null;
// 	    } else {
// 		LOG.info("Wriitng location "+pathstr+":"+pos);
// 		byte[] h = IndexService.toBytes(pos);
// 		byte[] tgt = avalue.getBytes();
// 		// will throw an exception if path is too long (~>4090) ...
// 		System.arraycopy(h,0,tgt,0, h.length);
// 		System.arraycopy(pathstr,0,tgt,h.length,pathstr.length);
// 		avalue.setSize(h.length+pathstr.length);
// 		in.getCurrentValue(dummy);
// 	    }
// 	    return more;
// 	}

// 	public RHBytesWritable getCurrentKey() {
// 	    return key;
// 	}
	
// 	public BytesWritable getCurrentValue() {
// 	    return avalue;
// 	}
  
// 	public float getProgress() throws IOException {
// 	    if (end == start) {
// 		return 0.0f;
// 	    } else {
// 		return Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
// 	    }
// 	}
  
// 	public synchronized void close() throws IOException { in.close(); }


//     }
// }
