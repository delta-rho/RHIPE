/*
 * RHIPE - software that integrates Hadoop mapreduce with R
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Saptarshi Guha sguha@purdue.edu
 */
package org.saptarshiguha.rhipe.hadoop;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;



import org.saptarshiguha.rhipe.utils.*;

public class RXTextOutputFormat extends TextOutputFormat<RXWritable,RXWritable> {

  static class RXTextRecordWriter extends RecordWriter<RXWritable,RXWritable> {
    private static final byte[] newLine = "\r\n".getBytes();
    private static  byte[] keyvaluesep = " ".getBytes();
    private static final String utf8 = "UTF-8";
    protected DataOutputStream out;

    public RXTextRecordWriter(DataOutputStream out,
			      String keyValueSeparator) {
	this.out=out;
	try{
	    keyvaluesep =keyValueSeparator.getBytes(utf8);;
	}catch (UnsupportedEncodingException uee) {
	    throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	}

    }

    public synchronized void write(RXWritable key, 
                                   RXWritable value) throws IOException {

	    writeObject(key);
	    out.write(keyvaluesep);
	    writeObject(value);
	    out.write(newLine, 0, newLine.length);
	}
    private void writeObject(RXWritable o) throws IOException {
	if (o instanceof RXWritableText) {
	    RXWritableText to = (RXWritableText) o;
	    out.write(to.getBytes(), 0, to.getLength());
	} else {
	    out.write(o.toString().getBytes(utf8));
	}
    }

    public synchronized 
	void close(TaskAttemptContext context) throws IOException {
	    out.close();
    }

  }


  public RecordWriter<RXWritable,RXWritable>
      getRecordWriter(TaskAttemptContext job
		      ) throws IOException, InterruptedException {
      Configuration conf = job.getConfiguration();
      boolean isCompressed = getCompressOutput(job);
      String keyValueSeparator= conf.get("mapred.textoutputformat.separator",
					 "\t");
      CompressionCodec codec = null;
      String extension = "";
      if (isCompressed) {
	  Class<? extends CompressionCodec> codecClass = 
	      getOutputCompressorClass(job, GzipCodec.class);
	  codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	  extension = codec.getDefaultExtension();
      }
      Path file = getDefaultWorkFile(job, extension);
      FileSystem fs = file.getFileSystem(conf);
      if (!isCompressed) {
	  FSDataOutputStream fileOut = fs.create(file, false);
	  return new RXTextRecordWriter(fileOut, keyValueSeparator);
      } else {
	  FSDataOutputStream fileOut = fs.create(file, false);
	  return new RXTextRecordWriter(new DataOutputStream
					    (codec.createOutputStream(fileOut)),
					    keyValueSeparator);
    }

  }
}
