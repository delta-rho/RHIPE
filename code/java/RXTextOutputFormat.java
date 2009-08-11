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
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import org.saptarshiguha.rhipe.utils.*;

public class RXTextOutputFormat extends TextOutputFormat<RXWritable,RXWritable> {

  static class RXTextRecordWriter extends LineRecordWriter<RXWritable,RXWritable> {
    private static final byte[] newLine = "\r\n".getBytes();
    private static  byte[] keyvaluesep = " ".getBytes();
    private static final String utf8 = "UTF-8";

    public RXTextRecordWriter(DataOutputStream out,
                            JobConf conf) {
	super(out);
	keyvaluesep = conf.get("mapred.textoutputformat.separator"," ").getBytes();
    }

    public synchronized void write(RXWritable key, 
                                   RXWritable value) throws IOException {
// 	    boolean nullKey = key == null || key instanceof RXWritableNull;
// 	    boolean nullValue = value == null || value instanceof RXWritableNull;
// 	    if (nullKey && nullValue) {
// 		return;
// 	    }
// 	    if (!nullKey) {
// 		writeObject(key);
// 	    }
// 	    if (!(nullKey || nullValue)) {
// 		out.write(keyvaluesep);
// 	    }
// 	    if (!nullValue) {
// 		writeObject(value);
// 	    }
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

    public void close() throws IOException {

      super.close(null);
    }
  }

  public RecordWriter<RXWritable,RXWritable> getRecordWriter(FileSystem ignored,
                                                 JobConf job,
                                                 String name,
                                                 Progressable progress
                                                 ) throws IOException {
    Path dir = getWorkOutputPath(job);
    FileSystem fs = dir.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
    return new RXTextRecordWriter(fileOut, job);
  }
}
