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
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;


public class LApplyInputFormat extends 
				   InputFormat<RXWritableRAW, RXWritableRAW>
{
    protected static class LApplyReader extends 
	RecordReader<RXWritableRAW, RXWritableRAW> {
	private LApplyInputSplit split;
	private long leftover;
	private long pos = 0; 
	private RXWritableRAW key = null;
	private RXWritableRAW value = null;
	private REXPRaw r;
	private byte[] hdr = new byte[] {0x00,0x00,0x00,0x0e,0x00,0x00,0x00,0x01};
	protected LApplyReader(LApplyInputSplit split, TaskAttemptContext tac) throws IOException{
	    this.split = split;
	    this.leftover = split.getLength();
	}
	public void initialize(InputSplit split,
			       TaskAttemptContext context)
	    throws IOException,
	    InterruptedException{}
	public void close()  throws IOException{ }
	public RXWritableRAW getCurrentKey() throws IOException,InterruptedException {
	    return key;	
	}
	public RXWritableRAW getCurrentValue() throws IOException,InterruptedException {
	    return value;
	}
	public long getPos() throws IOException {
	    return this.pos;
	}
	public float getProgress() throws IOException {
	    return this.pos / (float) this.split.getLength();
	}
	public byte[] doubleToRSerialized(double d){
	    byte[] b = new byte[4+4+8];
	    System.arraycopy(hdr,0,b,0,hdr.length);
	    long l = Double.doubleToRawLongBits(d);
	    for(int i=15;i>8;i--) {
		b[i] = (byte)(l);
		l >>>= 8;
	    }
	    b[8] = (byte)(l);
	    return(b);
	}
	public boolean nextKeyValue()
	    throws IOException,InterruptedException {
	      if (key == null) {
		  key = new RXWritableRAW();
	      }
	      if (value == null) {
		  value = new RXWritableRAW();
	      }
	      if (leftover == 0) return false;
	      long wi = pos + split.getStart();
	      try{
		  r = new REXPRaw( doubleToRSerialized((new Long(wi+1)).doubleValue()));
		  key.set(r);
		  value.set(r);
	      }catch(REXPMismatchException e){
		  e.printStackTrace();
		  throw new IOException(e);
	      }
	      pos ++; leftover --;
	      return true;
	}
    }
	
    public LApplyInputFormat() {}
	
    public List<InputSplit> getSplits(JobContext job) throws IOException {
	int n = job.getConfiguration().
	    getInt("rhipejob.lapply.lengthofinput",0);
	List<InputSplit> splits = new ArrayList<InputSplit>();
	int numSplits  = job.getConfiguration().getInt("mapred.map.tasks",0);
	int chunkSize ;
	if(n <= numSplits){
	    numSplits = n;
	    chunkSize = 1;
	}else{
	    chunkSize = n / numSplits;
	}
// 	System.out.println("Number of splits:"+numSplits+" chunkSize="+chunkSize);

	for (int i = 0; i < numSplits; i++) {
	    LApplyInputSplit split;
	    if ((i + 1) == numSplits)
		split = new LApplyInputSplit(i * chunkSize, n);
	    else
		split = new LApplyInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
	    splits.add(split);
	}
	return splits;
    }

    public RecordReader<RXWritableRAW, RXWritableRAW> createRecordReader(InputSplit split,TaskAttemptContext tac) throws IOException,InterruptedException {
	return new LApplyReader((LApplyInputSplit) split, tac);
    }


  
	
}

