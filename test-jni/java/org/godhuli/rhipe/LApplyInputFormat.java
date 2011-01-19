/**
 * Copyright 2009 Saptarshi Guha
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;
import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Writable;


public class LApplyInputFormat extends 
				   InputFormat<RHNumeric, RHNumeric>
{
    protected static class LApplyReader extends 
	RecordReader<RHNumeric, RHNumeric> {
	private LApplyInputSplit split;
	private long leftover;
	private long pos = 0; 
	private RHNumeric key = null;
	private RHNumeric value = null;
	protected LApplyReader(LApplyInputSplit split, TaskAttemptContext tac) throws IOException{
	    this.split = split;
	    this.leftover = split.getLength();
	}
	public void initialize(InputSplit split,
			       TaskAttemptContext context)
	    throws IOException,
	    InterruptedException{}
	public void close()  throws IOException{ }
	public RHNumeric getCurrentKey() throws IOException,InterruptedException {
	    return key;	
	}
	public RHNumeric getCurrentValue() throws IOException,InterruptedException {
	    return value;
	}
	public long getPos() throws IOException {
	    return this.pos;
	}
	public float getProgress() throws IOException {
	    return ((float) this.pos) / (float) this.split.getLength();
	}

	public boolean nextKeyValue()
	    throws IOException,InterruptedException {
	      if (key == null) {
		  key = new RHNumeric();
	      }
	      if (value == null) {
		  value = new RHNumeric();
	      }
	      if (leftover == 0) return false;
	      long wi = pos + split.getStart();
	      key.setAndFinis(wi+1);
	      value.setAndFinis(wi+1);
	      pos ++; leftover --;
	      return true;
	}
    }
	
    public LApplyInputFormat() {}
	
    public List<InputSplit> getSplits(JobContext job) throws IOException {
	int n = job.getConfiguration().
	    getInt("rhipe_lapply_lengthofinput",0);
	List<InputSplit> splits = new ArrayList<InputSplit>();
	int numSplits  = job.getConfiguration().getInt("mapred.map.tasks",0);
	int chunkSize =0;
	// System.out.println("a)Number of splits:"+numSplits+" chunkSize="+chunkSize);
	// System.out.println("n="+n);

	if(n <= numSplits){
	    numSplits = n;
	    chunkSize = 1;
	}else{
	    chunkSize = n / numSplits;
	}
	// System.out.println("b)Number of splits:"+numSplits+" chunkSize="+chunkSize);

	for (int i = 0; i < numSplits; i++) {

	    LApplyInputSplit split;
	    if ((i + 1) == numSplits)
		split = new LApplyInputSplit(i * chunkSize, n);
	    else
		split = new LApplyInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
	    splits.add(split);
	}
	// System.out.println("Generated splits");
	return splits;
    }

    public RecordReader<RHNumeric, RHNumeric> createRecordReader(InputSplit split,TaskAttemptContext tac) throws IOException,InterruptedException {
	return new LApplyReader((LApplyInputSplit) split, tac);
    }

    
    protected static class LApplyInputSplit extends InputSplit implements Writable {
	private long end = 0;
	private long start = 0;
	public LApplyInputSplit() {
	    super();
	}
	/**
	 * Convenience Constructor
	 * @param start the index of the first row to select
	 * @param end the index of the last row to select (non-inclusive)
	 */
	public LApplyInputSplit(long start, long end) {
	    this.start = start;
	    this.end = end-1;
	}

	/**
	 * @return The index of the first row to select
	 */
	public long getStart() {
	    return start;
	}
			
	/**
	 * @return The index of the last row to select
	 */
	public long getEnd() {
	    return end;
	}
	/**
	 * @return The total row count in this split
	 */
	public long getLength() throws IOException {
	    return end - start + 1;
	}

	public String[] getLocations() {
	    return new String[] {  };
	}

	public void readFields(DataInput input) throws IOException {
	    start = input.readLong();
	    end = input.readLong();
	}
			
	public void write(DataOutput output) throws IOException {
	    output.writeLong(start);
	    output.writeLong(end);
	}

    }
  
	
}

