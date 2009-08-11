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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;


public class LApplyInputFormat implements InputFormat<RXWritableLong, RXWritableRSV>, JobConfigurable
{
    public static final Log LOG = LogFactory.getLog("org.apache.hadoop.mapred.TaskTracker");
    protected static class LApplyReader implements RecordReader<RXWritableLong, RXWritableRSV> {
	private JobConf job;
	private LApplyInputSplit split;
	private long leftover;
	private long pos = 0; 
	private int needlist=0;
	private RList inputlist;
	private RConnection rc;
	protected LApplyReader(LApplyInputSplit split, JobConf job) throws IOException{
	    try{
		rc=new RConnection("127.0.0.1",job.getInt("rhipejob.rport",8888));
		byte[] inputlistb = null ;
		this.needlist = job.getInt("rhipejob.lapply.listneeded",0);
		if(this.needlist == 0) {
		    //Because a list.object was given.
		    uniWritable b= new uniWritable();
		    try{
			MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(job),job.get("rhipejob.lapply.mapfile"),job);
			inputlistb =( (uniWritable)mrd.get(new Text("rhipejob.lapply.inputlist"), b)).getBytes();
			mrd.close();
		    }catch(Exception e){
			e.printStackTrace();
			throw new IOException("MapFile Error");
		    }
// 		    if(inputlistb == null) throw new IOException("[RHIPE]:Serialized inputlist bytes is null");
// 		    REXPRaw rr = new REXPRaw(inputlistb);
// 		    rc.assign("..inputlistb..",rr);
// 		    REXP rcx = rc.eval("unserialize(..inputlistb..)");
// 		    inputlist=rcx.asList();
		}
		this.split = split;
		this.leftover = split.getLength();
// 	    }catch (REXPMismatchException e) {
// 		e.printStackTrace();
// 		throw new IOException(e);
	    }catch (RserveException e) {
		e.printStackTrace();
		throw new IOException(e);
	    }catch(Exception e){ e.printStackTrace(); throw new IOException(e);}
	}
	public void close()  throws IOException{ }
	public RXWritableLong createKey() {
	    return new RXWritableLong();	
	}
	public RXWritableRSV createValue() {
	    return new RXWritableRSV();
	}
	public long getPos() throws IOException {
	    return pos;
	}
	public float getProgress() throws IOException {
	    return pos / (float) split.getLength();
	}
	public boolean next(RXWritableLong key, RXWritableRSV value) throws IOException {
	    try{
		if (leftover == 0) return false;
		long wi = pos + split.getStart();
		key.set(wi);
		if(false){ // needlist==0){
		    int where1=(new Long(wi)).intValue();
		    value.set(inputlist.at(where1));
		}else{
		    value.set(new REXPDouble(new double[]{ (new Long(wi+1)).doubleValue() }));
		}
		pos ++; leftover --;
		return true;
	    }catch(REXPMismatchException e){ throw new IOException(e);}
	}
    }
	
    public LApplyInputFormat() {}

    public void validateInput(JobConf job) {}

    public void configure(JobConf job){

    }
	
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
	long n = job.getInt("rhipejob.lapply.lengthofinput",0)*1L;
	long chunkSize = n / (numSplits == 0 ? 1 : numSplits);
// 		System.out.println("NumSplits="+numSplits);	
	InputSplit[] splits = new InputSplit[numSplits];
	for (int i = 0; i < numSplits; i++) {
	    LApplyInputSplit split;
	    if ((i + 1) == numSplits)
		split = new LApplyInputSplit(i * chunkSize, n);
	    else
		split = new LApplyInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
	    splits[i] = split;
	}
	return splits;
    }

    public RecordReader<RXWritableLong, RXWritableRSV> getRecordReader(InputSplit split,JobConf job, Reporter reporter) throws IOException {
	return new LApplyReader((LApplyInputSplit) split, job);
    }


    protected static class LApplyInputSplit implements InputSplit {
	private long end = 0;
	private long start = 0;
	public LApplyInputSplit() {
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
	    return new String[] { };
	}

	/** {@inheritDoc} */
	public void readFields(DataInput input) throws IOException {
	    start = input.readLong();
	    end = input.readLong();
	}
			
	/** {@inheritDoc} */
	public void write(DataOutput output) throws IOException {
	    output.writeLong(start);
	    output.writeLong(end);
	}

    }
	
}

