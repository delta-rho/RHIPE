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
import org.apache.hadoop.io.Writable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;

public class LApplyInputSplit extends InputSplit implements Writable {
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