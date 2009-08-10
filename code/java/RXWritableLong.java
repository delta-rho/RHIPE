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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

public class RXWritableLong implements WritableComparable,RXWritable{ 
    public RXWritableLong() { ;}
    private long value;
    public void set(REXP v) throws REXPMismatchException{
	set((new Double(((REXPDouble)v).asDouble())).longValue());
    }
    public REXP getREXP() throws IOException {
	try{
	    return( new REXPDouble(new double[]{(double)get()}));
	}catch(Exception e){ throw new IOException(e);}
    }

    public void set(long value) { this.value = value; }
    public long get() { return value; }
    public void readFields(DataInput in) throws IOException {
	value = in.readLong();
    }
    public void write(DataOutput out) throws IOException {
	out.writeLong(value);
    }
    public boolean equals(Object o) {
	if (!(o instanceof RXWritableLong))
	    return false;
	RXWritableLong other = (RXWritableLong)o;
	return this.value == other.value;
    }
    
    public int hashCode() {
	return (int)value;
    }

    public int compareTo(Object o) {
	long thisValue = this.value;
	long thatValue = ((RXWritableLong)o).value;
	return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
    public String toString() {
	return Long.toString(value);
    }
    
    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(RXWritableLong.class);
	}
	
	public int compare(byte[] b1, int s1, int l1,
			   byte[] b2, int s2, int l2) {
	    long thisValue = readLong(b1, s1);
	    long thatValue = readLong(b2, s2);
	    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}
  }
    
    public static class DecreasingComparator extends Comparator {
	public int compare(WritableComparable a, WritableComparable b) {
	    return -super.compare(a, b);
	}
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    return -super.compare(b1, s1, l1, b2, s2, l2);
	}
    }
    
    static {                                       // register default comparator
	WritableComparator.define(RXWritableLong.class, new Comparator());
    }
    
}
