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
import org.apache.hadoop.io.DoubleWritable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;


public class RXWritableDouble implements WritableComparable,RXWritable {
    private double value = 0.0;
    public RXWritableDouble() { super(); }
    public void set(REXP v) throws REXPMismatchException{
	set( ((REXPDouble)v).asDouble());
    }
    public REXP getREXP() throws IOException{
	try{
	    return(new REXPDouble(new double[]{get()}));
	}catch(Exception e) { throw new IOException(e);}
    }

    public RXWritableDouble(double value) {
	set(value);
    }
    
    public void readFields(DataInput in) throws IOException {
	value = in.readDouble();
    }
    
    public void write(DataOutput out) throws IOException {
	out.writeDouble(value);
    }
    
    public void set(double value) { this.value = value; }
    
    public double get() { return value; }
    
    public boolean equals(Object o) {
	if (!(o instanceof RXWritableDouble)) {
	    return false;
	}
	RXWritableDouble other = (RXWritableDouble)o;
	return this.value == other.value;
    }
    
    public int hashCode() {
	return (int)Double.doubleToLongBits(value);
    }
    
    public int compareTo(Object o) {
	RXWritableDouble other = (RXWritableDouble)o;
	return (value < other.value ? -1 : (value == other.value ? 0 : 1));
    }
    
    public String toString() {
	return Double.toString(value);
    }
    
    /** A Comparator optimized for RXWritableDouble. */ 
    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(RXWritableDouble.class);
	}
	
	public int compare(byte[] b1, int s1, int l1,
			   byte[] b2, int s2, int l2) {
	    double thisValue = readDouble(b1, s1);
	    double thatValue = readDouble(b2, s2);
	    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
	}
    }
    
    static {                                        // register this comparator
	WritableComparator.define(RXWritableDouble.class, new RXWritableDouble.Comparator());
    }
    
}
