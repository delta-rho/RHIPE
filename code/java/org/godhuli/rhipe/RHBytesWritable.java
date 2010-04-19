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
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.CMPLX;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class RHBytesWritable
    implements WritableComparable<RHBytesWritable> {
    public static String fieldSep=" ";
		
    int bytelength;
    // int offset;
    private static final int LENGTH_BYTES = 0 ; // 4;
    private static final byte[] EMPTY_BYTES = {};
		
    byte[] bytes;
		
		
    public RHBytesWritable() {
	this(EMPTY_BYTES);
    }
		
    public RHBytesWritable(byte[] bytes) {
	this(bytes, bytes.length);
    }
		
    public RHBytesWritable(final byte[] bytes, 
			   final int length) {
	this.bytes = bytes;
	// this.offset = offset;
	this.bytelength = length;
    }
    public RHBytesWritable(final RHBytesWritable ibw) {
	this(ibw.getBytes(),  ibw.getSize());
    }
    public void set(final byte [] b) {
	set(b, b.length);
    }
    public byte []  getBytes(){
	return(this.bytes);
    }
    // public void set(final byte [] b, final int length) {
    // 	this.bytes = b;
    // 	this.bytelength = length;
    // }
    public void set(byte[] newData,  int length) {
    	setSize(0);
    	setSize(length);
    	System.arraycopy(newData,0, bytes, 0, bytelength);
    }

    public void setSize(int size) {
	if (size > getCapacity()) {
	    setCapacity(size * 3 / 2);
	}
	this.bytelength = size;
    }
    public void setCapacity(int new_cap) {
	if (new_cap != getCapacity()) {
	    byte[] new_data = new byte[new_cap];
	    if (new_cap < this.bytelength) {
		bytelength = new_cap;
	    }
	    if (bytelength != 0) {
		System.arraycopy(bytes, 0, new_data, 0, bytelength);
	    }
	    bytes = new_data;
	}
    }
    public int getSize() {
	return this.bytelength;
    }
    public int getCapacity() {
	return bytes.length;
    }
    public int hashCode() {
	return WritableComparator.hashBytes(bytes, this.bytelength);
    }
		
    public int compareTo(RHBytesWritable right_obj) {
	return compareTo(right_obj.getBytes());
    }
    public int compareTo(final byte [] that) {
	   return WritableComparator.compareBytes(this.bytes, 0, this.bytes.length, that,
					    0, that.length);
    }
    public boolean equals(Object right_obj) {
	// I changed this in 0.59!
	// if (right_obj instanceof byte []) {
	//     return compareTo((byte [])right_obj) == 0;
	// }
	// if (right_obj instanceof RHBytesWritable) {
	    return compareTo((RHBytesWritable)right_obj) == 0;
	// }
	// return false;
    }
    public static class Comparator extends WritableComparator {
	private BytesWritable.Comparator comparator =
	    new BytesWritable.Comparator();
			
	/** constructor */
	public Comparator() {
	    super(RHBytesWritable.class);
	}
	
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    return comparator.compare(b1, s1, l1, b2, s2, l2);
	}
    }
    static { // register this comparator
	WritableComparator.define(RHBytesWritable.class, new Comparator());
    }
    // public void readFields(final DataInput in) throws IOException {
    // 	this.bytelength = readVInt(in);
    // 	this.bytes = new byte[this.bytelength];
    // 	in.readFully(this.bytes, 0, this.bytelength);
    // }
    public void readFields(final DataInput in) throws IOException {
	// System.out.print("Old length="+bytelength);
    	setSize(0); // clear the old data
    	setSize(readVInt(in));
	// System.out.println(" new Length= "+bytelength+" of "+getCapacity());
    	in.readFully(this.bytes, 0, bytelength);
    }
    // public void readIntFields(final DataInput in) throws IOException {
    // 	this.bytelength = in.readInt();
    // 	this.bytes = new byte[this.bytelength];
    // 	in.readFully(this.bytes, 0, this.bytelength);
    // }
    public void readIntFields(final DataInput in) throws IOException {
    	setSize(0); // clear the old data
    	setSize(in.readInt());
    	in.readFully(bytes, 0, bytelength);
    }
	
    public void write(final DataOutput out) throws IOException {
	WritableUtils.writeVInt(out,this.bytelength);
	// out.writeInt(this.length);
	out.write(this.bytes, 0, this.bytelength);
    }

    public void writeAsInt(final DataOutput out) throws IOException {
	out.writeInt(this.bytelength);
	// out.writeInt(this.length);
	out.write(this.bytes, 0, this.bytelength);
    }

    public static boolean isNegativeVInt(byte value) {
	return value < -120 || (value >= -112 && value < 0);
    }

    public static int decodeVIntSize(byte value) {
	if (value >= -112) {
	    return 1;
	} else if (value < -120) {
	    return -119 - value;
	}
	return -111 - value;
    }

    public static int readVInt(DataInput stream) throws IOException {
	byte firstByte = stream.readByte();
	// System.out.println("readVInt: Got FB ="+firstByte);
	int len = decodeVIntSize(firstByte);
	// System.out.println("readVInt: length="+len);
	if (len == 1) {
	    return firstByte;
	}
	long i = 0;
	for (int idx = 0; idx < len-1; idx++) {
	    byte b = stream.readByte();
	    // System.out.println("readVInt: Got ="+b);

	    i = i << 8;
	    i = i | (b & 0xFF);
	}
	return (int) ((isNegativeVInt(firstByte) ? (i ^ -1L) : i));
    }
    
		
    public String toByteString() { 
	StringBuffer sb = new StringBuffer(3*this.bytelength);
	for (int idx = 0; idx < this.bytelength; idx++) {
	    // if not the first, put a blank separator in
	    if (idx != 0) {
		sb.append(" 0x");
	    }else sb.append("0x");
	    String num = Integer.toHexString(0xff & bytes[idx]);
	    // if it is only one digit, add a leading 0.
	    if (num.length() < 2) {
		sb.append('0');
	    }
	    sb.append(num);
	}
	return sb.toString();
    }
		
		
		

		
    public String toString() {
	String s =  REXPHelper.toString(bytes,0,this.bytelength);
	return(s);
	// return(toByteString());
    }
		
		
		
}
