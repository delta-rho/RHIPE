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
		
    int length;
    int offset;
    private static final int LENGTH_BYTES = 0 ; // 4;
    private static final byte[] EMPTY_BYTES = {};
		
    int size;
    byte[] bytes;
		
		
    public RHBytesWritable() {
	this(EMPTY_BYTES);
    }
		
    public RHBytesWritable(byte[] bytes) {
	this(bytes, 0, bytes.length);
    }
		
    public RHBytesWritable(final byte[] bytes, final int offset,
			   final int length) {
	this.bytes = bytes;
	this.offset = offset;
	this.length = length;
    }
		
		
    public RHBytesWritable(final RHBytesWritable ibw) {
	this(ibw.get(), 0, ibw.getSize());
    }
		

    public byte [] get() {
	if (this.bytes == null) {
	    throw new IllegalStateException("Uninitialiized. Null constructor " +
					    "called w/o accompaying readFields invocation");
	}
	return this.bytes;
    }
		
		
    public void set(final byte [] b) {
	set(b, 0, b.length);
    }
		
    public byte []  getBytes(){
	return(this.bytes);
    }
		
		
    public void set(final byte [] b, final int offset, final int length) {
	this.bytes = b;
	this.offset = offset;
	this.length = length;
    }
		
    public int getSize() {
	if (this.bytes == null) {
	    throw new IllegalStateException("Uninitialiized. Null constructor " +
					    "called w/o accompaying readFields invocation");
	}
	return this.length;
    }
		
		
		
    public int getLength() {
	if (this.bytes == null) {
	    throw new IllegalStateException("Uninitialiized. Null constructor " +
					    "called w/o accompaying readFields invocation");
	}
	return this.length;
    }
		
		
		
		
		
		
    // Below methods copied from BytesWritable
		
    @Override
	public int hashCode() {
	return WritableComparator.hashBytes(bytes, this.length);
    }
		
    /**
     * Define the sort order of the BytesWritable.
     * @param right_obj The other bytes writable
     * @return Positive if left is bigger than right, 0 if they are equal, and
     *         negative if left is smaller than right.
     */
    public int compareTo(RHBytesWritable right_obj) {
	return compareTo(right_obj.get());
    }
		
    /**
     * Compares the bytes in this object to the specified byte array
     * @param that
     * @return Positive if left is bigger than right, 0 if they are equal, and
     *         negative if left is smaller than right.
     */
    public int compareTo(final byte [] that) {
	int diff = this.length - that.length;
	return (diff != 0)?
	    diff:
	    WritableComparator.compareBytes(this.bytes, 0, this.length, that,
					    0, that.length);
    }
		
    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
	public boolean equals(Object right_obj) {
	if (right_obj instanceof byte []) {
	    return compareTo((byte [])right_obj) == 0;
	}
	if (right_obj instanceof RHBytesWritable) {
	    return compareTo((RHBytesWritable)right_obj) == 0;
	}
	return false;
    }
		
		
		
    /** A Comparator optimized for RXImmutableBytesWritable.
     */ 
    public static class Comparator extends WritableComparator {
	private BytesWritable.Comparator comparator =
	    new BytesWritable.Comparator();
			
	/** constructor */
	public Comparator() {
	    super(RHBytesWritable.class);
	}
			
	/**
	 * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
	 */
	@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    return comparator.compare(b1, s1, l1, b2, s2, l2);
	}
    }
		
    static { // register this comparator
	WritableComparator.define(RHBytesWritable.class, new Comparator());
    }
		
		
		
    public void readFields(final DataInput in) throws IOException {
	this.length = readVInt(in);
	// this.length = in.readInt();
	this.bytes = new byte[this.length];
	in.readFully(this.bytes, 0, this.length);
	this.offset = 0;
    }
		
    public void write(final DataOutput out) throws IOException {
	WritableUtils.writeVInt(out,this.length);
	// out.writeInt(this.length);
	out.write(this.bytes, this.offset, this.length);
    }

    public void writeAsInt(final DataOutput out) throws IOException {
	out.writeInt(this.length);
	// out.writeInt(this.length);
	out.write(this.bytes, this.offset, this.length);
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
	StringBuffer sb = new StringBuffer(3*this.bytes.length);
	for (int idx = 0; idx < this.bytes.length; idx++) {
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
	String s =  REXPHelper.toString(bytes);
	return(s);
	// return(toByteString());
    }
		
		
		
}
