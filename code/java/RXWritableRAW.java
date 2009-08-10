
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
import org.apache.hadoop.io.*;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RXWritableRAW extends BinaryComparable
    implements RXWritable,WritableComparable<BinaryComparable> {
  private static final Log LOG = LogFactory.getLog(RXWritableRAW.class);
  private static final int LENGTH_BYTES = 0;
  private static final byte[] EMPTY_BYTES = {};

  private int size;
  private byte[] bytes;
  public void set(REXP v) throws REXPMismatchException{
      byte[] v1 = ((REXPRaw)v).asBytes();
      set(v1);
  }
  public byte[] get(){
	byte[] x = new byte[getLength()];
	System.arraycopy(bytes,0,x,0,getLength());
	return x;
  }
  public void set(byte[] v){
      set(v,0,v.length);
  }
  public byte[] get_bytes(){
      return get();
  }

  public REXP getREXP(){
      REXP j = new REXPRaw(get());
      return j;
  }
  /**
   * Create a zero-size sequence.
   */
  public RXWritableRAW() {this(EMPTY_BYTES);}
  
  /**
   * Create a RXWritableRAW using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public RXWritableRAW(byte[] bytes) {
    this.bytes = bytes;
    this.size = bytes.length;
  }
  
  /**
   * Get the data from the RXWritableRAW.
   * @return The data is only valid between 0 and getLength() - 1.
   */
  public byte[] getBytes() {
    return bytes;
  }


  /**
   * Get the current size of the buffer.
   */
  public int getLength() {
    return size;
  }

  /**
   * Get the current size of the buffer.
   * @deprecated Use {@link #getLength()} instead.
   */
  @Deprecated
  public int getSize() {
    return getLength();
  }
  
  /**
   * Change the size of the buffer. The values in the old range are preserved
   * and any new values are undefined. The capacity is changed if it is 
   * necessary.
   * @param size The new number of bytes
   */
  public void setSize(int size) {
    if (size > getCapacity()) {
      setCapacity(size * 3 / 2);
    }
    this.size = size;
  }
  
  /**
   * Get the capacity, which is the maximum size that could handled without
   * resizing the backing storage.
   * @return The number of bytes
   */
  public int getCapacity() {
    return bytes.length;
  }
  
  /**
   * Change the capacity of the backing storage.
   * The data is preserved.
   * @param new_cap The new capacity in bytes.
   */
  public void setCapacity(int new_cap) {
    if (new_cap != getCapacity()) {
      byte[] new_data = new byte[new_cap];
      if (new_cap < size) {
        size = new_cap;
      }
      if (size != 0) {
        System.arraycopy(bytes, 0, new_data, 0, size);
      }
      bytes = new_data;
    }
  }

  /**
   * Set the RXWritableRAW to the contents of the given newData.
   * @param newData the value to set this RXWritableRAW to.
   */
  public void set(RXWritableRAW newData) {
    set(newData.bytes, 0, newData.size);
  }

  /**
   * Set the value to a copy of the given byte range
   * @param newData the new values to copy in
   * @param offset the offset in newData to start at
   * @param length the number of bytes to copy
   */
  public void set(byte[] newData, int offset, int length) {
    setSize(0);
    setSize(length);
    System.arraycopy(newData, offset, bytes, 0, size);
  }

  // inherit javadoc
  public void readFields(DataInput in) throws IOException {
    setSize(0); // clear the old data
    setSize(in.readInt());
    in.readFully(bytes, 0, size);
  }
  
  // inherit javadoc
  public void write(DataOutput out) throws IOException {
    out.writeInt(size);
    out.write(bytes, 0, size);
  }
  
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Are the two byte sequences equal?
   */
  public boolean equals(Object right_obj) {
    if (right_obj instanceof RXWritableRAW)
      return super.equals(right_obj);
    return false;
  }

  /**
   * Generate the stream of bytes as hex pairs separated by ' '.
   */
  public String toString() { 
    StringBuffer sb = new StringBuffer(3*size);
    for (int idx = 0; idx < size; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** A Comparator optimized for RXWritableRAW. */ 
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(RXWritableRAW.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
                          b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
    }
  }
  
  static {                                        // register this comparator
    WritableComparator.define(RXWritableRAW.class, new Comparator());
  }
  
}
