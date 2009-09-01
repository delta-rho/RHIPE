/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class RHBytesWritable
    implements WritableComparable<RHBytesWritable> {
  protected byte[] bytes;
  protected int offset;
  protected int length;
  protected REXP rexp;
  public static String fieldSep=" ";
  

  public RHBytesWritable() {
    super();
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
  
  public static void setFieldSep(String s){
      fieldSep=s;
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
  
  public void intoREXP() throws com.google.protobuf.InvalidProtocolBufferException{
      if(bytes==null){
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
      }
      rexp =  REXP.parseFrom(bytes);
  }

  public REXP getREXP(){
      return(rexp);
  }

  public String toDebugString() throws com.google.protobuf.InvalidProtocolBufferException{
      intoREXP();
      return(rexp.toString());
  }


  public void readFields(final DataInput in) throws IOException {
    this.length = WritableUtils.readVInt(in);
    // System.out.println("Read bytes="+this.length);
    this.bytes = new byte[this.length];
    in.readFully(this.bytes, 0, this.length);
    this.offset = 0;
  }
  
  public void write(final DataOutput out) throws IOException {
      WritableUtils.writeVInt(out,this.length);
      out.write(this.bytes, this.offset, this.length);
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

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() { 
    StringBuffer sb = new StringBuffer(3*this.bytes.length);
    for (int idx = 0; idx < this.bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
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
  

  public static byte [][] toArray(final List<byte []> array) {
    // List#toArray doesn't work on lists of byte [].
    byte[][] results = new byte[array.size()][];
    for (int i = 0; i < array.size(); i++) {
      results[i] = array.get(i);
    }
    return results;
  }

  public String writeAsString() throws IOException{
      try{
	  intoREXP();
	  return(writeAsString(rexp));
      }catch( com.google.protobuf.InvalidProtocolBufferException e){
	  throw new IOException(e);
      }
  }
  private String writeAsString(REXP r) {
      StringBuilder sb = new StringBuilder();
      int i;
      REXP.RClass clz = r.getRclass();
      switch(clz){
      case STRING:
	  org.godhuli.rhipe.REXPProtos.STRING ss;
	  for(i=0;i<r.getStringValueCount()-1;i++){
	      ss = r.getStringValue(i);
	      if(ss.getIsNA()) sb.append("NA");
	      else{
		  sb.append("\"");sb.append(ss.getStrval());sb.append("\"");
	      }
	      sb.append(fieldSep);
		    
	  }
	  ss = r.getStringValue(i);
	  if(ss.getIsNA()) sb.append("NA");
	  else{
	      sb.append("\"");sb.append(ss.getStrval());sb.append("\"");
	  }
	  break;
      case RAW:
	  String num;
	  com.google.protobuf.ByteString bs = r.getRawValue();
	  for(i=0;i<bs.size()-1;i++){
	      num = Integer.toHexString(bs.byteAt(i));
	      if (num.length() < 2) sb.append('0');
	      sb.append(num);sb.append(fieldSep);
	  }
	  num = Integer.toHexString(bs.byteAt(i));
	  if (num.length() < 2) sb.append('0');
	  sb.append(num);
	  break;
      case REAL:
	  for(i=0;i< r.getRealValueCount()-1;i++){
	      sb.append(r.getRealValue(i));
	      sb.append(fieldSep);
	  }
	  sb.append(r.getRealValue(i));
	  break;
      case COMPLEX:
	  CMPLX cp;
	  for(i=0;i< r.getComplexValueCount()-1;i++){
	      cp = r.getComplexValue(i);
	      sb.append(cp.getReal());sb.append("+");
	      sb.append(cp.getImag());
	      sb.append(fieldSep);
	  }
	  cp = r.getComplexValue(i);
	  sb.append(cp.getReal());sb.append("+");
	  sb.append(cp.getImag());
	  break;
      case INTEGER:
	  for(i=0;i< r.getIntValueCount()-1;i++){
	      sb.append(r.getIntValue(i));
	      sb.append(fieldSep);
	  }
	  sb.append(r.getIntValue(i));
	  break;
      case LIST:
	  for(i=0;i< r.getRexpValueCount()-1;i++){
	      sb.append( writeAsString( r.getRexpValue(i)));
	      sb.append(fieldSep);
	  }
	  sb.append( writeAsString(r.getRexpValue(i)));
	  break;
      case LOGICAL:
	  for(i=0;i< r.getBooleanValueCount()-1;i++){
	      sb.append(r.getBooleanValue(i));
	      sb.append(fieldSep);
	  }
	  sb.append(r.getBooleanValue(i));
	  break;
      case NULLTYPE:
	  sb.append("NULL");
	  break;

      }
      return(sb.toString());
      
  }
  

}
