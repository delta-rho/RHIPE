/**
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
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;


public class RHReader {
    private SequenceFile.Reader sqr;
    private byte[] keys=null;
    private byte[] values=null;
    private int max,numread;
    private RHBytesWritable k,v;
    private ByteArrayOutputStream bos;
    // private List<Integer> szlist;
    public RHReader(){
	sqr = null;
	List<Integer> intlist = new ArrayList<Integer>();
	bos = new ByteArrayOutputStream();
    }
	    

    public void set(String name,Configuration cf) throws IOException {
	Path p = new Path(name);
	FileSystem fs = FileSystem.get(cf);
	sqr = new SequenceFile.Reader(fs,p,cf);
    }
    
    public int readKVByteArrayHow(int n,boolean how) throws IOException{
	bos.reset();
	// szlist.clear();
	DataOutputStream dos = new DataOutputStream(bos);
	int i =0;
	boolean b;
	k=new RHBytesWritable();
	v=new RHBytesWritable();
	while( n<0 || i< n ){
	    b=sqr.next((Writable)k,(Writable)v);
	    if(!b) {
		return(i);
	    }else{
		i=i+1;
		if(how){
		    k.write(dos); v.write(dos);
		}else{
		    k.writeAsInt(dos); v.writeAsInt(dos);
		}
		// szlist.add(new Integer(k.getLength()));
		// szlist.add(new Integer(v.getLength()));
	    }
	}
	return(i);
    }
    
    public int readKVByteArray(int n) throws IOException{
	return( readKVByteArrayHow(n,false));
    }

    // public int[] getSizes(){
    // 	int[] intArray = new int[szlist.size()];
    // 	for (int i = 0; i < szlist.size(); i++) {
    // 	    intArray[i] = szlist.get(i);
    // 	}
    // 	return(intArray);
    // }

    public byte[] getKVByteArray(){
	byte[] kv = bos.toByteArray();
	return(kv);
    }

    public void close() throws IOException{
	sqr.close();
    }
}
