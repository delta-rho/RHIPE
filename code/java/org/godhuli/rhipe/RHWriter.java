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

import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.EOFException;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Calendar;
import java.net.URI;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;


public class RHWriter {
    private SequenceFile.Writer sqw;
    private byte[] keys=null;
    private byte[] values=null;
    private int num;
    private RHBytesWritable k,v;
    private RHBytesWritable NullKey;

    public RHWriter(){
	sqw = null;
	k=new RHBytesWritable();
	v=new RHBytesWritable();
	NullKey = new RHBytesWritable((new RHNull()).getRawBytes());
    }
	    
    public RHWriter(String name,Configuration cf) throws IOException{
	k=new RHBytesWritable();
	v=new RHBytesWritable();
	NullKey = new RHBytesWritable((new RHNull()).getRawBytes());	
	set(name,cf);
    }

    public void set(String name,Configuration cf) throws IOException {
	Path p = new Path(name);
	FileSystem fs = FileSystem.get(cf);
	sqw = new SequenceFile.Writer(fs,cf,p, RHBytesWritable.class,
				      RHBytesWritable.class);
    }
    
    public void setKeyArray(int n,byte[] keyarray,byte[] valarray){
	this.keys = keyarray;
	this.values = valarray;
	this.num = n;
    }

    public void doWrite() throws IOException{
	DataInputStream bkis = new DataInputStream(new ByteArrayInputStream(keys));
	DataInputStream bvis = new DataInputStream(new ByteArrayInputStream(values));
	for(int i=0;i<num;i++){
	    k.readFields(bkis);
	    // System.out.println(k);
	    v.readFields(bvis);
	    sqw.append(k,v);
	}
    }
    
    public void writeAValue(DataInputStream in) throws IOException {
	try{
	    v.readIntFields(in);
	    sqw.append(NullKey,v);
	}catch(EOFException e){}
    }
    public void doWriteFile(DataInputStream in,int count) throws IOException{
	for(int i=0;i<count;i++){
	    try{
		k.readIntFields(in);
		v.readIntFields(in);
		sqw.append(k,v);
	    }catch(EOFException e){}
	}
    }

    public void close() throws IOException{
	sqw.close();
    }
    public SequenceFile.Writer getSQW(){
	return sqw;
    }
}

