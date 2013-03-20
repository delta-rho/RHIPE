// /**
//  * Licensed to the Apache Software Foundation (ASF) under one
//  * or more contributor license agreements.  See the NOTICE file
//  * distributed with this work for additional information
//  * regarding copyright ownership.  The ASF licenses this file
//  * to you under the Apache License, Version 2.0 (the
//  * "License"); you may not use this file except in compliance
//  * with the License.  You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
package org.godhuli.rhipe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import org.godhuli.rhipe.REXPProtos.REXP;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;


public class RHWriter {
    protected static final Log LOG = LogFactory.getLog(RHWriter.class
						       .getName());

    private SequenceFile.Writer sqw;
    Configuration c;
    FileSystem f;
    String dest;
    int currentfile;
    int numperfile;
    int numwritten = 0;
    final RHBytesWritable anull = new RHBytesWritable( (new RHNull()).getRawBytes());
    public RHWriter(String output, int numper,PersonalServer s) throws Exception{
	dest= output;
	currentfile = 0;
	numperfile = numper;
	c = s.getConf();
	f = s.getFS();
	makeNewFile();
    }

    public void write(byte[] d,boolean b) throws Exception{
	try{
	    REXP elements =  REXP.newBuilder().mergeFrom(d, 0, d.length).build();
	    RHBytesWritable x = new RHBytesWritable();
	    RHBytesWritable y = new RHBytesWritable();
	    for(int i=0; i < elements.getRexpValueCount();i++){
		REXP ie= elements.getRexpValue(i);
		// LOG.info("Numwritten="+numwritten);
		if(numwritten >= numperfile){
		    close();
		    makeNewFile();
		}
		if(b){
		    x.set(ie.toByteArray());
		    sqw.append(anull,x);
		}else{
		    REXP ie2 = ie.getRexpValue(0);
		    x.set(ie2.getRexpValue(0).toByteArray());
		    y.set(ie2.getRexpValue(1).toByteArray());
		    sqw.append(x,y);
		}
		numwritten++;
	    }
	} catch(IOException e){
	    sqw.close();
	}
    }

    public void makeNewFile() throws IOException{
	currentfile ++;
	// LOG.info("New File for "+currentfile);
	sqw = new SequenceFile.Writer(f,c,new Path(dest+"/part_"+currentfile), RHBytesWritable.class, RHBytesWritable.class);
	numwritten = 0;
    }
    public void close() throws IOException{
	// LOG.info("Closed file corresponding to "+currentfile);
	if(sqw!=null) {
	    sqw.sync();
	    sqw.close();
	}
    }

}

