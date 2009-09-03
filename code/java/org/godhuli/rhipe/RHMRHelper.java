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

import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Properties;
import java.io.*;
import java.io.IOException;
import org.apache.hadoop.io.*;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RHMRHelper {
    private  static int BUFFER_SIZE = 10*1024;
    protected static final Log LOG = LogFactory.getLog(RHMRHelper.class.getName());
    public boolean copyFile;
    static private Environment env_;
    private String callID;

    public RHMRHelper(String fromWHo){
	callID=fromWHo;
    }
    void addEnvironment(Properties env, String nameVals) {
	if (nameVals == null) return;
	String[] nv = nameVals.split(" ");
	for (int i = 0; i < nv.length; i++) {
	    String[] pair = nv[i].split("=", 2);
	    if (pair.length != 2) {
	    } else {
		env.put(pair[0], pair[1]);
	    }
	}
    }
    void addJobConfToEnvironment(Configuration conf, Properties env) {
	Iterator it = conf.iterator();
	while (it.hasNext()) {
	    Map.Entry en = (Map.Entry) it.next();
	    String name = (String) en.getKey();
	    String value = conf.get(name); // does variable expansion 
	    env.put(name, value);
	}
    }
    void setup(Configuration cfg, String argv,boolean doPipe){     
	try {
	    BUFFER_SIZE = cfg.getInt("rhipe_stream_buffer",10*1024);
	    joinDelay_ = cfg.getLong("rhipe_joindelay_milli", 100);
	    nonZeroExitIsFailure_ = cfg.getBoolean("rhipe_non_zero_exit_is_failure", true);
	    doPipe_ = doPipe;
	    thisfs=FileSystem.get(cfg);
	    outputFolder = new Path(cfg.get("rhipe_output_folder"));
	    if(!doPipe_) return;
	    copyFile=cfg.get("rhipe_copy_file").equals("TRUE")? true: false;
	    String[] argvSplit = argv.split(" ");
	    String prog = argvSplit[0];
	    Environment childEnv = (Environment) env().clone();
	    addJobConfToEnvironment(cfg, childEnv);
	    childEnv.put( "TMPDIR", System.getProperty("java.io.tmpdir"));
	    // Start the process
	    ProcessBuilder builder = new ProcessBuilder(argvSplit);
	    builder.environment().putAll(childEnv.toMap());
	    sim = builder.start();
	    clientOut_=new DataOutputStream(new BufferedOutputStream(
				                  sim.getOutputStream(),
						  BUFFER_SIZE));
	    clientIn_ =new DataInputStream(new BufferedInputStream(
						sim.getInputStream(),
						BUFFER_SIZE));
	    clientErr_ = new DataInputStream(new BufferedInputStream(sim.getErrorStream()));
	    startTime_ = System.currentTimeMillis();
	    LOG.info(callID+":"+"Started external program:"+argv);
	    errThread_ = new MRErrorThread();
	    LOG.info(callID+":"+"Started Error Thread");
	    errThread_.start();
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new RuntimeException("configuration exception", e);
	}
    }


    void startOutputThreads(TaskInputOutputContext<RHBytesWritable,RHBytesWritable,
			    RHBytesWritable,RHBytesWritable> ctx) {
	outThread_ = new MROutputThread(ctx,true);
	outThread_.start();
	errThread_.setContext(ctx);
	LOG.info(callID+":"+"Started Output Thread");
    }
    
 

    public void mapRedFinished(TaskInputOutputContext<RHBytesWritable,RHBytesWritable,
			       RHBytesWritable,RHBytesWritable> ctx) {
	try {
	    if (!doPipe_) {
		return;
	    }
	    try {
		if (clientOut_ != null) {
		    clientOut_.flush();
		    clientOut_.close();
		}
	    } catch (IOException io) {
	    }
	    
	    waitOutputThreads(ctx);
	    if (sim != null) sim.destroy();
	} catch (RuntimeException e) {
	    e.printStackTrace();
	    throw e;
	}
    }

	
    
    void waitOutputThreads(TaskInputOutputContext<RHBytesWritable,RHBytesWritable
			   ,RHBytesWritable,RHBytesWritable> ctx) {
	try {
	    if (outThread_ == null) {
		startOutputThreads(new DummyContext(ctx)); //will fail
	    }
	    int exitVal = sim.waitFor();
	    if (exitVal != 0) {
		if (nonZeroExitIsFailure_) {
		    throw new RuntimeException("RHMRMapRed.waitOutputThreads(): subprocess failed with code "
					       + exitVal);
		} 
	    }
	    if (outThread_ != null) 
		outThread_.join(joinDelay_);
	    if (errThread_ != null) 
		errThread_.join(joinDelay_);
	} catch (InterruptedException e) {
	}
    }
    

    public void checkOuterrThreadsThrowable() throws IOException{
	if (outerrThreadsThrowable != null) {
	    throw new IOException ("MROutput/MRErrThread failed:"
				   +
				   StringUtils.stringifyException(outerrThreadsThrowable));
	}
    }
    
    public String getSimExitInfo() throws IllegalThreadStateException{
	String extraInfo="";
	int exitVal = sim.exitValue();
	if (exitVal == 0) {
	    extraInfo = "subprocess exited successfully\n";
	} else {
	    extraInfo = "subprocess exited with error code " + exitVal + "\n";
	};
	return(extraInfo);
    }

    class DummyContext extends TaskInputOutputContext<RHBytesWritable,RHBytesWritable,
			       RHBytesWritable,RHBytesWritable> {
	DummyContext(TaskInputOutputContext<RHBytesWritable,RHBytesWritable,
		     RHBytesWritable,RHBytesWritable>ctx){
	    super(null, null, null, null, null); //wont work
	}
	public RHBytesWritable getCurrentKey() throws IOException, InterruptedException {
	    return null;
	}
	
	public RHBytesWritable getCurrentValue() throws IOException, InterruptedException {
	    return null;
	}
	public boolean nextKeyValue() throws IOException, InterruptedException {
	    return false;
	}
	public void write(RHBytesWritable key, RHBytesWritable value
			  ) throws IOException, InterruptedException {
	}
	public void setStatus(String status){}
	public void progress(){}
    }

    public void writeCMD(int s) throws IOException{
	WritableUtils.writeVInt(clientOut_,s);
    }
	
    public void write(RHBytesWritable c) throws IOException{
	c.write(clientOut_);
    }

	
 
    class MROutputThread extends Thread {
	volatile TaskInputOutputContext <RHBytesWritable,RHBytesWritable,
	    RHBytesWritable,RHBytesWritable> ctx;
	long lastStdoutReport = 0;

	MROutputThread(TaskInputOutputContext<RHBytesWritable,RHBytesWritable,
		       RHBytesWritable,RHBytesWritable> ctx,boolean isD) {
	    setDaemon(isD);
	    this.ctx = ctx;
	}
	boolean readRecord(RHBytesWritable k, RHBytesWritable v) {
	    try{
		k.readFields(clientIn_);
		v.readFields(clientIn_);
	
	    }catch(IOException e){
		return(false);
	    }
	    return(true);
	}

	public void run() {
	    RHBytesWritable key = new RHBytesWritable();
	    RHBytesWritable value = new RHBytesWritable();
	    try {
		while (readRecord(key,value) ) {
		    // LOG.info("KEY="+key.toDebugString());
		    // LOG.info("Value="+value.toDebugString());
		    ctx.write(key,value);
		    numRecWritten_++;
		    long now = System.currentTimeMillis();
		    if (now-lastStdoutReport > reporterOutDelay_) {
			lastStdoutReport = now;
			ctx.setStatus("R/W merrily moving along: W="+numRecWritten_+" ");
		    }
		}
		if (clientIn_ != null) {
		    clientIn_.close();
		    clientIn_ = null;
		    LOG.info(callID+":"+"MROutputThread done");
		}
	    }catch(EOFException e){
		LOG.info("Acchoo");
	    }catch (Throwable th) {
		outerrThreadsThrowable = th;
		LOG.warn(callID+":"+StringUtils.stringifyException(th));
		try {
		    if (clientIn_ != null) {
			clientIn_.close();
			clientIn_ = null;
		    }
		} catch (IOException io) {
		LOG.info(StringUtils.stringifyException(io));
		}
	    }
	}
    }


    class MRErrorThread extends Thread {
	long lastStderrReport = 0;
	volatile TaskInputOutputContext ctx;
	public MRErrorThread() {
	    setDaemon(true);
	}
	public void setContext(TaskInputOutputContext ctx) {
	    this.ctx = ctx;
	}
	public void run() {
	    try {
		try{
		    int ln;
		    byte[] k;
		    while(true){
			int cmd = clientErr_.readByte();
			switch(cmd){
			case RHTypes.ERROR_MSG:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String errmsg = new String(k);
			    throw new Exception("\nR ERROR\n=======\n"+errmsg);
			case RHTypes.PRINT_MSG:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String pmsg = new String(k);
			    System.out.println(pmsg);
			    break;
			case RHTypes.SET_STATUS:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String status = new String(k);
			    ctx.setStatus(status);
			    break;
			case RHTypes.SET_COUNTER:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String grcnt = new String(k);
			    String[] columns = grcnt.split(",");
			    ctx.getCounter(columns[0], columns[1])
				.increment(Long.parseLong(columns[2]));
			    break;
			}
			long now = System.currentTimeMillis(); 
			if ( now-lastStderrReport > reporterErrDelay_) {
			    lastStderrReport = now;
			    ctx.progress();
			}
		    }
		}catch(EOFException e){
		    if (clientErr_ != null) {
			clientErr_.close();
			clientErr_ = null;
			
			LOG.info(callID+":"+"MRErrorThread done");
		    }
		}
	    } catch (Throwable th) {
		outerrThreadsThrowable = th;
		LOG.warn(callID+":"+StringUtils.stringifyException(th));
		try {
		    if (clientErr_ != null) {
			clientErr_.close();
			clientErr_ = null;
		    }
		} catch (IOException io) {
		    LOG.info(callID+":"+StringUtils.stringifyException(io));
		}
	    }
	}
    }

    static Environment env() {
	if (env_ != null) {
	    return env_;
	}
	try {
	    env_ = new Environment();
	} catch (IOException io) {
	    io.printStackTrace();
        }
	return env_;
    }

    static String asHex(byte[] v, int max){
	StringBuffer sb = new StringBuffer(3*v.length);
	for (int idx = 0; idx < Math.min(max,v.length); idx++) {
	    if (idx != 0) sb.append(' ');
	    String num = Integer.toHexString(v[idx]);
	    if (num.length() < 2) sb.append('0');
	    sb.append(num);
	}
	if(max<v.length) sb.append(" ... ");
	return sb.toString();
    }
    
    public void copyFiles(String dirfrom) throws IOException{
	if(copyFile){
	    File dirf = new File(dirfrom);
	    ArrayList<Path> lop = new ArrayList<Path>();
	    for(File ff :  dirf.listFiles()){
		if( ff.isFile() && ff.length()>0)
		    lop.add(new Path(ff.toString()));
	    }
	    if (lop.size()>0) thisfs.copyFromLocalFile(false,true,lop.toArray(new Path[]{}), outputFolder);
	}
    }


    
    long startTime_;
    long numRecWritten_ = 0;
    boolean copyFile_;
    long reporterOutDelay_ = 10*1000L; 
    long reporterErrDelay_ = 10*1000L; 
    long joinDelay_;
    
    boolean doPipe_;

    
    boolean nonZeroExitIsFailure_;
    FileSystem thisfs;
    Path outputFolder;
    Process sim;
    MROutputThread outThread_;
    MRErrorThread errThread_;
    volatile DataOutputStream clientOut_;
    volatile DataInputStream clientErr_;
    volatile DataInputStream clientIn_;
    
    
    protected volatile Throwable outerrThreadsThrowable;
}