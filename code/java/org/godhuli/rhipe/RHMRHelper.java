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

import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Properties;
import java.io.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import java.net.InetAddress;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;

import java.lang.reflect.*;

public class RHMRHelper {
    private  static int BUFFER_SIZE = 10*1024;
    private  static final String R_MAP_ERROR = "R MAP ERROR";
    private  static final String R_REDUCE_ERROR = "R REDUCE ERROR";
    protected static final Log LOG = LogFactory.getLog(RHMRHelper.class.getName());
    public boolean copyFile;
    static private Environment env_;
    private String callID;
    private String hostname;
    private RHMRMapper mapper;
    protected static int PARTITION_START=0, PARTITION_END=0;
    protected static REXP.RClass PARTITION_TYPE=REXP.RClass.REAL;
    public RHMRHelper(String fromWHo,RHMRMapper m){
	callID=fromWHo;
	mapper = m;
    }
    public RHMRHelper(String fromWHo){
	callID=fromWHo;
	mapper = null;
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
    int exitval(){
	int exitVal=0;
	try{
	     exitVal = sim.waitFor();
	}catch(InterruptedException e){
	    exitVal=-99;
	}
	return exitVal;
    }

    void addJobConfToEnvironment(Configuration conf, Properties env) {
	Iterator it = conf.iterator();
	while (it.hasNext()) {
	    Map.Entry en = (Map.Entry) it.next();
	    String name = (String) en.getKey();
	    if(name.equals("mapred.input.dir") 
	       || name.equals("rhipe_input_folder")) 
		continue;
	    String value = null;
	    if(!(name.equals("LD_LIBRARY_PATH") || name.equals("PATH"))){
		value = conf.get(name); // does variable expansion
	    } else {
		value = conf.getRaw(name);
	    }
	    env.put(name, value);
	}
    }
    void doPartitionRelatedSetup(Configuration cfg){
	if(!cfg.get("rhipe_partitioner_class").equals("none")){
	    RHMRHelper.PARTITION_START = Integer.parseInt(cfg.get("rhipe_partitioner_start"))-1;
	    RHMRHelper.PARTITION_END = Integer.parseInt(cfg.get("rhipe_partitioner_end"))-1;
	}
    }
    void setup(Configuration cfg, String argv,boolean doPipe){     
	try {
// 	    InetAddress addr = InetAddress.getLocalHost();
// 	    hostname = addr.getHostName();
	    doPartitionRelatedSetup(cfg);
	    String squote = cfg.get("rhipe_string_quote");
	    if(squote == null) squote="";

	    REXPHelper.setFieldSep(cfg.get("mapred.field.separator"," "));
	    REXPHelper.setStringQuote(squote);

	    if(cfg.get("rhipe_test_output")!=null && cfg.get("rhipe_test_output").equals("TRUE"))
		writeErr = true;
	    else
		writeErr= false;

	    BUFFER_SIZE = cfg.getInt("rhipe_stream_buffer",10*1024);
	    joinDelay_ = cfg.getLong("rhipe_joindelay_milli", 0);
	    nonZeroExitIsFailure_ = cfg.getBoolean("rhipe_non_zero_exit_is_failure", true);
	    doPipe_ = doPipe;
	    thisfs=FileSystem.get(cfg);
	    
	    Class<?> _kc =null;
	    
	    if( callID.equals("Mapper")){
		if( cfg.getInt("mapred.reduce.tasks",0) == 0)
		    _kc = Class.forName( cfg.get("rhipe_outputformat_keyclass"));
		else
		    _kc = Class.forName( cfg.get("rhipe_map_output_keyclass"));
	    }else{
		_kc = Class.forName( cfg.get("rhipe_outputformat_keyclass"));
	    }
	    keyclass = _kc.asSubclass( RHBytesWritable.class );


	    if(cfg.get("rhipe_output_folder")!=null)
		outputFolder = new Path(cfg.get("rhipe_output_folder"));
	    if(!doPipe_) return;
	    copyFile=cfg.get("rhipe_copy_file").equals("TRUE")? true: false;
	    String[] argvSplit = argv.split(" ");
	    String prog = argvSplit[0];
	    Environment childEnv = (Environment) env().clone();
	    cfg.set("io_sort_mb",cfg.get("io.sort.mb"));
	    addJobConfToEnvironment(cfg, childEnv);
	    childEnv.put( "TMPDIR", 
			  System.getProperty("java.io.tmpdir"));
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


    void startOutputThreads(TaskInputOutputContext<WritableComparable,RHBytesWritable,
			    WritableComparable,RHBytesWritable> ctx) {
	outThread_ = new MROutputThread(ctx,true);
	outThread_.start();
	errThread_.setContext(ctx);
	LOG.info(callID+":"+"Started Output Thread");
    }
    
 

    public void mapRedFinished(TaskInputOutputContext<WritableComparable,RHBytesWritable,
			       WritableComparable,RHBytesWritable> ctx) {
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

	
    
    void waitOutputThreads(TaskInputOutputContext<WritableComparable,RHBytesWritable
			   ,WritableComparable,RHBytesWritable> ctx) {
	try {
	    // I commented this out, if uncommented, then uncomment the bit for DummyContext
	    // if (outThread_ == null) {
	    // 	startOutputThreads(new DummyContext(ctx)); //will fail
	    // }
	    int exitVal = sim.waitFor();
	    if (exitVal != 0) {
		if (nonZeroExitIsFailure_) {
		    ctx.getCounter("R_ERRORS","subprocess failed with code: "+exitVal).increment(1);
		}else{
		    ctx.getCounter("R_SUBPROCESS","subprocess failed with code: "+exitVal).increment(1);
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

    // class DummyContext extends TaskInputOutputContext<WritableComparable,RHBytesWritable,
    // 			       WritableComparable,RHBytesWritable> {
    // 	DummyContext(TaskInputOutputContext<WritableComparable,RHBytesWritable,
    // 		     WritableComparable,RHBytesWritable>ctx){
    // 	    super(null, null, null, null, null); //wont work
    // 	}
    // 	public RHBytesWritable getCurrentKey() throws IOException, InterruptedException {
    // 	    return null;
    // 	}
	
    // 	public RHBytesWritable getCurrentValue() throws IOException, InterruptedException {
    // 	    return null;
    // 	}
    // 	public boolean nextKeyValue() throws IOException, InterruptedException {
    // 	    return false;
    // 	}
    // 	public void write(RHBytesWritable key, RHBytesWritable value
    // 			  ) throws IOException, InterruptedException {
    // 	}
    // 	public void setStatus(String status){}
    // 	public void progress(){}
    // }

    public void writeCMD(int s) throws IOException{
    	WritableUtils.writeVInt(clientOut_,s);
    	// clientOut_.writeInt(s);
    }
	
    public void write(RHBytesWritable c) throws IOException{
    	c.write(clientOut_);
    }

    public void write(WritableComparable c) throws IOException{
    	c.write(clientOut_);
    }

	
 
    class MROutputThread extends Thread {
	volatile TaskInputOutputContext <WritableComparable,RHBytesWritable,
	    WritableComparable,RHBytesWritable> ctx;
	// volatile TaskInputOutputContext <Object,Object,
	//     Object,Object> ctx;

	long lastStdoutReport = 0;

	MROutputThread(TaskInputOutputContext<WritableComparable,RHBytesWritable,
		       WritableComparable,RHBytesWritable> ctx,boolean isD) {
	    setDaemon(isD);
	    this.ctx = ctx;
	}
	boolean readRecord(WritableComparable k, Writable v) {
	    try{
		k.readFields(clientIn_);
		v.readFields(clientIn_);

	    }catch(IOException e){
		return(false);
	    }
	    return(true);
	}

	public void run() {
	    // Writable key 
	    // RHBytesWritable key = new RHBytesWritable();
	    RHBytesWritable value = new RHBytesWritable();
	    WritableComparable key=  null ;
	    try{
		key= keyclass.newInstance();
	    }catch(InstantiationException e){
		throw new RuntimeException(e);
	    }catch(IllegalAccessException e){
		throw new RuntimeException(e);
	    }

	    try {
		while (readRecord(key,value) ) {
		    // LOG.info("KEY="+value.toString());
		    // LOG.info("Value="+value.toDebugString());
		    // System.out.println(value.getClass().getName());

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
		// throw new IOException(th);
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
			     // mapper.setreadcomplete(true);
			    ctx.getCounter("R_ERRORS",errmsg).increment(1);
			    int y = errmsg.length();
			    throw new RuntimeException(errmsg);
			case RHTypes.PRINT_MSG:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String pmsg = new String(k);
			    LOG.info(pmsg);
			    break;
			case RHTypes.SET_STATUS:
			    ln = clientErr_.readInt();
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    String status = new String(k);
			    ctx.setStatus(status);
			    break;
			// case RHTypes.SET_COUNTER:
			//     ln = clientErr_.readInt();
			//     k = new byte[ln]; 
			//     clientErr_.readFully(k,0,ln);
			//     String grcnt = new String(k);
			//     String[] columns = grcnt.split(",");
			//     ctx.getCounter(columns[0], columns[1])
			// 	.increment(Long.parseLong(columns[2]));
			//     break;
			case RHTypes.SET_COUNTER:
			    ln = RHBytesWritable.readVInt(clientErr_);
			    k = new byte[ln]; 
			    clientErr_.readFully(k,0,ln);
			    REXP r = REXP.parseFrom(k);
			    String grcnt = REXPHelper.toString_(r.getRexpValue(0));
			    String subcnt = REXPHelper.toString_(r.getRexpValue(1));
			    long value = (long)(Double.parseDouble( REXPHelper.toString_(r.getRexpValue(2))));
			    ctx.getCounter(grcnt, subcnt).increment(value);
			    break;
			default:
			    if(writeErr){
				BufferedReader d = new BufferedReader(new InputStreamReader(clientErr_));
				int l=0;
				while(true){
				    String line = d.readLine();
				    if(line == null) break;
				    System.err.println("RHIPE Runner Output["+l+"]: "+line);
				    l++;
				}
			    }
			    
			}
			long now = System.currentTimeMillis(); 
			if ( now-lastStderrReport > reporterErrDelay_) {
			    lastStderrReport = now;
			    if(ctx!=null) {
				ctx.progress();
			    }
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

    public static void invoke
	(String aClass, String aMethod, Class[] params, Object[] args)
    {
	try{
	    Class c = Class.forName(aClass);
	    Method m = c.getDeclaredMethod(aMethod, params);
	    Object i = c.newInstance();
	    Object r = m.invoke(i, args);
	}catch(Exception e){
	    throw new RuntimeException(e);
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
    Class<? extends RHBytesWritable> keyclass;
    public MROutputThread outThread_;
    public MRErrorThread errThread_;
    volatile DataOutputStream clientOut_;
    volatile DataInputStream clientErr_;
    volatile DataInputStream clientIn_;
	boolean writeErr;
    protected volatile Throwable outerrThreadsThrowable;
}
