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
import java.util.Vector;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;

public class RHMRHelper {
    public static final int REXP_MAX_SIZE = 128*1024*1024;
    protected static final Log LOG = LogFactory.getLog(RHMRHelper.class.getName());
    protected static REXP.RClass PARTITION_TYPE=REXP.RClass.REAL;
    protected static int PARTITION_START=0, PARTITION_END=0;
    private RHBytesWritable v_;
    private RHBytesWritable k_;
    private static RHMRHelper _single_instance = null;
    private String _state="NULL";
    volatile TaskInputOutputContext <WritableComparable,RHBytesWritable,
	WritableComparable,RHBytesWritable> ctx;   
    ByteBuffer keyvalue_stage;
    ByteBuffer reduce_key_stage;
    CodedOutputStream keyvalue_stage_stream;
    CodedInputStream  keyvalue_stage_r2j_stream;
    int keyvalue_stage_stream_bytes,keyvalue_stage_stream_max,keyvalue_stage_stream_nwritten;
    int keyvalue_stage_stream_pad,keyvalue_stage_stream_r2j;
    String _rhome, _tempdir,_libloc;
    String[] _args;
    public RHMRHelper(){}
    public static RHMRHelper embed(String libloc,String rhome,String[] args,String tempdir,int iospillsize) 
	throws RuntimeException 
    {
	if(_single_instance == null){
	    LOG.info("Loading RHMRHelper");
	    System.load(libloc);
	    _single_instance = new RHMRHelper();
	    _single_instance._rhome = rhome;
	    _single_instance._args = args;
	    _single_instance._tempdir = tempdir;
	    if( rhome == null || args.length==0 || tempdir == null)
		throw new RuntimeException("RHIPE: Bad arguments given to embed");
	    // byte[] rh = RObjects.makeStringVector(rhome).toByteArray();
	    // byte[] a =  RObjects.makeStringVector(args).toByteArray();
	    int embedresult = _single_instance.embedR(rhome,args,iospillsize);
	    if(embedresult<0) 
		throw new RuntimeException("RHIPE: Could not embed R engine:"+embedresult);
	    LOG.info("Loaded RHMRHelper");
	}
	return _single_instance;
    }
    public static RHMRHelper embed(Configuration c)
    throws RuntimeException 
    {
	String libloc = c.get("rhipe_lib_location");
	if(libloc==null) throw new RuntimeException("RHIPE: Missing rhipe_lib_location");
	if(c.get("rhipe_embed_args")==null) 
	    throw new RuntimeException("RHIPE: Missing rhipe_embed_args");
	String[] args = c.get("rhipe_embed_args").split(" ");
	String rhome  = c.get("R_HOME");
	String tmpdir = System.getProperty("java.io.tmpdir");
	LOG.info("libloc="+libloc+" R_HOME="+rhome+" TMPDIR="+tmpdir+" args="+c.get("rhipe_embed_args"));
	return embed(libloc, rhome,args, tmpdir,Integer.parseInt(c.get("rhipe_combiner_spill_size")));
    }
    public  int eval(String expr)  throws RHIPEException {
	return(evalR( expr));
    }
    public  void exit(){
	exitR();
    }
    public  void createListOfConfiguration(Configuration c, String robjectname)
    throws RHIPEException
    {
	Iterator it = c.iterator();
	Vector<String> keys = new Vector<String>();
	Vector<REXP> value = new Vector<REXP>();
	while (it.hasNext()) {
	    Map.Entry en = (Map.Entry) it.next();
	    String name = (String) en.getKey();
	    String val = c.get(name);
	    keys.add(name);
	    value.add(RObjects.makeStringVector(val));
	}
	REXP r = RObjects.makeList(keys,value);
	createOpts(r.toByteArray(),RObjects.makeStringVector(robjectname).toByteArray());
    }
    public void initialize_java_to_R_buffer(String s,int buffbytes,int reversebytes,int pad)
    throws RHIPEException
    {
	if(keyvalue_stage == null){
	    LOG.info("Creating DirectBuffer for "+s+" bytes of size "+pad+ " bytes " + buffbytes+" max bytes");
	    keyvalue_stage = create_backing_store(buffbytes+pad);
	}
	keyvalue_stage_stream_max = buffbytes;
	keyvalue_stage_stream_r2j = Math.min(reversebytes,buffbytes);
	keyvalue_stage_stream_pad = pad;
	initialize_map_storage();
    }
    public void initialize_java_to_R_buffer(String s,int keystorage,int buffbytes,int reversebytes,int pad)
	throws RHIPEException
    {
	initialize_java_to_R_buffer(s,buffbytes,reversebytes,pad);
	if(reduce_key_stage == null){
	    LOG.info("Creating DirectBuffer for reduce key of size "+keystorage + " bytes");
	    reduce_key_stage = create_reduce_key_store(keystorage);
	}
    }
	   
    public void initialize_map_storage(){
	keyvalue_stage_stream_bytes = 0;
	keyvalue_stage_stream_nwritten=0;
	keyvalue_stage.rewind();
	keyvalue_stage_stream =  
	    CodedOutputStream.newInstance(
					  new OutputStream() {
					      public synchronized void write(int b) throws IOException {
						  keyvalue_stage.put((byte)b);
					      }
					      public synchronized void write(byte[] bytes, int off, int len) throws IOException {
						  keyvalue_stage.put(bytes, off, len);
					      }
					      public void write(byte[] buf) throws IOException {
						  keyvalue_stage.put(buf);
					      }
					  }
					  );
    }
    public boolean write_v(RHBytesWritable value)
	throws IOException
    {
	keyvalue_stage_stream.writeRawVarint32(value.getLength());
	keyvalue_stage_stream.writeRawBytes(value.getBytes(),0,value.getLength());
	keyvalue_stage_stream.flush();
	LOG.debug("Values="+keyvalue_stage_stream_nwritten+" Writing "+value.getLength());
	keyvalue_stage_stream_bytes = keyvalue_stage.position();
	keyvalue_stage_stream_nwritten ++;
	return(keyvalue_stage_stream_bytes>=keyvalue_stage_stream_max);
    }
    public boolean write_kv(WritableComparable key,RHBytesWritable value)
	throws IOException
    {
	RHBytesWritable k = (RHBytesWritable) key;
	keyvalue_stage_stream.writeRawVarint32(k.getLength());
	keyvalue_stage_stream.writeRawBytes(k.getBytes(),0,k.getLength());
	keyvalue_stage_stream.writeRawVarint32(value.getLength());
	keyvalue_stage_stream.writeRawBytes(value.getBytes(),0,value.getLength());
	keyvalue_stage_stream.flush();
	LOG.debug("Pairs="+keyvalue_stage_stream_nwritten+" Writing "+ k.getLength()+" and "+ value.getLength());
	keyvalue_stage_stream_bytes = keyvalue_stage.position();
	keyvalue_stage_stream_nwritten ++;
	return(keyvalue_stage_stream_bytes>=keyvalue_stage_stream_max);
    }
    public int get_written(){ return keyvalue_stage_stream_nwritten;}
    public void assign(String s,byte[] b, int l)
    {
	reduce_key_stage.rewind();
	reduce_key_stage.put(b,0, l);
	assign_reduce_key(s,l);
    }

    public void reduce_spill(String s, boolean doserial)
	throws RHIPEException,IOException,InterruptedException
    {
	LOG.info("Reduce_Spill: Spilling "+keyvalue_stage_stream_nwritten+" pairs from "+ keyvalue_stage.mark());
	if(doserial) 
	    deserialize_v(keyvalue_stage_stream_nwritten, keyvalue_stage_stream_bytes);
	// The keyvalue_stage directbuffer is now good for us to use all the
	// values have been moved to a list.  We will use this buffer to store
	// key,values when as a buffer rhcollect is called with fast=TRUE thus
	// this is where we store keys/valyes before calling Java to send it
	// to Hadoop via collect.
	rewind_kv_stage(keyvalue_stage_stream_r2j,keyvalue_stage_stream_r2j+keyvalue_stage_stream_pad);
	eval(s);
	send_kv();
	initialize_map_storage();
    }	
    public void map_spill(String s)
	throws RHIPEException,IOException,InterruptedException
    {
	LOG.info("Spilling "+keyvalue_stage_stream_nwritten+" pairs from "+ keyvalue_stage.mark());
	deserialize_kv(keyvalue_stage_stream_nwritten, keyvalue_stage_stream_bytes);
	// The keyvalue_stage directbuffer is now good for us to use all the
	// values have been moved to a list.  We will use this buffer to store
	// key,values when as a buffer rhcollect is called with fast=TRUE thus
	// this is where we store keys/valyes before calling Java to send it
	// to Hadoop via collect.
	rewind_kv_stage(keyvalue_stage_stream_r2j,keyvalue_stage_stream_r2j+keyvalue_stage_stream_pad);
	eval(s);
	send_kv();
	initialize_map_storage();
    }
    public void send_kv() throws IOException,InterruptedException{
	int h = get_num_to_flush_from_c();
	LOG.info("send_kv(),We are going to write "+ h+" values");
	if(h>0) flush_n_pairs(h);
    }
    public void send_kv(int h) 
	throws RHIPEException,IOException,InterruptedException
    {
	LOG.info("We are going to write "+ h+" values");
	if(h>0) flush_n_pairs(h);
	LOG.info("COMPLETELY WRITTEN");
	rewind_kv_stage(keyvalue_stage_stream_r2j,keyvalue_stage_stream_r2j+keyvalue_stage_stream_pad);
    }
    public void flush_n_pairs(int h)
	throws IOException,InterruptedException {
	keyvalue_stage.rewind();
	keyvalue_stage_r2j_stream =  
	    CodedInputStream.newInstance(
					  new InputStream() {
					  
					      public synchronized int read() throws IOException {
					      if (!keyvalue_stage.hasRemaining()) {
						  return -1;
					      }
					      return keyvalue_stage.get();
					      }
					      public synchronized int read(byte[] bytes, int off, int len) throws IOException {
						  len = Math.min(len, keyvalue_stage.remaining());
						  keyvalue_stage.get(bytes, off, len);
						  return len;
					      }
					  }
					 );

	for(int i=0;i < h;i++){
	    k_.readFieldsCodedInputStream(keyvalue_stage_r2j_stream);
	    v_.readFieldsCodedInputStream(keyvalue_stage_r2j_stream);
	    ctx.write(k_,v_);
	}
    }
    public void destroy_r2j(){
	LOG.info("DESTROYONINH:");
	remove_r2j_wrappers();
    }

    public void  setJobState(String s,Configuration cfg){
	_state= s;
	Class<?> _kc =null;
	try{
	    if( s.equals("MAP")){
		if( cfg.getInt("mapred.reduce.tasks",0) == 0)
		    _kc = Class.forName( cfg.get("rhipe_outputformat_keyclass"));
		else
		    _kc = Class.forName( cfg.get("rhipe_map_output_keyclass"));
	    }else{
		_kc = Class.forName( cfg.get("rhipe_outputformat_keyclass"));
	    }
	    _single_instance.k_ = _kc.asSubclass( RHBytesWritable.class ).newInstance();
	    _single_instance.v_ = new RHBytesWritable();
	}catch(InstantiationException e){
	    throw new RuntimeException(e);
	}catch(IllegalAccessException e){
	    throw new RuntimeException(e);
	}catch(ClassNotFoundException e){
	    throw new RuntimeException(e);
	}

    }

    public  void setContext(TaskInputOutputContext <WritableComparable,RHBytesWritable,
				  WritableComparable,RHBytesWritable> ct){
	ctx=ct;
    }
    // JNI Functions
    private native int embedR(String rhome,String[] a,int iospillsize); 
    private native int evalR(String expression); 
    private native void exitR(); 
    private native void createOpts(byte[] o,byte[] s);
    private native ByteBuffer create_backing_store(int buff_size);
    private native ByteBuffer  create_reduce_key_store(int buff_size);
    private native void deserialize_kv(int numto,int tbytes) 	throws RHIPEException ;
    private native void deserialize_v(int numto,int tbytes) 	throws RHIPEException ;
    private native void remove_r2j_wrappers();
    native void assign_reduce_key(String s,int l);
    native void rewind_kv_stage(int n,int b) throws RHIPEException;
    native int get_num_to_flush_from_c();
    native int map_buffer_flush_if_nonempty();
    
    
    // utility functions
    public static void print(String s){
	System.out.print(s);
    }
    public static String jobState(){
	return _single_instance._state;
    }
    public static void counter(String x,String y, int c){
	_single_instance.ctx.getCounter(x, y).increment(c);
    }
    public static void status(String x){
	_single_instance.ctx.setStatus(x);
    }

    public void copy_files(Configuration cfg)
	throws IOException
    {
	if(cfg.get("rhipe_copy_file").equals("TRUE")){
	    FileSystem thisfs;
	    File dirf;
	    if(cfg.get("rhipe_output_folder")!=null){
		Path outputFolder = new Path(cfg.get("rhipe_output_folder"));
		dirf =  new File(System.getProperty("java.io.tmpdir"));
		thisfs = FileSystem.get(cfg);
		ArrayList<Path> lop = new ArrayList<Path>();
		for(File ff :  dirf.listFiles()){
		    if( ff.isFile() && ff.length()>0)
			lop.add(new Path(ff.toString()));
		}
		if (lop.size()>0) thisfs.copyFromLocalFile(false,true,lop.toArray(new Path[]{}), outputFolder);
	    }
	}
    }
}