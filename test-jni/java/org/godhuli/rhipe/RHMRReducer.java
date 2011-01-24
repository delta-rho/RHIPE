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
import java.util.Iterator;
import java.io.*;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;


public class RHMRReducer extends Reducer<WritableComparable,
				 RHBytesWritable,WritableComparable,RHBytesWritable> {
    protected static final Log LOG = LogFactory.getLog(RHMRReducer.class.getName());
    protected RHMRHelper  re;
    boolean donot_use_r_in_reduce;
    public void run(Context context) 
	throws IOException, InterruptedException {
	Configuration cfg = context.getConfiguration();
	re = RHMRHelper.embed(cfg);
	re.setJobState("REDUCE",cfg);
	re.setContext(context);
	donot_use_r_in_reduce = context.getConfiguration().get("rhipe_reduce_justcollect").equals("TRUE")?true:false;
	if(!donot_use_r_in_reduce){
	    custom_reducer(context);
	}else{
	    /******************************************************
	     * User wants to reduce, but does not want any R code 
	     ******************************************************/
	    try{
		Class<?> _kc =null;
		Class<? extends RHBytesWritable> keyclass;
		WritableComparable wck= null;
		_kc = Class.forName( context.getConfiguration().get("rhipe_outputformat_keyclass"));
		keyclass = _kc.asSubclass( RHBytesWritable.class );
		wck = keyclass.newInstance();
	    }catch(InstantiationException e){
		throw new RuntimeException(e);
	    }catch(IllegalAccessException e){
		throw new RuntimeException(e);
	    }catch(ClassNotFoundException e){
		throw new RuntimeException(e);
	    }
	    while (context.nextKey()) {
		simplereduce(context.getCurrentKey(), context.getValues(), context);
	    }
	}
    }
    public void custom_reducer(Context context)
	throws IOException, InterruptedException{
	try{
	    re.initialize_java_to_R_buffer("Reducer",
					   RHMRHelper.REXP_MAX_SIZE,
					   Integer.parseInt(context.getConfiguration()
							    .get("rhipe_reduce_buffer_bytes")),
					   Integer.parseInt(context.getConfiguration()
							    .get("rhipe_r2j_buffer_bytes")),
					   // Because the buffer used to send sequence of values
					   // is the same buffer used to send /back/ key/values!
					   RHMRHelper.REXP_MAX_SIZE*2+16);
	    LOG.info("Installing mapred.opts");
	    re.createListOfConfiguration(context.getConfiguration(), "mapred.opts");
	    re.eval("mapred.opts$reduce.setup		<- unserialize(charToRaw(mapred.opts$rhipe_setup_reduce));"
		    +"mapred.opts$reduce.pre		<- unserialize(charToRaw(mapred.opts$rhipe_reduce_prekey));"
		    +"mapred.opts$reduce.reduce		<- unserialize(charToRaw(mapred.opts$rhipe_reduce));"
		    +"mapred.opts$reduce.post		<- unserialize(charToRaw(mapred.opts$rhipe_reduce_postkey));"
		    +"mapred.opts$reduce.cleanup	<- unserialize(charToRaw(mapred.opts$rhipe_cleanup_reduce));");
	    re.eval("rhcounter				<- function(f,g,x) .Call('rh_counter',as.character(c(f,g)),as.integer(x))");
	    re.eval("rhstatus				<- function(f) .Call('rh_status',as.character(f))");

	}catch(RHIPEException e) {
	    throw new IOException(e);
	}

	setup(context);
	
	while (context.nextKey()) {
	    RHBytesWritable thekey = (RHBytesWritable)context.getCurrentKey();
	    re.assign("reduce.key", thekey.getBytes(),thekey.getLength());
	    try{
		re.eval("eval(mapred.opts$reduce.pre)");
		re.send_kv();
	    }catch(RHIPEException e){
		throw new IOException(e);
	    }
	    user_reduce(context.getValues(), context);
	    
	}
	cleanup(context);
	re.copy_files(context.getConfiguration());
	re.destroy_r2j() ;
    }
    public void setup(Context ctx) 
    throws IOException{
	try{
	    LOG.info("Running reduce setup");
	    re.eval("rhcollect <- function(k,v,flush=FALSE) .Call('rh_spool_kv',k,v,flush)");
	    re.eval("eval(mapred.opts$reduce.setup)");
	}
	catch(RHIPEException e){
	    throw new IOException(e);
	}
    }

  public void user_reduce(Iterable<RHBytesWritable> values, 
		     Context ctx) 
      throws IOException,InterruptedException {
      	try{
	    for(RHBytesWritable val : values){
		if(re.write_v(val)){
		    re.reduce_spill("eval(mapred.opts$reduce.reduce)",true);
		}
	    }
	    if(re.get_written()>0){
		LOG.info("Flushing left over"); 
		re.reduce_spill("eval(mapred.opts$reduce.reduce)",true);
	    }
	    re.reduce_spill("eval(mapred.opts$reduce.post)",false);
	}
	catch(RHIPEException e){
	    throw new IOException(e);
	}
  }
  public void simplereduce(WritableComparable key, Iterable<RHBytesWritable> values, 
		     Context ctx) 
      throws IOException,InterruptedException {
      try {	    
	  for(RHBytesWritable val : values)
	      ctx.write(key,val);
      } catch (IOException io) {
    }
  }

  public void cleanup(Context ctx)
  throws IOException{
	try{
	    LOG.info("Running reduce cleanup");
	    re.eval("eval(mapred.opts$reduce.cleanup)");
	}catch(RHIPEException e){
	    throw new IOException(e);
	}
  }
}
