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

import java.io.*;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class RHMRMapper extends Mapper<WritableComparable,
				RHBytesWritable,WritableComparable,RHBytesWritable>{
    protected static final Log LOG = LogFactory.getLog(RHMRMapper.class.getName());
    protected RHMRHelper  re;
    protected boolean using_combiner;
    public void run(Context context) 
	throws IOException,InterruptedException {
	Configuration cfg = context.getConfiguration();
	re = RHMRHelper.embed(cfg);
	re.setJobState("MAP",cfg);
	re.setContext(context);
	try{
	    re.initialize_java_to_R_buffer("Mapper",Integer.parseInt(context.getConfiguration()
							    .get("rhipe_buffer_bytes")), // Java to R
					   Integer.parseInt(context.getConfiguration()   // R to Java
							    .get("rhipe_r2j_buffer_bytes")),
					   RHMRHelper.REXP_MAX_SIZE*2+8);
	    LOG.info("Installing mapred.opts");
	    re.createListOfConfiguration(cfg, "mapred.opts");
	    re.eval("mapred.opts$map.setup	<- unserialize(charToRaw(mapred.opts$rhipe_setup_map));"
		    +"mapred.opts$map.map	<- unserialize(charToRaw(mapred.opts$rhipe_map));"
		    +"mapred.opts$map.cleanup	<- unserialize(charToRaw(mapred.opts$rhipe_cleanup_map));");
	    re.eval("rhcounter <- function(f,g,x) .Call('rh_counter',as.character(c(f,g)),as.integer(x))");
	    re.eval("rhstatus <- function(f) .Call('rh_status',as.character(f))");
	    using_combiner = cfg.get("rhipe_combiner").equals("0") ? false : true;
	}catch(RHIPEException e) {
	    throw new IOException(e);
	}
	setup(context);
	while (context.nextKeyValue()) {
	    map(context.getCurrentKey(), context.getCurrentValue(), context);
	}
	process_left_over_kv();
	cleanup(context);
	re.copy_files(context.getConfiguration());
	re.destroy_r2j() ;
    }

    public void setup(Context context)
    throws IOException {
	Configuration cfg = context.getConfiguration();
	try{
	    String mif = ((FileSplit) context.getInputSplit()).getPath().toString();
	    if(mif != null) cfg.set("mapred.input.file",mif);
	}catch(ClassCastException e){
	    // throws an exception if input format does not extend FileInputFormat
	    // I should do an if check, but who cares? tiny cost
	}
	try{
	    LOG.info("Running map setup");
	    re.eval("eval(mapred.opts$map.setup)");

	    if(using_combiner){
		LOG.info("Using a combiner");
		re.eval("mapred.opts$reduce.setup	<- unserialize(charToRaw(mapred.opts$rhipe_setup_reduce));"
		    +"mapred.opts$reduce.pre		<- unserialize(charToRaw(mapred.opts$rhipe_reduce_prekey));"
		    +"mapred.opts$reduce.reduce		<- unserialize(charToRaw(mapred.opts$rhipe_reduce));"
		    +"mapred.opts$reduce.post		<- unserialize(charToRaw(mapred.opts$rhipe_reduce_postkey));"
		    +"mapred.opts$reduce.cleanup	<- unserialize(charToRaw(mapred.opts$rhipe_cleanup_reduce));");
		re.eval("rhcollect			<- function(k,v,buffer=TRUE) .Call('rh_combine_kv',k,v,buffer)");
	    }
	    else {
		re.eval("rhcollect <- function(k,v,buffer=TRUE) .Call('rh_spool_kv',k,v,buffer)");
	    }
	}
	catch(RHIPEException e){
	    throw new IOException(e);
	}
    }
    public void map(WritableComparable key, RHBytesWritable value, Context ctx) 
	throws IOException,InterruptedException {
	try{
	    if(re.write_kv(key,value)){
		re.map_spill("eval(mapred.opts$map.map)");
	    }
	}
	catch(RHIPEException e){
	    throw new IOException(e);
	}
    }
    public void process_left_over_kv()
	throws IOException,InterruptedException{
	try{
	    if(re.get_written()>0){
		LOG.info("Flushing left over"); 
		re.map_spill("eval(mapred.opts$map.map)");
		if(using_combiner) re.map_buffer_flush_if_nonempty();
		re.send_kv();
	    }else
		{
		    if(using_combiner) re.map_buffer_flush_if_nonempty();
		    re.send_kv(); //retrieves from R		if(using_combiner){
		}
	}
	catch(RHIPEException e){
	    throw new IOException(e);
	}
    }

    public void cleanup(Context ctx) 
	throws IOException
    {
	try{
	    LOG.info("Running map cleanup");
	    re.eval("eval(mapred.opts$map.cleanup)");
	}catch(RHIPEException e){
	    throw new IOException(e);
	}
    }
}
	