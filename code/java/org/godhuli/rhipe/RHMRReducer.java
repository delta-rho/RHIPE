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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class RHMRReducer extends Reducer<RHBytesWritable,
				 RHBytesWritable,RHBytesWritable,RHBytesWritable> {
    protected static final Log LOG = LogFactory.getLog(RHMRReducer.class.getName());
    boolean isAMap;
    RHMRHelper helper;
    boolean doPipe_;
    boolean justCollect;
    String getPipeCommand(Configuration cfg) {
	String str = System.getenv("RHIPECOMMAND");
	if (str == null) {
	    str=cfg.get("rhipe_command");
	    if(str==null) System.err.println("No rhipe_command");
	}
	return(str);

    }

    boolean getDoPipe(Configuration cfg) {
	String argv = getPipeCommand(cfg);
	doPipe_= getPipeCommand(cfg) !=null && cfg.getInt("mapred.reduce.tasks",0)!=0;
	return(!justCollect);
    }
    
    public void run(Context context) throws IOException, InterruptedException {
	helper = new RHMRHelper("Reduce");
	justCollect = context.getConfiguration().
	    get("rhipe_reduce_justcollect").equals("TRUE")?true:false;
	if(!justCollect){
	    setup(context);
	    helper.startOutputThreads(context);
	    while (context.nextKey()) {
		pipereduce(context.getCurrentKey(), context.getValues(), context);
	    }
	    cleanup(context);
	}else{
	    while (context.nextKey()) {
		simplereduce(context.getCurrentKey(), context.getValues(), context);
	    }
	}
    }
    
  public void setup(Context ctx) {
      Configuration cfg =ctx.getConfiguration();
      cfg.set("RHIPEWHAT","1");
      helper.setup(cfg,getPipeCommand(cfg),getDoPipe(cfg));
      isAMap = cfg.getBoolean("mapred.task.is.map",true);
      try{
	  if(!justCollect) helper.writeCMD(RHTypes.EVAL_SETUP_REDUCE);
      }catch(IOException e){
	  e.printStackTrace();
	  throw new RuntimeException(e);
      }
  }

  public void pipereduce(RHBytesWritable key, Iterable<RHBytesWritable> values, 
		     Context ctx) throws IOException,InterruptedException {
      try {	    
	    helper.writeCMD(RHTypes.EVAL_REDUCE_THEKEY);
	    helper.write(key);
	    helper.writeCMD(RHTypes.EVAL_REDUCE_PREKEY);
	    for(RHBytesWritable val : values){
	    	helper.checkOuterrThreadsThrowable();
	    	helper.write(val);
	    }
	    helper.writeCMD(RHTypes.EVAL_REDUCE_POSTKEY);
      } catch (IOException io) {
	String extraInfo = "";
	try {
	    extraInfo = helper.getSimExitInfo();
	} catch (IllegalThreadStateException e) {
	    extraInfo = "subprocess still running\n";
	};
	helper.mapRedFinished(ctx);
	throw new IOException(extraInfo + "::" + io.getMessage());
    }
  }


  public void simplereduce(RHBytesWritable key, Iterable<RHBytesWritable> values, 
		     Context ctx) throws IOException,InterruptedException {
      try {	    
	  for(RHBytesWritable val : values)
	      ctx.write(key,val);
	      
      } catch (IOException io) {
	String extraInfo = "";
	try {
	    extraInfo = helper.getSimExitInfo();
	} catch (IllegalThreadStateException e) {
	    extraInfo = "subprocess still running\n";
	};
	helper.mapRedFinished(ctx);
	throw new IOException(extraInfo + "::" + io.getMessage());
    }
  }



  public void cleanup(Context ctx) {
      try{
		  if(!justCollect) {
		      if(!isAMap)
			  helper.writeCMD(RHTypes.EVAL_CLEANUP_REDUCE);
		      helper.writeCMD(RHTypes.EVAL_FLUSH);
		  }

	  helper.mapRedFinished(ctx);
	  if(!isAMap) helper.copyFiles(System.getProperty("java.io.tmpdir"));
      }catch(IOException e){
	  e.printStackTrace();
	  throw new RuntimeException(e);
      }
  }


}
