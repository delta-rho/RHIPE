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



public class RHMRCombiner extends Reducer<RHBytesWritable,
				 RHBytesWritable,RHBytesWritable,RHBytesWritable> {
    protected static final Log LOG = LogFactory.getLog(RHMRCombiner.class.getName());
    private RHMRHelper helper;
    public void run(Context context) throws IOException, InterruptedException {
	
	Configuration cfg =context.getConfiguration();
	helper = new RHMRHelper("Combiner");
	cfg.set("RHIPEWHAT","2");
	helper.setup(cfg, cfg.get("rhipe_command"),true);
	helper.startOutputThreads(context,false);
	while (context.nextKey()) {
	    pipereduce(context.getCurrentKey(), context.getValues(), context);
	}
	helper.writeCMD(-10);
	helper.mapRedFinished(context);
	// helper.funfun();
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




}
