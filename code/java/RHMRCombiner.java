/*
 * RHIPE - software that integrates Hadoop mapreduce with R
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 *  more details.
 *
 *  You should have received a copy of the GNU General Public License along with
 *  this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 *  Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * Saptarshi Guha sguha@purdue.edu
 */
package org.saptarshiguha.rhipe.hadoop;
import org.saptarshiguha.rhipe.utils.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Enumeration;
import java.util.Set;
import java.util.Random;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.net.URI;
import java.io.IOException;
import java.io.File;
import java.io.StringReader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.BytesWritable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;


import org.saptarshiguha.rhipe.utils.*;

public class RHMRCombiner extends MapReduceBase implements Reducer<RXWritableRAW,RXWritableRAW, RXWritableRAW,RXWritableRAW>  {
	RConnection re;
	int COUNT_MAX;
	ArrayList<REXP> rexpArray;
	RXWritableRAW tk,tv;
	static byte[] combinerfunc;
	static byte[] configbytes;
	static byte[] clozebytes;

	public void configure(JobConf job){
	    COUNT_MAX=job.getInt("rhipejob.combinerspill",100000);
	    try{
		re =  new RConnection("127.0.0.1",job.getInt("rhipejob.rport",8888));
		re.assign("simplifychar",new REXPInteger(job.getInt("rhipejob.charsxp.short",0)));
		uniWritable b= new uniWritable();
		if( RHMRCombiner.combinerfunc==null || RHMRCombiner.configbytes==null ||  RHMRCombiner.clozebytes==null){
		    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(job),
							     job.get("rhipejob.mapfile"),job);
		    RHMRCombiner.combinerfunc =( (uniWritable)mrd.get(new Text("rhipejob.serializedreducer"), b))
			.getBytes();
		    RHMRReducer.configbytes = ( (uniWritable)mrd.get(new Text("rhipejob.serializedconfig"), b))
		    .getBytes();
		    if(RHMRCombiner.combinerfunc == null || RHMRReducer.configbytes == null) 
			throw new Exception("[RHIPE]: serialized bytes for combiner function or config are null");
		    mrd.close();
		}
		Utils.loadFuncs(re);
		re.assign("cmb.func",new REXPRaw( RHMRCombiner.combinerfunc ));
		re.voidEval("cmb.func=unserialize(cmb.func)");
		re.assign("configbytes",new REXPRaw( RHMRReducer.configbytes ));
		String fsep= System.getProperty("file.separator");
		String temppfx =System.getProperty("java.io.tmpdir");
		re.voidEval("setwd('"+temppfx+fsep+".."+"')");
		re.assign("mapred.task.is.map",job.get("mapred.task.is.map"));
		re.assign("mapred.iswhat","combiner");
		re.voidEval("configure=unserialize(configbytes)");
		rexpArray = new ArrayList<REXP>();
// 		re.voidEval("for(x... in ls(preload$env)) assign(x...,get(x...,envir=preload$env),envir=.GlobalEnv)");
		RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure$reduce,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
		Utils.showError(re,rl,"==== COMBINER PRELOAD ERROR ====",Utils.ERRTYPE.CONFIG);
		Utils.showStdout(rl,"==== COMBINER PRELOAD STANDARD OUTPUT ====");
		tk = new RXWritableRAW();
		tv = new RXWritableRAW();
		re.assign("...cmbexp...",
			  "tryCatch(list(s=capture.output(ret<-lapply(cmb.func(red.key,red.value),lapply,rhsz)),v=ret)"+
			  ",error=function(ex){ list(e=paste(ex))})");
		re.voidEval("...cmbexp...=parse(text=...cmbexp...)");
	    }catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
	public void close() throws IOException{  re.close();	}
	
	public void spiller(ArrayList<REXP>rexpArr,OutputCollector<RXWritableRAW,RXWritableRAW> output)
	    throws IOException{
	    try{
		
		RList rl = new RList(rexpArr);
		re.assign("red.value",new REXPGenericVector(rl));
		re.voidEval("red.value=lapply(red.value,rhuz)");
		rl = re.eval("eval(...cmbexp...)").asList();
		Utils.showError(re,rl,"==== COMBINER ERROR ====",Utils.ERRTYPE.CMB);
		Utils.showStdout(rl,"==== COMBINER STANDARD OUTPUT ====");
		rl = rl.at("v").asList();
		RList rl2;
		REXP k,v;
		for(int i=0;i< rl.size(); i++){
		    rl2 = rl.at(i).asList();
		    k =  rl2.at(0); v = rl2.at(1);
		    if(k==null || v==null) continue;
		    tk.set(k); tv.set(v);
		    output.collect(tk,tv);
		}
	    }catch(RserveException e) {
		throw new IOException(e);
	    } catch(REXPMismatchException e) {
		throw new IOException(e);
	    }
	}
	
	public void reduce(RXWritableRAW key, Iterator<RXWritableRAW> values,
			   OutputCollector<RXWritableRAW,RXWritableRAW> output, Reporter reporter) 
	    throws IOException {
	    rexpArray.ensureCapacity(COUNT_MAX);
	    int count = 0;
	    try{
		re.assign("red.key",key.getREXP());
		re.voidEval("red.key=rhuz(red.key)");
		while(values.hasNext()) {
		    count=count+1;
		    if(count <= COUNT_MAX){
			rexpArray.add(values.next().getREXP());
		    }else{
			spiller(rexpArray,output);
			rexpArray.clear();
			rexpArray.ensureCapacity(COUNT_MAX);//do i need this?
			count = 0;
		    }
		    reporter.progress();
		}
		if(count>0){
		    rexpArray.trimToSize();
		    spiller(rexpArray,output);
		}
		
		rexpArray.clear();
		if(count>=COUNT_MAX) re.voidEval("rm(red.value)");
		count = 0;
		reporter.progress();
	    }catch(RserveException e) {
		throw new IOException(e);
	    } 
	}
}
