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


import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import org.apache.hadoop.mapred.FileSplit;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.codec.binary.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;
import org.saptarshiguha.rhipe.utils.*;
import org.saptarshiguha.rhipe.utils.*;



public class RHMRMapper extends MapReduceBase implements Mapper<Object,Object,RXWritable,RXWritable> {
	RConnection re;
	FileSystem dstFS = null;
	String temppfx = null;
	Path dstPath= null;
	File tempdir=null;
	int getbatches;
	static byte[] mapperfunc;
	static byte[] configbytes;
	static byte[] clozebytes;
	RXWritable tv;
	RXWritable tk;
	boolean istxtof,noreducer;
	String commentchar,fieldsep;
	boolean copystuff;
    public void configure(JobConf job){
	REXP cmderrex = null;
	String[] rout = null;
	//	FileSplit myfileSplit = null;
	//	String myfileName = null;
	try{
	    re =  new RConnection("127.0.0.1",job.getInt("rhipejob.rport",8888));
	    re.assign("simplifychar",new REXPInteger(job.getInt("rhipejob.charsxp.short",0)));
	    uniWritable b= new uniWritable();
	    if(RHMRMapper.mapperfunc==null || RHMRMapper.configbytes==null ||  RHMRMapper.clozebytes==null){
		MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(job),
							 job.get("rhipejob.mapfile"),job);
		RHMRMapper.mapperfunc =( (uniWritable)mrd.get(new Text("rhipejob.serializedmapper"), b)).getBytes();
		RHMRMapper.configbytes = ( (uniWritable)mrd.get(new Text("rhipejob.serializedconfig"), b))
		    .getBytes();
		RHMRMapper.clozebytes = ( (uniWritable)mrd.get(new Text("rhipejob.serializedcloze"), b))
		    .getBytes();

		mrd.close();
	    }
	    re.assign("v___",new REXPInteger(Utils.sdd()));
	    re.voidEval("set.seed(v___)");
	    Utils.loadFuncs(re);

	    re.assign("map.function",new REXPRaw( RHMRMapper.mapperfunc ));
	    re.voidEval("map.function=unserialize(map.function)");
	    re.assign("configbytes",new REXPRaw( RHMRMapper.configbytes ));
	    re.assign("clozebytes",new REXPRaw( RHMRMapper.clozebytes ));

	    dstFS = FileOutputFormat.getOutputPath(job).getFileSystem(job);
	    String fsep= System.getProperty("file.separator");
	    dstPath = new Path(job.get("rhipejob.output.folder"));
	    temppfx =System.getProperty("java.io.tmpdir");
	    re.voidEval("setwd('"+temppfx+fsep+".."+"')");
	    if ( !temppfx.endsWith(fsep))
		temppfx = temppfx +fsep;
	    tempdir = new File(temppfx);
	    re.assign("mapred.iswhat","mapper");
		
	    re.voidEval("configure=unserialize(configbytes)");
	    re.voidEval("cloze=unserialize(clozebytes)");
// 	    re.voidEval("for(x... in ls(preload$env)) assign(x...,get(x...,envir=preload$env),envir=.GlobalEnv)");
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure$map,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== MAP CONFIGURE ERROR ====",Utils.ERRTYPE.CLOSE);
	    Utils.showStdout(rl,"==== MAP CONFIGURE STANDARD OUTPUT ====");
	    getbatches = job.getInt("rhipejob.getmapbatches",100)-1;
	    re.assign("mapred.task.is.map",job.get("mapred.task.is.map"));
	    tk = (RXWritable)Class.forName(job.get("mapred.mapoutput.key.class")).newInstance();
	    tv = (RXWritable)Class.forName(job.get("mapred.mapoutput.value.class")).newInstance();
	    if(job.getInt("mapred.reduce.tasks",0)==0) noreducer = true ;else noreducer=false;
	    if(job.getInt("rhipejob.outfmt.is.text",0)==0) istxtof = false; else istxtof=true;
	    fieldsep = job.get("rhipejob.textoutput.fieldsep");
	    commentchar = job.get("rhipejob.textinput.comment");
	    copystuff = job.getInt("rhipejob.copy.to.dfs",1) == 1? true : false;

	    re.assign("...fieldsep...",fieldsep);
	    if(noreducer && istxtof) 
		re.assign("...mapexp...",
			  "tryCatch(list(s=capture.output(ret<-lapply(map.function(mapdata$key,mapdata$value),lapply,paste,sep='',collapse=...fieldsep...)),v=ret)"+
			  ",error=function(ex){ list(e=paste(ex))})");
	    else
		re.assign("...mapexp...",
			  "tryCatch(list(s=capture.output(ret<-lapply(map.function(mapdata$key,mapdata$value),lapply, rhsz)),v=ret)"+
			  ",error=function(ex){ list(e=paste(ex))})");
	    re.voidEval("...mapexp...=parse(text=...mapexp...)");

	}catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }
    public void close() throws IOException{
	try{
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(cloze$map,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== MAP CLOSE ERROR ====",Utils.ERRTYPE.CONFIG);
	    Utils.showStdout(rl,"==== MAP CLOSE STANDARD OUTPUT ====");
	    if(copystuff){
		ArrayList<Path> lop = new ArrayList<Path>();
		for(String ff :  tempdir.list()){
		    File f=new File(temppfx+ff);
		    if( f.isFile() && f.length()>0)
			lop.add(new Path(temppfx+ff));
		}
		if (lop.size()>0) dstFS.copyFromLocalFile(false,true,lop.toArray(new Path[]{}), dstPath);
	    }
	    re.close();
	}catch(Exception e){
	    e.printStackTrace();
	    throw new IOException(e);
	}
    }

    public void map(Object key, Object value,
		    OutputCollector<RXWritable,RXWritable> output, 
		    Reporter reporter)  throws IOException {
	try{
	    //myfileSplit = (FileSplit)reporter.getInputSplit();
	    //myfileName = myfileSplit.getPath().getName();
	    //re.assign("rhfilename",fileName);
	    REXP rkey = ((RXWritable)key).getREXP();
	    REXP rvalue = ((RXWritable)value).getREXP();
	    if(rvalue.isString()){
		String k = rvalue.asString(); 
		//What happens on blank lines? is k== null?
		//I have never checked, so I include k==null
		if(k==null || k.equals("") || k.startsWith(commentchar)) return;
	    }
	    RList rl = new RList(new REXP[]{ rkey,rvalue}, new String[]{"key","value"});
	    re.assign("mapdata",new REXPGenericVector(rl));
	    if(rkey.isRaw()) re.voidEval("mapdata=lapply(mapdata,rhuz)");
	    /**
	       IMPORTANT:
	       Need to collect map key,values, append in a list
	       and run them in R
	       and return the combined list results, i.e.
	       Each map function returns
	       list( list(key,value), list(key,value), ...)
	       so our pseudo code looks like
	       unlist(lapply(list(list(k1,v1),list(k2,v2)),
	              do.call("user_function",r))   ,rec=F)
	    **/
	    rl = re.eval("eval(...mapexp...)").asList();
	    Utils.showError(re,rl,"==== MAP ERROR ====",Utils.ERRTYPE.MAP);
	    Utils.showStdout(rl,"==== MAP STANDARD OUTPUT ====");
	    rl = rl.at("v").asList();
	    RList rl2;
	    REXP k,v;
	    for(int i=0;i< rl.size(); i++){
		rl2 = rl.at(i).asList();
		k =  rl2.at(0); v = rl2.at(1);
		if(k==null) continue;
		tk.set(k); tv.set(v);
		output.collect(tk,tv);
	    }
	}catch(REXPMismatchException e){
	    throw new IOException(e);
	}catch(RserveException e){
	    throw new IOException(e);
	}
    }
}

