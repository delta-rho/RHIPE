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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;


import org.saptarshiguha.rhipe.utils.*;


public class RHMRReducer extends  Reducer<RXWritableRAW,RXWritableRAW,
				  Object, Object> {
    RConnection re;
    FileSystem dstFS = null;
    String temppfx = null,fieldsep;
    Path dstPath= null;
    File tempdir=null;
    int getbatches,maxforreduce,rdbatch;
    boolean isstepper,lowmem,istxtof,noreducer,skipemoremax,copystuff;
    RXWritable tk,tv;
    static byte[] reducef;
    static byte[] configbytes;
    static byte[] clozebytes;

    public void setup(Context ctx) throws IOException, InterruptedException{
	Configuration cfg = ctx.getConfiguration();	  
	try{
	    re =  new RConnection("127.0.0.1",cfg.getInt("rhipejob.rport",8888));
	    byte[] reducef, spreload;
	    uniWritable b= new uniWritable();
	    if(RHMRReducer.reducef == null || 
	       RHMRReducer.configbytes==null ||  
	       RHMRReducer.clozebytes==null){
		MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(cfg)
							 ,cfg.get("rhipejob.mapfile"),cfg);
		RHMRReducer.reducef =( (uniWritable)mrd
				       .get(new Text("rhipejob.serializedreducer"), b))
		    .getBytes();
		RHMRReducer.configbytes = ( (uniWritable)mrd
					    .get(new Text("rhipejob.serializedconfig"),b))
		    .getBytes();
		RHMRReducer.clozebytes = ( (uniWritable)mrd
					   .get(new Text("rhipejob.serializedcloze"), b))
		    .getBytes();
		mrd.close();
	    }
	    re.assign("v___",new REXPInteger(Utils.sdd()));
	    re.voidEval("set.seed(v___);rm(v___)");
	    Utils.loadFuncs(re);
	    re.assign("red.func",new REXPRaw( RHMRReducer.reducef ));
	    re.voidEval("red.func=unserialize(red.func)");
	    re.assign("..configbytes",new REXPRaw( RHMRReducer.configbytes ));
	    re.assign("..clozebytes",new REXPRaw( RHMRReducer.clozebytes ));

	    dstFS = FileOutputFormat.getOutputPath(ctx).getFileSystem(cfg);
	    String fsep= System.getProperty("file.separator");
	    dstPath = new Path(cfg.get("rhipejob.output.folder"));
	    temppfx =System.getProperty("java.io.tmpdir");
	    re.voidEval("setwd('"+temppfx+fsep+".."+"')");
	    if ( !temppfx.endsWith(fsep))
		temppfx = temppfx +fsep;
	    tempdir = new File(temppfx);
	    re.assign("mapred.task.is.map",cfg.get("mapred.task.is.map"));
	    re.assign("mapred.iswhat","reducer");
	    re.voidEval("configure=unserialize(..configbytes);rm(...configbytes)");
	    re.voidEval("cloze=unserialize(..clozebytes);rm(..clozebytes)");
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure$reduce,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== REDUCE CONFIGURE ERROR ====",Utils.ERRTYPE.CONFIG);
	    Utils.showStdout(rl,"==== REDUCE CONFIGURE STANDARD OUTPUT ====");
	    copystuff = cfg.getInt("rhipejob.copy.to.dfs",1) == 1? true : false;
	    rdbatch = cfg.getInt("rhipejob.tor.batch",200000);
	    isstepper = cfg.getInt("rhipejob.reduce.stepper",0) == 1? true : false;
	    maxforreduce = cfg.getInt("rhipejob.max.count.reduce",Integer.MAX_VALUE);
	    tk = (RXWritable)Class.forName(cfg.get("mapred.output.key.class"))
		.newInstance();
	    tv = (RXWritable)Class.forName(cfg.get("mapred.output.value.class"))
		.newInstance();
	    istxtof = cfg.getInt("rhipejob.outfmt.is.text",0)==0? false: true;
	    fieldsep = cfg.get("rhipejob.textoutput.fieldsep");
	    re.assign("...fieldsep...",fieldsep);
	    Utils.assignConfVars(cfg,re);

	    if(istxtof){
		re.assign("...redexp...",
			      "tryCatch(list(s=capture.output(ret<-lapply(red.func(red.key,red.value),lapply,paste,sep='',collapse=...fieldsep...)),v=ret)"+
			      ",error=function(ex){ list(e=paste(ex))})");
	    }else{
		re.assign("...redexp...",
			      "tryCatch(list(s=capture.output(ret<-lapply(red.func(red.key,red.value),lapply, rhsz)),v=ret)"+
			      ",error=function(ex){ list(e=paste(ex))})");
	    }

	    re.voidEval("...redexp...=parse(text=...redexp...)");
	}catch (Exception e) {
	    e.printStackTrace();
	    throw new IOException(e);
	}
    }
    public void cleanup(Context ctx) throws IOException,InterruptedException{
	try{
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(cloze$reduce,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== REDUCE CLOSE ERROR ====",Utils.ERRTYPE.CLOSE);
	    Utils.showStdout(rl,"==== REDUCE CLOSE STANDARD OUTPUT ====");
	    
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
	}catch (Exception e) {
	    e.printStackTrace();
	    throw new IOException(e);
	}
    }

    public boolean evaluate(Context context) 
	throws REXPMismatchException,RserveException,IOException,
	InterruptedException{
	RList rl = re.eval("eval(...redexp...)").asList();
	Utils.showError(re,rl,"==== REDUCE ERROR ====",Utils.ERRTYPE.RED);
	Utils.showStdout(rl,"==== REDUCE STANDARD OUTPUT ====");
	rl = rl.at("v").asList();
	RList rl2;
	REXP k,v;
	for(int i=0;i< rl.size(); i++){
	    rl2 = rl.at(i).asList();
	    if(rl2.at("stop")!=null) return true;
	    k =  rl2.at(0); v = rl2.at(1);
	    if(k==null || v==null) continue;
	    tk.set(k); tv.set(v);
	    context.write(tk,tv);
	}
	return false;
    }
	
    public void run(Context context) throws 
	IOException, InterruptedException {
	setup(context);
	while (context.nextKey()) {
	    ArrayList<REXP> rexpArray = new ArrayList<REXP>();
	    rexpArray.ensureCapacity(rdbatch);
	    boolean thisisbig=false;
	    try{
		re.assign("red.key", ((RXWritableRAW)context.getCurrentKey()).getREXP());
		re.voidEval("red.key=rhuz(red.key)");
		re.assign("red.status", new REXPInteger(1));
		re.voidEval("red.value=list()");
		context.setStatus("Now submitting to R");
		boolean fist=true,breakme = false;
		int batchid = 0,count_=0;
		Iterable<RXWritableRAW> values = context.getValues();
		for(RXWritableRAW rawvalue : values){
		    if(count_ > maxforreduce) break;
		    rexpArray.add(rawvalue.getREXP());
		    batchid+=1;count_+=1;
		    if(batchid >= rdbatch){
			RList rlist = new RList(rexpArray);
			REXPGenericVector rl = new REXPGenericVector(rlist);
			re.assign("....x0",rl);
			re.voidEval("....x0=lapply(....x0,rhuz)");
			if(isstepper){
			    re.voidEval("red.value=....x0");
			    breakme=evaluate(context);
			    if(breakme) break;
			}else
			    re.voidEval("red.value=append(red.value,....x0)");
			if(fist){ 	    
			    re.assign("red.status", new REXPInteger(0));
			    fist=false;
			}
			rexpArray.clear();
			batchid=0;
			thisisbig=true;
		    }
		    context.progress();
		}
	    //Spill left over
		if(batchid>0 && !breakme){
		    rexpArray.trimToSize();
		    RList rlist = new RList(rexpArray);
		    REXPGenericVector rl = new REXPGenericVector(rlist);
		    re.assign("....x0",rl);
		    re.voidEval("....x0=lapply(....x0,rhuz)");
		    if(isstepper){
			re.voidEval("red.value=....x0");
			breakme=evaluate(context);
		    } else
			re.voidEval("red.value=append(red.value,....x0)");
		    rexpArray.clear();
		}
		re.assign("red.status", new REXPInteger(-1));
		if(!breakme && isstepper){
		    re.voidEval("red.value=NULL");
		}
		evaluate(context);
		if(thisisbig) re.voidEval("gc()");
		context.progress();
	    }catch(RserveException e){
		e.printStackTrace();
		throw new IOException(e);
	    }catch(REXPMismatchException e){
		e.printStackTrace();
		throw new IOException(e);
	    }
	}
	cleanup(context);
    }
}