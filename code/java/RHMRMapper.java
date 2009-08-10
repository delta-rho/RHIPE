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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;
import org.saptarshiguha.rhipe.utils.*;
import org.saptarshiguha.rhipe.utils.*;



public class RHMRMapper extends  Mapper<Object,Object,
				 RXWritable,RXWritable> {
	RConnection re;
	FileSystem dstFS = null;
	String temppfx = null;
	Path dstPath= null;
	File tempdir=null;
	static byte[] mapperfunc;
	static byte[] configbytes;
	static byte[] clozebytes;
	RXWritable tv;
	RXWritable tk;
	boolean istxtof,noreducer,copystuff;
	String commentchar,fieldsep;
	int tomapside = 0,frommapside=0;
	RList rl = null,erl=null;
	ArrayList<REXP> mapcollector=null;
	
    public void setup(Context ctx) throws IOException, InterruptedException{
	Configuration cfg = ctx.getConfiguration();
	try{

	    re =  new RConnection("127.0.0.1",cfg.getInt("rhipejob.rport",8888));
	    re.assign("simplifychar",new REXPInteger(cfg.getInt("rhipejob.charsxp.short",0)));
	    uniWritable b= new uniWritable();
	    if(RHMRMapper.mapperfunc==null || RHMRMapper.configbytes==null 
	       ||  RHMRMapper.clozebytes==null){
		MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(cfg),
							 cfg.get("rhipejob.mapfile"),cfg);
		RHMRMapper.mapperfunc =((uniWritable)mrd.get(new Text("rhipejob.serializedmapper"), b)).getBytes();
		RHMRMapper.configbytes = ( (uniWritable)mrd.get(new Text("rhipejob.serializedconfig"), b)).getBytes();
		RHMRMapper.clozebytes = ( (uniWritable)mrd.get(new Text("rhipejob.serializedcloze"), b)).getBytes();
		mrd.close();
	    }
	    re.assign("v___",new REXPInteger(Utils.sdd()));
	    re.voidEval("set.seed(v___);rm(v___);");
	    Utils.loadFuncs(re);

	    re.assign("map.function",new REXPRaw( RHMRMapper.mapperfunc ));
	    re.voidEval("map.function=unserialize(map.function)");
	    re.assign("configbytes",new REXPRaw( RHMRMapper.configbytes ));
	    re.assign("clozebytes",new REXPRaw( RHMRMapper.clozebytes ));

	    dstFS = FileOutputFormat.getOutputPath(ctx).getFileSystem(cfg);
	    String fsep= System.getProperty("file.separator");
	    dstPath = new Path(cfg.get("rhipejob.output.folder"));
	    temppfx =System.getProperty("java.io.tmpdir");
	    re.voidEval("setwd('"+temppfx+fsep+".."+"')");
	    if ( !temppfx.endsWith(fsep))
		temppfx = temppfx +fsep;
	    tempdir = new File(temppfx);
	    re.assign("mapred.iswhat","mapper");
	    
	    re.voidEval("configure=unserialize(configbytes);rm(configbytes);");
	    re.voidEval("cloze=unserialize(clozebytes);rm(clozebytes);");
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure$map,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== MAP CONFIGURE ERROR ====",Utils.ERRTYPE.CLOSE);
	    Utils.showStdout(rl,"==== MAP CONFIGURE STANDARD OUTPUT ====");
	    tk = (RXWritable)Class.forName(cfg.get("mapred.mapoutput.key.class")).
		newInstance();
	    tv = (RXWritable)Class.forName(cfg.get("mapred.mapoutput.value.class")).
		newInstance();
	    noreducer = cfg.getInt("mapred.reduce.tasks",0)==0 ? true:false;
	    istxtof = cfg.getInt("rhipejob.outfmt.is.text",0)==0 ? false: true;
	    fieldsep = cfg.get("rhipejob.textoutput.fieldsep");
	    commentchar = cfg.get("rhipejob.textinput.comment");
	    copystuff = cfg.getInt("rhipejob.copy.to.dfs",1) == 1? true : false;

	    Utils.assignConfVars(cfg,re);

	    re.assign("...fieldsep...",fieldsep);
	    re.voidEval("..srhuz=function(r) if(is.raw(r)) return(rhuz(r)) else return(r)");
	    if(noreducer && istxtof) 
		re.assign("...mapexp...",
			 "tryCatch(list(s=capture.output(ret<-lapply(unlist(lapply(mapdata,function(r){ do.call('map.function',lapply(r,..srhuz))}),rec=F),lapply,paste,sep='',collapse=...fieldsep...)),v=ret),error=function(ex){ list(e=paste(ex))})");
	    else
		re.assign("...mapexp...",
			  "tryCatch(list(s=capture.output(ret<-lapply(unlist(lapply(mapdata,function(r){ do.call('map.function',lapply(r,..srhuz))}),rec=F),lapply,rhsz)),v=ret),error=function(ex){ list(e=paste(ex))})");

	    re.voidEval("...mapexp...=parse(text=...mapexp...)");

	    tomapside = cfg.getInt("rhipejob.tom",200000);
	    mapcollector = new ArrayList<REXP>();
	    mapcollector.ensureCapacity(tomapside);
	    frommapside = cfg.getInt("rhipejob.frommap",200000);
	}catch (Exception e) {
	    e.printStackTrace();
	    throw new IOException(e);
	}
    }
    public void cleanup(Context ctx) throws IOException,InterruptedException{
	try{
	    if(re!=null){
		RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(cloze$map,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
		
		Utils.showError(re,rl,"==== MAP CLOSE ERROR ====",Utils.ERRTYPE.CONFIG);
		Utils.showStdout(rl,"==== MAP CLOSE STANDARD OUTPUT ====");
		re.close();
	    }
	    if(copystuff){
		ArrayList<Path> lop = new ArrayList<Path>();
		for(String ff :  tempdir.list()){
		    File f=new File(temppfx+ff);
		    if( f.isFile() && f.length()>0)
			lop.add(new Path(temppfx+ff));
		}
		if (lop.size()>0) dstFS.copyFromLocalFile(false,true,lop.toArray(new Path[]{}), dstPath);
	    }
	}catch(Exception e){
	    e.printStackTrace();
	    throw new IOException(e);
	}
    }

    public void map(Object key, Object value,
		    Context ctx)  throws IOException {
    }

    public void run(Context context)
	throws IOException, InterruptedException{
	setup(context);
	REXP rkey,rvalue;
	String k;
	try{
	    while (context.nextKeyValue()) {
		rkey = ((RXWritable)context.getCurrentKey()).getREXP();
		rvalue = ((RXWritable)context.getCurrentValue()).getREXP();
		if(rvalue.isString()){
		    k = rvalue.asString(); 
		    if(k==null || k.equals("") || k.startsWith(commentchar)) continue;
		}
		REXPGenericVector thiskv = 
		    new REXPGenericVector(new RList(new REXP[]{ rkey,rvalue}));
		mapcollector.add( thiskv);
		if(mapcollector.size()>=tomapside){
		    publish(context);
		}
	    }
	}catch(REXPMismatchException e){
	    e.printStackTrace();
	    throw new IOException(e);
	}catch(NullPointerException e){
	    e.printStackTrace();
	}
	if(mapcollector.size()>0) {
	    mapcollector.trimToSize();
	    publish(context);
	}
	cleanup(context);
    }

    public void publish(Context context)  throws IOException,InterruptedException {	 
	try{
	    RList dataList = new RList(mapcollector);
	    REXPGenericVector dataGVector = new REXPGenericVector(dataList);
	    re.assign("mapdata",dataGVector);
	    int alen = re.eval(".d.=eval(...mapexp...);as.integer(length(.d.$v))").
		asInteger();
	    erl = re.eval(".d.[c('e','s')]").asList();
	    Utils.showError(re,erl,"==== MAP ERROR ====",Utils.ERRTYPE.MAP);
	    Utils.showStdout(erl,"==== MAP STANDARD OUTPUT ====");
	    for(int kk=1;kk<=alen;kk+=frommapside){
		rl = re.eval(".d.$v[ "+kk+":"+Math.min(kk+frommapside-1,alen)+"]").
		    asList();
		RList rl2;
		REXP k,v;
		for(int i=0;i< rl.size(); i++){
		    rl2 = rl.at(i).asList();
		    if(rl2.size()<2) continue;
		    k =  rl2.at(0); v = rl2.at(1);
		    tk.set(k); tv.set(v);
		    context.write(tk,tv);
		}
	    }
	    mapcollector.clear();
	}catch(REXPMismatchException e){
	    throw new IOException(e);
	}catch(RserveException e){
	    throw new IOException(e);
	}
    }
}


