/*
 * RHIPE - software that integrates Hadoop mapreduce with R
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Saptarshi Guha sguha@purdue.edu
 */
package org.saptarshiguha.rhipe.hadoop;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.BytesWritable;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;

import org.saptarshiguha.rhipe.utils.*;
    

public class RHLApply  {
    public RHLApply(){}
    static class LApplyMapper
	extends Mapper<RXWritable,RXWritable,RXWritableRAW,RXWritableRAW> {
	RConnection re;
	FileSystem dstFS = null;
	String temppfx = null;
	File tempdir = null;
	Path dstPath= null;
	RXWritableRAW onekeyasWritable = null;
	boolean copystuff;
	RXWritableRAW rw ;
	public void setup(Context ctx) throws IOException, InterruptedException{
	try{
	    Configuration cfg = ctx.getConfiguration();
	    re =  new RConnection("127.0.0.1",cfg.getInt("rhipejob.lapply.rport",8888));
	    re.assign("v___",new REXPInteger(Utils.sdd()));
	    re.voidEval("set.seed(v___);rm(v___)");
	    byte[] lapplfunc, sconfig;
	    uniWritable b= new uniWritable();
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(cfg),
						     cfg.get("rhipejob.lapply.mapfile"),cfg);
	    lapplfunc =( (uniWritable)mrd.get(new Text("rhipejob.lapply.serializedfunction"), b)).
		getBytes();
	    sconfig = ( (uniWritable)mrd.get(new Text("rhipejob.lapply.serializedconfig"), b)).
		getBytes();
	    mrd.close();

	    re.assign("lapply.function",new REXPRaw( lapplfunc ));
	    re.voidEval("lapply.function=unserialize(lapply.function)");
	    re.assign("configbytes",new REXPRaw( sconfig ));
	    Utils.loadFuncs(re);
	    
	    REXP onekey =  re.eval("rhsz(1)");
	    onekeyasWritable = new RXWritableRAW();
	    onekeyasWritable.set(onekey);

	    dstFS =FileSystem.get(cfg);
	    String fsep= System.getProperty("file.separator");
	    dstPath = new Path(cfg.get("rhipejob.lapply.routput"));
	    temppfx =System.getProperty("java.io.tmpdir");
	    re.voidEval("setwd('"+temppfx+fsep+".."+"')");
	    copystuff = cfg.getInt("rhipejob.copy.to.dfs",1) == 1? true : false;

	    if ( !temppfx.endsWith(fsep))
		temppfx = temppfx +fsep;
	    tempdir = new File(temppfx);

	    re.voidEval("configure=unserialize(configbytes);rm(configbytes)");
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== CONFIGURE ERROR ====",Utils.ERRTYPE.CONFIG);
	    Utils.showStdout(rl,"==== CONFIGURE STANDARD OUTPUT ====");
	    re.voidEval("..srhuz=function(r) if(is.raw(r)) return(rhuz(r)) else return(r)");
	    re.assign("...lapplyexp...",
		      "tryCatch(list(s=capture.output(ret<-lapply(lapply(mapdata,function(r){ do.call('lapply.function',list(..srhuz(r)))}),rhsz)),v=ret),error=function(ex){ list(e=paste(ex))})");
	    re.voidEval("...lapplyexp...=parse(text=...lapplyexp...)");
	    rw = new RXWritableRAW(); 
	}catch (Exception e) {
	    throw new IOException(e);
	}
    }
    /********************************************************
     * This works nicely assuming tmp folder is always ./tmp
     * Users could change this in hadoop via mapred.child.tmp
     * and i would have to pass a variable to the R code
     * indicating name of the 'tmp' folder.
     *******************************************************/
	public void cleanup(Context ctx) throws IOException,InterruptedException{
	try{
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

    public void run(Context context)
         throws IOException,
	InterruptedException{
	setup(context);
	REXPGenericVector keyVector,valueVector;
	REXP rkey,rvalue;
	ArrayList<REXP> keyCollector =  new ArrayList<REXP>(),
	    valueCollector = new ArrayList<REXP>();
	keyCollector.ensureCapacity(100000);
	valueCollector.ensureCapacity(100000);
	while (context.nextKeyValue()) {
	    rkey = ((RXWritable)(context.getCurrentKey())).getREXP();
	    rvalue = ((RXWritable)(context.getCurrentValue())).getREXP();
	    keyCollector.add( rkey );
	    valueCollector.add( rkey );
	}
	keyCollector.trimToSize();
	valueCollector.trimToSize();
	try{
	    RList lapplyList = new RList(valueCollector);
	    REXPGenericVector lapplyVector = new REXPGenericVector(lapplyList);
	    re.assign("mapdata",lapplyVector);
	    RList resultsList = re.eval("eval(...lapplyexp...)").asList();
	    Utils.showError(re,resultsList,"==== LAPPLY ERROR ====",Utils.ERRTYPE.LAPPLY);
	    Utils.showStdout(resultsList,"==== LAPPLY STANDARD OUTPUT ====");
	    RXWritableRAW tk = new RXWritableRAW();
	    RXWritableRAW tv = new RXWritableRAW();
	    RList valuesList = resultsList.at("v").asList();
	    REXP v;
	    for(int i=0;i< valuesList.size(); i++){
		v = valuesList.at(i);
		tv.set(v);
		tk.set(keyCollector.get(i));
		context.write(tk,tv);
	    }
	}catch(RserveException e) {
	    e.printStackTrace();
	    throw new IOException(e);
	}catch(REXPMismatchException e) {
	    e.printStackTrace();
	    throw new IOException(e);
	}
	cleanup(context);
    }
    
    }

    static class LApplyReducer extends  Reducer<RXWritableRAW, RXWritableRAW, RXWritableRAW, 
		   RXWritableRAW> {
	public void setup(Context ctx) throws IOException,InterruptedException {}

	public void cleanup(Context ctx) throws IOException,InterruptedException {}

	public void reduce(RXWritableRAW key, Iterator<RXWritableRAW> values,
			   Context ctx) 
	    throws IOException,InterruptedException {
		while(values.hasNext()) {
		    ctx.write( key, values.next());
		    ctx.progress();
		}
	}
    }

    public static Job createJob(Configuration defaults,String mapfile,String tmpinput,String local) 
	throws URISyntaxException,IOException
    {

	try{
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(defaults),mapfile,defaults);
	    uniWritable a = new uniWritable();
	    int listlength = ((uniWritable)mrd.get(new Text("rhipejob.lapply.lengthofinput"),a))
		.getInt();
	    int listobjectneeded = ((uniWritable)mrd.get(new Text("rhipejob.lapply.listneeded"), a))
		.getInt();
	    MapWritable mapredopts =  ((uniWritable)mrd.get(new Text("rhipejob.lapply.mapredopts"), a))
		.getMap();
	    int rport =((uniWritable) mrd.get(new Text("rhipejob.lapply.rport"),a)).getInt();
	    String output_folder = ((uniWritable)mrd.get(new Text("rhipejob.lapply.routput"),a))
		.getString();
	    String[] sharedfiles = ((uniWritable) mrd.get(new Text("rhipejob.lapply.shared.files"), a))
		.getStrings();
	    String uid = ((uniWritable)mrd.get(new Text("rhipejob.lapply.uid"), a)).
		getString();
	    int delOutOnEnd = ((uniWritable)mrd.get(new Text("rhipejob.lapply.wdelout"),a)).getInt();

	    defaults.set("rhipejob.lapply.uid",uid);
	    defaults.set("rhipejob.lapply.mapfile",mapfile);
	    defaults.setInt("rhipejob.lapply.lengthofinput",listlength);
	    defaults.setInt("rhipejob.lapply.listneeded",listobjectneeded ) ;
	    defaults.setInt("rhipejob.lapply.rport",rport);
	    defaults.setInt("rhipejob.lapply.wdelout",delOutOnEnd);
	    if(sharedfiles ==null) sharedfiles=new String[]{};
	    for(String p : sharedfiles){
		if(p.length()>1) DistributedCache.addCacheFile(new URI(p),defaults);
	    }

	    Set<Writable> keySet = mapredopts.keySet();
	    Iterator<Writable> it = keySet.iterator();
	    while( it!=null && it.hasNext()){
		Text k = (Text)it.next();
		Text v = (Text) mapredopts.get(k);
		defaults.set(k.toString(), v.toString());
	    }
	    defaults.setInt("mapred.task.timeout",0);
	    DistributedCache.createSymlink(defaults);
	    if(output_folder!=null && !output_folder.equals(""))
		defaults.set("rhipejob.lapply.routput", output_folder+"/");

	    Job job = new Job(defaults);
	    job.setJarByClass(RHLApply.class);
	    job.setJobName("LAPPLY:"+uid);
	    if(output_folder!=null && !output_folder.equals("")){
		Path pp = new Path(output_folder);
		org.apache.hadoop.fs.FileSystem srcFs = org.apache.hadoop.fs.FileSystem.get(defaults);
		srcFs.delete(new Path(output_folder+System.getProperty("file.separator")), true);
		FileOutputFormat.setOutputPath(job,pp );
	    }
	    if(listobjectneeded!=0) 
		job.setInputFormatClass(LApplyInputFormat.class);
	    else{
		FileInputFormat.setInputPaths(job,tmpinput);
		job.setInputFormatClass(SequenceFileInputFormat.class);
	    }
	    job.setMapperClass(LApplyMapper.class);
	    job.setReducerClass(LApplyReducer.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    job.setMapOutputKeyClass(RXWritableRAW.class);
	    job.setMapOutputValueClass(RXWritableRAW.class);
	    job.setOutputKeyClass(RXWritableRAW.class);
	    job.setOutputValueClass(RXWritableRAW.class);
	    return(job);
	}catch(Exception ex){
	    ex.printStackTrace();
	    return(null);
	}
    }

    public static String makeMapFile(Configuration defaults,FileSystem filesystem,int listlength, int listobjectneeded,
				byte[] listobject, byte[] func_serialize,
				byte[] config, String output_folder,
				String[] shared_files, Hashtable hadoop_options0,
				int rport){
	String uid =  UUID.randomUUID().toString();
	String dirName = new String("/tmp/"+uid+".mapfile");
	MapWritable m = new MapWritable();
	Enumeration e=null;
	int deleteOutput = 0;
	if(output_folder.equals("")) {
	    deleteOutput = 1;
	    output_folder = new String("/tmp/"+uid+".work");

	}
	if(hadoop_options0!=null) e=hadoop_options0.keys();
	while(e!=null && e.hasMoreElements() ) {
	    String key = (String) e.nextElement();
	    String value = (String)hadoop_options0.get(key);
	    m.put(new Text(key), new Text(value));
	}
	try{
	    MapFile.Writer.setIndexInterval(defaults, 1);
	    MapFile.Writer writer = new MapFile.Writer(defaults,filesystem,dirName, Text.class, uniWritable.class);
	    writer.append(new Text("rhipejob.lapply.inputlist"), (new uniWritable(listobject)));
	    writer.append(new Text("rhipejob.lapply.lengthofinput"), new uniWritable(listlength));
	    writer.append(new Text("rhipejob.lapply.listneeded"), new uniWritable(listobjectneeded));
	    writer.append(new Text("rhipejob.lapply.mapredopts"), new uniWritable( m));
	    writer.append(new Text("rhipejob.lapply.routput"), new uniWritable( output_folder));
	    writer.append(new Text("rhipejob.lapply.rport"), new uniWritable( rport ));
	    writer.append(new Text("rhipejob.lapply.serializedconfig"), new uniWritable( config));
	    writer.append(new Text("rhipejob.lapply.serializedfunction"), new uniWritable(func_serialize));
	    writer.append(new Text("rhipejob.lapply.shared.files"), new uniWritable( shared_files));
	    writer.append(new Text("rhipejob.lapply.uid"), new uniWritable(uid));
	    writer.append(new Text("rhipejob.lapply.wdelout"), new uniWritable(deleteOutput));
	    writer.close();
	}catch(Exception ex){
	    ex.printStackTrace();
	    try{
		MapFile.delete(FileSystem.get(defaults),dirName);
	    }catch(Exception ey){};
	    return(null);
	}
	return(uid);
    }
 
    public int run(String[] args) throws Exception {
	if (args.length < 4) {
	    System.out.println("rhlapply mapfile verbose inputfile local");
	    System.exit(-1);
	}
	int k;
	Configuration conf = new Configuration();
	Job job;
	try{
	    job = RHLApply.createJob(conf,args[0],args[2],args[3]);
	    k = job.waitForCompletion(true) ? 115 : 120;
	}catch(Exception e){
	    k=-2;
	    e.printStackTrace();
	}finally{
	    try{
		org.apache.hadoop.fs.FileSystem srcFs = org.apache.hadoop.fs.FileSystem.get(conf);
		srcFs.delete(new Path(args[0]), true);
	    }catch(Exception e){
		k=-3;
		e.printStackTrace();
	    }
	}
	return(k);
    }

    public static void main(String[] args)  {

	int res;
	try{
	    res = (new RHLApply()).run(args);
	}catch(Exception ex){
	    ex.printStackTrace();
	    res=101;
	}
	System.exit(res);
    }

}



    




