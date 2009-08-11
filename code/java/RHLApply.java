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
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Enumeration;
import java.util.Set;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.net.URI;
import java.io.IOException;
import java.io.File;
import java.util.Random;
import java.security.SecureRandom;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
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
    

public class RHLApply extends Configured implements Tool {
    public RHLApply(){}
    //Given a random int, between 0 and B (inclusive)
    //Convert to 0,B-1 without generating another number.

    static class LApplyMapper extends MapReduceBase
	implements Mapper<RXWritable,RXWritable,RXWritableRAW,RXWritableRAW> {
	LongWritable thekey = new LongWritable(1L);
	BytesWritable val = new BytesWritable();
	RConnection re;
	int numreduce;
	FileSystem dstFS = null;
	String temppfx = null;
	File tempdir = null;
	Path dstPath= null;
	REXP onekey;
	RXWritableRAW onekeyasWritable = null;
	boolean isfromsqfile= false;
	boolean copystuff;
    public void configure(JobConf job){
	REXP cmderrex = null;
	String[] rout = null;
	try{

	    re =  new RConnection("127.0.0.1",job.getInt("rhipejob.lapply.rport",8888));
// 	    re.assign("v___",new REXPInteger(10)); //Utils.sdd()));
	    re.assign("simplifychar",new REXPInteger(job.getInt("rhipejob.charsxp.short",0)));
// 	    re.voidEval("set.seed(v___)");
	    byte[] lapplfunc, sconfig;
	    uniWritable b= new uniWritable();
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(job),job.get("rhipejob.lapply.mapfile"),job);
	    lapplfunc =( (uniWritable)mrd.get(new Text("rhipejob.lapply.serializedfunction"), b)).getBytes();
	    sconfig = ( (uniWritable)mrd.get(new Text("rhipejob.lapply.serializedconfig"), b)).getBytes();
	    if(lapplfunc == null || sconfig ==null) throw new Exception("[RHIPE]: serialized bytes for lapply function or configure are null");
	    mrd.close();
	    isfromsqfile = job.getInt("rhipejob.lapply.listneeded",0)==0? true: false;
	    re.assign("lapply.function",new REXPRaw( lapplfunc ));
	    re.voidEval("lapply.function=unserialize(lapply.function)");
	    re.assign("configbytes",new REXPRaw( sconfig ));
	    Utils.loadFuncs(re);
	    
	    onekey =  re.eval("rhsz(1)");
	    onekeyasWritable = new RXWritableRAW();
	    onekeyasWritable.set(onekey);
	    dstFS = FileOutputFormat.getOutputPath(job).getFileSystem(job);
	    String fsep= System.getProperty("file.separator");
	    dstPath = new Path(job.get("rhipejob.lapply.routput"));
	    temppfx =System.getProperty("java.io.tmpdir");
	    re.voidEval("setwd('"+temppfx+fsep+".."+"')");
	    copystuff = job.getInt("rhipejob.copy.to.dfs",1) == 1? true : false;

	    if ( !temppfx.endsWith(fsep))
		temppfx = temppfx +fsep;
	    tempdir = new File(temppfx);

	    re.voidEval("configure=unserialize(configbytes);");
// 	    re.voidEval("for(x... in ls(preload$env)) assign(x...,get(x...,envir=preload$env),envir=.GlobalEnv)");
	    RList rl = re.eval("tryCatch(list(s=capture.output(ret <-eval(configure,envir=.GlobalEnv)),v=ret),error=function(ex){ list(e=paste(ex))})").asList();
	    Utils.showError(re,rl,"==== CONFIGURE ERROR ====",Utils.ERRTYPE.CONFIG);
	    Utils.showStdout(rl,"==== CONFIGURE STANDARD OUTPUT ====");
	    re.assign("...lapplyexp...",
			  "tryCatch(list(s=capture.output(ret<-rhsz(lapply.function(lapply.input))),v=ret)"+
			  ",error=function(ex){ list(e=paste(ex))})");
	    re.voidEval("...lapplyexp...=parse(text=...lapplyexp...)");

	}catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }
    /********************************************************
     * This works nicely assuming tmp folder is always ./tmp
     * Users could change this in hadoop via mapred.child.tmp
     * and i would have to pass a variable to the R code
     * indicating name of the 'tmp' folder.
     *******************************************************/
    public void close() throws IOException{
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

    public void map(RXWritable key, RXWritable value,
		    OutputCollector<RXWritableRAW,RXWritableRAW> output, 
		    Reporter reporter)  throws IOException {
	try{
	    re.assign("lapply.input",value.getREXP());
	    if(value.getREXP().isRaw()) re.voidEval("lapply.input=rhuz(lapply.input)");
	    RList rl = re.eval("eval(...lapplyexp...)").asList();
	    Utils.showError(re,rl,"==== LAPPLY ERROR ====",Utils.ERRTYPE.LAPPLY);
	    Utils.showStdout(rl,"==== LAPPLY STANDARD OUTPUT ====");
	    RXWritableRAW rw = new RXWritableRAW(); rw.set(rl.at("v"));
	    if(key instanceof RXWritableLong)
		output.collect(onekeyasWritable,rw);
	    else
		output.collect((RXWritableRAW)key,rw);
	    reporter.progress();
	} catch(RserveException e) {
	    throw new IOException(e);
	} catch(REXPMismatchException e) {
	    throw new IOException(e);
	}
    }
    }

    static class LApplyReducer extends MapReduceBase 
	implements Reducer<RXWritableRAW, RXWritableRAW, RXWritableRAW, 
		   RXWritableRAW> {
	public void configure(JobConf job){}

	public void close() throws IOException{}

	public void reduce(RXWritableRAW key, Iterator<RXWritableRAW> values,
			   OutputCollector<RXWritableRAW, 
			   RXWritableRAW> output, Reporter reporter) 
	    throws IOException {
		while(values.hasNext()) {
		    output.collect( key, values.next());
		    reporter.progress();
		}
	}
    }

    public static JobConf createConf(Configuration defaults,String mapfile,String tmpinput,String local) 
	throws URISyntaxException,IOException
    {
	JobConf jobConf = new JobConf(defaults, RHLApply.class);
	if(local.equals("local")){
	    //	    jobConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	}
	try{
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(jobConf),mapfile,jobConf);
	    uniWritable a = new uniWritable();
	    int listlength = ((uniWritable)mrd.get(new Text("rhipejob.lapply.lengthofinput"),a)).getInt();
	    int listobjectneeded = ((uniWritable)mrd.get(new Text("rhipejob.lapply.listneeded"), a)).getInt();
	    MapWritable mapredopts =  ((uniWritable)mrd.get(new Text("rhipejob.lapply.mapredopts"), a)).getMap();
	    int rport =((uniWritable) mrd.get(new Text("rhipejob.lapply.rport"),a)).getInt();
	    String output_folder = ((uniWritable)mrd.get(new Text("rhipejob.lapply.routput"),a)).getString();
	    String[] sharedfiles = ((uniWritable) mrd.get(new Text("rhipejob.lapply.shared.files"), a)).getStrings();
	    String uid = ((uniWritable)mrd.get(new Text("rhipejob.lapply.uid"), a)).getString();
	    int delOutOnEnd = ((uniWritable)mrd.get(new Text("rhipejob.lapply.wdelout"),a)).getInt();
	    jobConf.setJobName("LAPPLY:"+uid);
	    jobConf.set("rhipejob.lapply.uid",uid);
	    jobConf.set("rhipejob.lapply.mapfile",mapfile);
	    jobConf.setInt("rhipejob.lapply.lengthofinput",listlength);
	    jobConf.setInt("rhipejob.lapply.listneeded",listobjectneeded ) ;
	    jobConf.setInt("rhipejob.lapply.rport",rport);
	    jobConf.setInt("rhipejob.lapply.wdelout",delOutOnEnd);
	    if(sharedfiles ==null) sharedfiles=new String[]{};
	    for(String p : sharedfiles){
		if(p.length()>1) DistributedCache.addCacheFile(new URI(p),jobConf);
	    }

	    Set<Writable> keySet = mapredopts.keySet();
	    Iterator<Writable> it = keySet.iterator();
	    while( it!=null && it.hasNext()){
		Text k = (Text)it.next();
		Text v = (Text) mapredopts.get(k);
		jobConf.set(k.toString(), v.toString());
	    }
	    if(output_folder!=null && !output_folder.equals("")){
		Path pp = new Path(output_folder);
		org.apache.hadoop.fs.FileSystem srcFs = (new Path(output_folder+System.getProperty("file.separator"))).getFileSystem(jobConf);
		srcFs.delete(pp, true);
		FileOutputFormat.setOutputPath(jobConf,pp );
		jobConf.set("rhipejob.lapply.routput", output_folder+"/"); 
	    }
// 	    jobConf.setInt("mapred.task.timeout",0);
	    DistributedCache.createSymlink(jobConf);
	    if(listobjectneeded!=0) 
		jobConf.setInputFormat(LApplyInputFormat.class);
	    else{
		FileInputFormat.setInputPaths(jobConf,tmpinput);
		jobConf.setInputFormat(SequenceFileInputFormat.class);
	    }
	    jobConf.setMapperClass(LApplyMapper.class);
	    jobConf.setReducerClass(LApplyReducer.class);
	    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
	    jobConf.setMapOutputKeyClass(RXWritableRAW.class);
	    jobConf.setMapOutputValueClass(RXWritableRAW.class);
	    jobConf.setOutputKeyClass(RXWritableRAW.class);
	    jobConf.setOutputValueClass(RXWritableRAW.class);
	    return(jobConf);
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
// 	Configuration defaults = new Configuration();
//	JobConf jobConf = new JobConf(defaults);
// 	jobConf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/hadoop-site.xml"));
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
	    ToolRunner.printGenericCommandUsage(System.out);
	    return -1;
	}
	Configuration conf = getConf();
	boolean verb = Boolean.valueOf(args[1]);
	JobConf jc = RHLApply.createConf(conf,args[0],args[2],args[3]);
	int k=0;
	RunningJob rjb=null;
	int delWonOut = jc.getInt("rhipejob.lapply.wdelout",0);
	try{
	    if(verb) {
		rjb = JobClient.runJob(jc);
	    }else{
		JobClient jcl = new JobClient(jc);
		rjb = jcl.submitJob(jc);
		System.out.println("[RHIPE] Job ID: "+rjb.getID().toString());
		System.out.println("[RHIPE] Job Name: "+rjb.getJobName());
		System.out.println("[RHIPE] Job URL: "+rjb.getTrackingURL());
		rjb.waitForCompletion();
	    }
	    k = rjb.isSuccessful() ? 115: 120;
	}catch(Exception e){
	    throw new Exception(e);
	}finally{
	    //FileSystem.get(jc).delete(new Path(args[0]), true);
	    //if(delWonOut==1) FileSystem.get(jc).delete(new Path(jc.get("rhipejob.lapply.routput")),true);
	}
	return(k);
    }
    public static void main(String[] args)  {
	int res;
	try{
	    res = ToolRunner.run(new Configuration(), new RHLApply(), args);
	}catch(Exception ex){
	    ex.printStackTrace();
	    res=101;
	}
	System.exit(res);
    }

    
}
    




