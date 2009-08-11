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
import org.apache.hadoop.util.GenericOptionsParser;
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

public class RHMR extends Configured implements Tool {
    public RHMR(){}
    public static JobConf createConf(Configuration defaults,String mapfile) 
	throws URISyntaxException,IOException
    {
	JobConf jobConf = new JobConf(defaults, RHMR.class);
	jobConf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/hadoop-site.xml"));
	try{
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(jobConf),mapfile,jobConf);
	    uniWritable a = new uniWritable();
	    MapWritable mapredopts =  ((uniWritable)mrd.get(new Text("rhipejob.mapredopts"), a)).
		getMap();
	    String[] sharedfiles = ((uniWritable) mrd.get(new Text("rhipejob.shared.files"), a)).
		getStrings();

	    Set<Writable> keySet = mapredopts.keySet();
	    Iterator<Writable> it = keySet.iterator();
	    while( it!=null && it.hasNext()){
		Text k = (Text)it.next();
		Text v = (Text) mapredopts.get(k);
		jobConf.set(k.toString(), v.toString());
	    }
	    Class clz=Class.forName( mapredopts.get(new Text("rhipejob.output.format.class")).toString());
	    jobConf.setOutputFormat(clz);

	    clz=Class.forName( mapredopts.get(new Text("rhipejob.input.format.class")).toString());
	    jobConf.setInputFormat(clz);


	    jobConf.setJobName("MR:"+jobConf.get("rhipejob.uid"));
	    jobConf.set("rhipejob.mapfile",mapfile);
	    if(sharedfiles != null) {
		for(String p : sharedfiles)
		    if(p.length()>1) DistributedCache.addCacheFile(new URI(p),jobConf);
	    }

	    FileInputFormat.setInputPaths(jobConf,jobConf.get("rhipejob.input.folder"));
	    String output_folder = jobConf.get("rhipejob.output.folder");
	    if(output_folder!=null && !output_folder.equals("")){
		Path pp = new Path(output_folder);
		org.apache.hadoop.fs.FileSystem srcFs = (new Path(output_folder+System.getProperty("file.separator"))).getFileSystem(jobConf);
		srcFs.delete(pp, true);
		FileOutputFormat.setOutputPath(jobConf,pp );
		jobConf.set("rhipejob.output.folder", output_folder+"/"); 
	    }
	    DistributedCache.createSymlink(jobConf);
	    jobConf.setInt("mapred.task.timeout",0);
	    jobConf.setMapperClass(RHMRMapper.class);
	    jobConf.setReducerClass(RHMRReducer.class);
	    if(jobConf.getInt("rhipejob.needcombiner",0)==1) 
		jobConf.setCombinerClass(RHMRCombiner.class);
	    

	    jobConf.setOutputKeyClass(Class.forName(jobConf.get("rhipejob.outputformat.keyclass")));
	    jobConf.setOutputValueClass(Class.forName(jobConf.get("rhipejob.outputformat.valueclass")));
	    
	    if(Integer.parseInt(jobConf.get("mapred.reduce.tasks"))==0){
		jobConf.setMapOutputKeyClass(Class.forName(jobConf.get("rhipejob.outputformat.keyclass")));
		jobConf.setMapOutputValueClass(Class.forName(jobConf.get("rhipejob.outputformat.valueclass")));
	    }else{
		jobConf.setMapOutputKeyClass(RXWritableRAW.class);
		jobConf.setMapOutputValueClass(RXWritableRAW.class);
	    }
	    return(jobConf);
	}catch(Exception ex){
	    ex.printStackTrace();
	    return(null);
	}
    }
    public static String makeMapFile(Configuration defaults, FileSystem filesystem,byte[] mappers,byte[] reducers,
				     byte[] config,byte[] cloze, 
				     String[] shared_files, Hashtable<Object,Object> hadop){
// 	Configuration defaults = new Configuration();
// 	JobConf jobConf = new JobConf(defaults);
// 	jobConf.addResource(new Path(System.getenv("HADOOP_CONF_DIR")+"/hadoop-site.xml"));
	String uid =  UUID.randomUUID().toString();
	String dirName = new String("/tmp/"+uid+".mapfile");
	MapWritable m = new MapWritable();
	Enumeration e=null;
	hadop.put("rhipejob.uid",uid);

	if(hadop!=null) e=hadop.keys();
	while(e!=null && e.hasMoreElements() ) {
	    String key = (String) e.nextElement();
	    String value = (String)hadop.get(key);
	    m.put(new Text(key), new Text(value));
	}
	try{
	    MapFile.Writer.setIndexInterval(defaults, 1);
	    MapFile.Writer writer = new MapFile.Writer(defaults,filesystem,dirName, 
						       Text.class, uniWritable.class);
	    writer.append(new Text("rhipejob.mapredopts"), new uniWritable( m));
	    writer.append(new Text("rhipejob.serializedcloze"), new uniWritable( cloze));
	    writer.append(new Text("rhipejob.serializedconfig"), new uniWritable( config));
	    writer.append(new Text("rhipejob.serializedmapper"), new uniWritable(mappers));
	    writer.append(new Text("rhipejob.serializedreducer"), new uniWritable(reducers));
	    writer.append(new Text("rhipejob.shared.files"), new uniWritable( shared_files));
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
//     public int run(String[] args) {
// 	return 0;
//     }
    public int run(String[] args) throws Exception {
	boolean verb = Boolean.valueOf(args[1]);
	Configuration conf = getConf();
	JobConf jc = RHMR.createConf(conf,args[0]);
	int k=0;
	RunningJob rjb=null;
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
	return(k);
    }
    public static void main(String[] args)  {
	int res;
	try{
	    res = ToolRunner.run(new Configuration(), new RHMR(), args);
	}catch(Exception ex){
	    ex.printStackTrace();
	    res=101;
	}
	System.exit(res);
    }
}




