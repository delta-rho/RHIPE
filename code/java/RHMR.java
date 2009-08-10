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
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.GenericOptionsParser;
import org.rosuda.REngine.*;
import org.rosuda.REngine.Rserve.*;
import org.apache.commons.codec.binary.*;
import org.rosuda.REngine.Rserve.protocol.REXPFactory;


import org.saptarshiguha.rhipe.utils.*;

public class RHMR  {
    public RHMR(){}
    public static Job createJob(Configuration defaults,String mapfile) 
	throws URISyntaxException,IOException
    {
	try{
	    MapFile.Reader mrd = new MapFile.Reader( FileSystem.get(defaults),mapfile,defaults);
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
		defaults.set(k.toString(), v.toString());
	    }
	    defaults.set("rhipejob.mapfile",mapfile);
	    if(sharedfiles != null) {
		for(String p : sharedfiles)
		    if(p.length()>1) DistributedCache.addCacheFile(new URI(p),defaults);
	    }

	    String output_folder = defaults.get("rhipejob.output.folder");
	    DistributedCache.createSymlink(defaults);
	    defaults.setInt("mapred.task.timeout",0);
	    String opformat = defaults.get("mapred.output.format.class");
	    if(output_folder!=null && !output_folder.equals("")){
	    		defaults.set("rhipejob.output.folder", output_folder+"/"); 
	    }

	    Job job = new Job(defaults);
	    job.setJobName("MR:"+defaults.get("rhipejob.uid"));
	    job.setJarByClass(RHMR.class);
	    FileInputFormat.setInputPaths(job,defaults.get("rhipejob.input.folder"));
	    job.setMapperClass(RHMRMapper.class);
	    job.setReducerClass(RHMRReducer.class);

	    String ifclassStr = defaults.get("rhipejob.input.format.class");
	    String ofclassStr = defaults.get("rhipejob.output.format.class");
	    Class c1 = defaults.getClassByName(ifclassStr),c2 = defaults.getClassByName(ofclassStr);
	    Class<? extends InputFormat>  ifclass = c1.asSubclass(InputFormat.class);
	    Class<? extends OutputFormat> ofclass = c2.asSubclass(OutputFormat.class);
	    job.setInputFormatClass(ifclass);
	    job.setOutputFormatClass(ofclass);


	    if(defaults.getInt("rhipejob.needcombiner",0)==1) 
		job.setCombinerClass(RHMRCombiner.class);
	    if(output_folder!=null && !output_folder.equals("")){
		Path pp = new Path(output_folder);
		org.apache.hadoop.fs.FileSystem srcFs = (new Path(output_folder+System.
								  getProperty("file.separator"))).
		    getFileSystem(defaults);
		srcFs.delete(pp, true);
		FileOutputFormat.setOutputPath(job,pp );
	    }

	    job.setOutputKeyClass(Class.forName(defaults.
						get("rhipejob.outputformat.keyclass")));
	    job.setOutputValueClass(Class.forName(defaults.
						  get("rhipejob.outputformat.valueclass")));
	    
	    if(Integer.parseInt(defaults.get("mapred.reduce.tasks"))==0){
		job.setMapOutputKeyClass(Class.forName(defaults.
						       get("rhipejob.outputformat.keyclass")));
		job.setMapOutputValueClass(Class.forName(defaults.
							 get("rhipejob.outputformat.valueclass")));
	    }else{
		job.setMapOutputKeyClass(RXWritableRAW.class);
		job.setMapOutputValueClass(RXWritableRAW.class);
	    }
	    return(job);
	}catch(Exception ex){
	    ex.printStackTrace();
	    return(null);
	}
    }
    public static String makeMapFile(Configuration defaults, FileSystem filesystem,
				     byte[] mappers,byte[] reducers,
				     byte[] config,byte[] cloze, 
				     String[] shared_files, Hashtable<Object,Object> hadop){
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


    public static void main(String[] args)  {
	RHMR rh = new RHMR();
	int k;
	Configuration conf = new Configuration();
	Job job;
	try{
	    job = RHMR.createJob(conf,args[0]);
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
	System.exit(k);
	
    }
}




