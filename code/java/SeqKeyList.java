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
package org.saptarshiguha.rhipe.utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.saptarshiguha.rhipe.hadoop.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
public class SeqKeyList {

    private String inputFile;
    private Configuration config;
    private FileSystem filesystem;
    private Path path;
    private SequenceFile.Reader reader;
    private SequenceFile.Writer writer;
    private RXWritableRAW thekey;
    private RXWritableRAW thevalue;
    public SeqKeyList(Configuration config,int local)  {
	try{
// 	    config = new  Configuration();
// 	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR")
// 				    +"/hadoop-default.xml"));
// 	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR")
// 					+"/hadoop-site.xml"));

	    if(local==1){
		//		config.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	    }
	    filesystem = FileSystem.get(config);
	    if(local==1) filesystem = FileSystem.getLocal(config);
	    
	}catch(Exception e){
	    e.printStackTrace();
	}
    }
    
    public boolean newFile(String p,String codectype) {
	try{
	    path = new Path(p);
	    writer = SequenceFile.createWriter(filesystem, config, path, 
					       RXWritableRAW.class, RXWritableRAW.class, 
					       SequenceFile.CompressionType.BLOCK, 
					       (CompressionCodec)Class.forName(codectype).newInstance());
	    return(true);
	}catch(Exception e){
	    e.printStackTrace();
	    return(false);
	}
    }
    public boolean writeKeyValue(byte[] key,byte[] value){
	try{
	    //org.apache.hadoop.io.compress.DefaultCodec for default.
	    RXWritableRAW k=new RXWritableRAW();
	    RXWritableRAW v = new RXWritableRAW();
	    k.set(key);v.set(value);
	    writer.append(k,v);
	    return true;
	}catch(Exception e){
	    e.printStackTrace();
	    return false;
	}
    }
    public boolean setFile(String p) {
	try{
	    path = new Path(p);
	    reader = new SequenceFile.Reader(filesystem, path, config);
	    return(true);
	}catch(Exception e){
	    e.printStackTrace();
	    return(false);
	}
    }
    public boolean closeFile(){
	try{
	    if(reader!=null) reader.close();
	    if(writer!=null) writer.close();
	    return(true);
	}catch(Exception e){
	    e.printStackTrace();
	    return(false);
	}
    }

    public boolean nextKV(){
	thekey = new RXWritableRAW();
	thevalue = new RXWritableRAW();
	try{
	    boolean v=reader.next(thekey,thevalue);
	    if(v)
		return(true);
	    else return(false);
	}catch(Exception e){
	    e.printStackTrace();
	    return(false);
	}
    }

    public byte[] theKey() {
	return(thekey.get_bytes());
    }
    public byte[] theValue()  {
	return(thevalue.get_bytes());
    }

}