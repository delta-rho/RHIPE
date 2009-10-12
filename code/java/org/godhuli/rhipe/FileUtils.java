/**
 * Copyright 2009 Saptarshi Guha
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;
import java.io.Writer;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.io.FileOutputStream;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.EOFException;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Calendar;
import java.net.URI;
import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos.REXP.RClass;
import java.util.*;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.WritableUtils;

import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

public class FileUtils {
    private FsShell fsshell;
    private Configuration cfg;
    private static final SimpleDateFormat formatter = 
	new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final String fsep="\t";
    public FileUtils(Configuration cfg){
	this.cfg = cfg;
	fsshell = new FsShell(cfg);
    }

    public Configuration getConf(){
	return(cfg);
    }
    public FsShell getFsShell(){
	return fsshell;
    }
    public void copyFromLocalFile(String[] src,String dst,boolean overwrite)
    throws IOException{
	Path dstPath = new Path(dst);
	FileSystem dstFs = dstPath.getFileSystem(cfg);
	Path[] srcp = new Path[src.length];
	for(int i = 0;i<src.length;i++)
	    srcp[i] = new Path(src[i]);
	dstFs.copyFromLocalFile(false, overwrite, srcp, dstPath);
    }
    static final String COPYTOLOCAL_PREFIX = "_copyToLocal_";

    public void copyToLocal(FileSystem srcFS,Path src, File dst) 
	throws IOException{
	if (!srcFS.getFileStatus(src).isDir()) {
	    File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(),
						    COPYTOLOCAL_PREFIX, true);
	    if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
		throw new IOException("Failed to copy " + src + " to " + dst); 
	    }
      
	    if (!tmp.renameTo(dst)) {
		throw new IOException("Failed to rename tmp file " + tmp + 
				      " to local destination \"" + dst + "\".");
	    }
	} else {
	    dst.mkdirs();
	    for(FileStatus path : srcFS.listStatus(src)) {
		copyToLocal(srcFS,path.getPath(), 
			    new File(dst, path.getPath().getName()));
	    }
	}
    }


    public void makeFolderToDelete(String s) throws IOException{
	Path p = new Path(s);
	FileSystem fs = FileSystem.get(cfg);
	fs.mkdirs(p);
	fs.deleteOnExit(p);
    }


    public  void copyMain(String src,String dest) throws IOException{
	File dst = new File(dest);      
	Path srcpath = new Path(src);
	FileSystem srcFS = FileSystem.get(cfg);
	FileStatus[] srcs = srcFS.globStatus(srcpath);
	boolean dstIsDir = dst.isDirectory(); 
	if (srcs.length > 1 && !dstIsDir) {
	    throw new IOException("When copying multiple files, "
				  + "destination should be a directory.");
	}
	for (FileStatus status : srcs) {
	    Path p = status.getPath();
	    File f = dstIsDir? new File(dst, p.getName()): dst;
	    copyToLocal(srcFS, p, f);
	}
    }

    public String ls(String path) throws IOException,FileNotFoundException{
	FileSystem srcFS = FileSystem.get(cfg);
	Path spath = new Path(path);
	FileStatus[] srcs;
	srcs = srcFS.globStatus(spath);
	if (srcs==null || srcs.length==0) {
	    throw new FileNotFoundException("Cannot access " + path + 
					    ": No such file or directory.");
	}
	if(srcs.length==1 && srcs[0].isDir())
	    srcs = srcFS.listStatus(spath);
	StringBuilder sb = new StringBuilder();
	Calendar c =  Calendar.getInstance();

	for(FileStatus status : srcs){
	    String x = status.isDir() ? "d" : "-";
	    sb.append(x);
	    sb.append(status.getPermission().toString());
	    sb.append(fsep);

	    sb.append(status.getOwner());
	    sb.append(fsep);

	    sb.append(status.getGroup());
	    sb.append(fsep);

	    sb.append(status.getLen());
	    sb.append(fsep);
	    
	    Date d = new Date(status.getModificationTime());
	    sb.append(formatter.format(d));
	    sb.append(fsep);

	    sb.append(status.getPath().toUri().getPath());
	    sb.append("\n");
	}
	return(sb.toString());
    }

    public void delete(String srcf, final boolean recursive) throws IOException {
	Path srcPattern = new Path(srcf);
	new DelayedExceptionThrowing() {
	    @Override
		void process(Path p, FileSystem srcFs) throws IOException {
		delete(p, srcFs, recursive);
	    }
	}.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
    }
    
  /* delete a file */
    private void delete(Path src, FileSystem srcFs, boolean recursive) 
	throws IOException {
	if (srcFs.isDirectory(src) && !recursive) {
	    throw new IOException("Cannot remove directory \"" + src +
				  "\", use -rmr instead");
	}
	Trash trashTmp = new Trash(srcFs, getConf());
	if (trashTmp.moveToTrash(src)) {
	    System.out.println("Moved to trash: " + src);
	    return;
	}
	if (srcFs.delete(src, true)) {
	    System.out.println("Deleted " + src);
	} else {
	    if (!srcFs.exists(src)) {
		throw new FileNotFoundException("cannot remove "
						+ src + ": No such file or directory.");
	    }
	    throw new IOException("Delete failed " + src);
	}
    }
    
    
    private abstract class DelayedExceptionThrowing {
	abstract void process(Path p, FileSystem srcFs) throws IOException;
	
	final void globAndProcess(Path srcPattern, FileSystem srcFs
				  ) throws IOException {
	    ArrayList<IOException> exceptions = new ArrayList<IOException>();
	    for(Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern), 
					     srcPattern))
		try { process(p, srcFs); } 
		catch(IOException ioe) { exceptions.add(ioe); }
    
	    if (!exceptions.isEmpty())
		if (exceptions.size() == 1)
		    throw exceptions.get(0);
		else 
		    throw new IOException("Multiple IOExceptions: " + exceptions);
	}
    }
    private REXP readInfo(String file) throws IOException{
	DataInputStream in = new 
	    DataInputStream(new FileInputStream(file));
	return(REXP.parseFrom(in));
    }

    private  REXP mapredopts() throws Exception{
	Iterator<Map.Entry<String,String>> iter = cfg.iterator();
	Vector<REXP> ent = new Vector<REXP>();
	Vector<String> str = new Vector<String>();
	while(iter.hasNext()){
	    Map.Entry<String,String> c = iter.next();
	    String key = c.getKey();
	    String value = c.getValue();
	    str.add( key);
	    ent.add( RObjects.makeStringVector( value ));
	}
	return(RObjects.makeList(str,ent));
    }

    private static String getStackTrace(Throwable aThrowable) {
	final Writer result = new StringWriter();
	final PrintWriter printWriter = new PrintWriter(result);
	aThrowable.printStackTrace(printWriter);
	return result.toString();
    }
    public void binary2sequence(REXP rexp0) throws Exception{
	String tf= rexp0.getStringValue(0).getStrval();
	String ofold= rexp0.getStringValue(1).getStrval();
	int groupsize = Integer.parseInt(rexp0.getStringValue(2).getStrval());
	int howmany = Integer.parseInt(rexp0.getStringValue(3).getStrval());
	int N = Integer.parseInt(rexp0.getStringValue(4).getStrval());
	DataInputStream in = new 
	    DataInputStream(new FileInputStream(tf));
	int count=0;
	for(int i=0;i < howmany-1;i++){
	    String f = ofold+"/"+i;
	    RHWriter w = new RHWriter(f,cfg);
	    w.doWriteFile(in,groupsize);
	    count=count+groupsize;
	    w.close();
	}
	if(count < N){
	    count=N-count;
	    String f = ofold+"/"+(howmany-1);
	    RHWriter w = new RHWriter(f,cfg);
	    w.doWriteFile(in,count);
	    w.close();
	}
    }

    public void sequence2binary(REXP rexp0) throws Exception{
	int n = rexp0.getStringValueCount();
	String[] infile = new String[n-2];
	String ofile = rexp0.getStringValue(n-2).getStrval();
	int local = Integer.parseInt(rexp0.getStringValue(n-1).getStrval());
	for(int i=0;i< n-2;i++) infile[i] = rexp0.getStringValue(i).getStrval();
	S2B s = new S2B();
	if(!s.runme( infile, ofile,local==1 ? true:false)){
	    throw new Exception("Could not convert sequence to binary");
	}
    }
    public void writeTo(String ofile, REXP b) {
	try{
	    DataOutputStream out = new 
	    DataOutputStream(new FileOutputStream(ofile));
	    byte[] bytes = b.toByteArray();
	    out.writeInt( bytes.length);
	    out.write(bytes);
	    out.close();
	}catch(Exception e){e.printStackTrace();}
    }

    public static void main(String[] args) throws Exception{
	int cmd = Integer.parseInt(args[0]);
	//parse data
	//invokes class CMD inputfile
	//writes results(or erors) to inputfile
	REXP r;
	REXP b=null;
	boolean error = false;
	FileUtils fu= new FileUtils(new Configuration());
	try{
	    switch(cmd){
	    case 0:
		// hadoop options
		b = fu.mapredopts();
		fu.writeTo(args[1], b);
		break;
	    case 1:
		// ls
		r = fu.readInfo(args[1]);
		String folder = r.getStringValue(0).getStrval();
		String result0 = fu.ls(folder);
		b = RObjects.makeStringVector(result0);
		fu.writeTo(args[1], b);
		break;
	    case 2:
		//copy from hdfs to local
		r = fu.readInfo(args[1]);
		String src = r.getStringValue(0).getStrval();
		String dest = r.getStringValue(1).getStrval();
		fu.copyMain(src,dest);
		break;
	    case 3:
		//delete from the hdfs
		r = fu.readInfo(args[1]);
		String s = r.getStringValue(0).getStrval();
		fu.delete(s,true);
		break;
	    case 4:
		//copy local files to hdfs
		r = fu.readInfo(args[1]);
		String[] locals = new String[r.getRexpValue(0).getStringValueCount()];
		for(int i=0;i<locals.length;i++) locals[i] = r.getRexpValue(0).
						getStringValue(i).getStrval();
		String dest2 = r.getRexpValue(1).getStringValue(0).getStrval();
		REXP.RBOOLEAN overwrite_ = r.getRexpValue(2).getBooleanValue(0);
		boolean overwrite;
		if(overwrite_==REXP.RBOOLEAN.F)
		    overwrite=false;
		else if(overwrite_==REXP.RBOOLEAN.T)
		    overwrite=true;
		else
		    overwrite=false;
		fu.copyFromLocalFile(locals,dest2,overwrite);
		break;
	    case 5:
		r = fu.readInfo(args[1]);
		fu.binary2sequence(r);
		break;
	    case 6:
		r = fu.readInfo(args[1]);
		fu.sequence2binary(r);
		break;
	    }
	    
	}catch(Exception e){
	    e.printStackTrace();
	    String x = getStackTrace(e);
	    error=true;
	    b = RObjects.makeStringVector(x);
	    fu.writeTo(args[1], b);
	}
	System.exit(error? 1:0);
    }
}
