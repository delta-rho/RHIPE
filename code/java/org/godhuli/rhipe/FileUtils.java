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
import org.apache.hadoop.io.Writable;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.BufferedOutputStream;
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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Job;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.SequenceFile;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileUtils {
    private FsShell fsshell;
    private Configuration cfg;
    private org.apache.hadoop.mapred.JobConf jobconf;
    private static final SimpleDateFormat formatter = 
	new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final String fsep="\t";
    private org.apache.hadoop.mapred.JobClient jclient;
    protected static final Log LOG = LogFactory.getLog(FileUtils.class
			.getName());

    public FileUtils(Configuration cfg) throws IOException{
	this.cfg = cfg;
	fsshell = new FsShell(cfg);
    	jobconf = new org.apache.hadoop.mapred.JobConf(cfg);
	jclient = new org.apache.hadoop.mapred.JobClient(jobconf);
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

    public String[] ls(REXP r,int f) throws IOException,FileNotFoundException,
    URISyntaxException{
	ArrayList<String> lsco = new ArrayList<String>();
	for(int i=0;i<r.getStringValueCount();i++){
	    String path = r.getStringValue(i).getStrval();
	    ls__(path,lsco,f>0 ? true:false);
	}
	return(lsco.toArray(new String[]{}));
    }

    private void ls__(String path, ArrayList<String> lsco,boolean dorecurse)  throws IOException,FileNotFoundException,URISyntaxException{
	
	Path spath = null;
	spath = new Path(path);
	FileSystem srcFS = spath.getFileSystem(getConf());
	FileStatus[] srcs;
	srcs = srcFS.globStatus(spath);
	if (srcs==null || srcs.length==0) {
	    throw new FileNotFoundException("Cannot access " + path + 
					    ": No such file or directory.");
	}
	if(srcs.length==1 && srcs[0].isDir())
	    srcs = srcFS.listStatus(srcs[0].getPath());
	Calendar c =  Calendar.getInstance();
	for(FileStatus status : srcs){
	    StringBuilder sb = new StringBuilder();
	    boolean idir = status.isDir();
	    String x= idir? "d":"-";
	    if(dorecurse && idir) 
		ls__(status.getPath().toUri().getPath(),lsco,dorecurse);
	    else{
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
		lsco.add(sb.toString());
	    }
	}
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
    
    
    public abstract class DelayedExceptionThrowing {
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
    public REXP readInfo(String file) throws IOException{
	DataInputStream in = new 
	    DataInputStream(new FileInputStream(file));
	return(REXP.parseFrom(in));
    }

    public  REXP mapredopts() throws Exception{
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

    public static String getStackTrace(Throwable aThrowable) {
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
	// System.out.println(rexp0);
	
	int n = rexp0.getStringValueCount();
	String[] infile = new String[n-3];
	String ofile = rexp0.getStringValue(0).getStrval();
	int local = Integer.parseInt(rexp0.getStringValue(1).getStrval());
	int maxnum = Integer.parseInt(rexp0.getStringValue(2).getStrval());
	for(int i=3;i< n;i++) {
	    infile[i-3] = rexp0.getStringValue(i).getStrval();
	}
	
	DataOutputStream cdo = new DataOutputStream(new BufferedOutputStream(System.out,2*1024*1024));
	// System.err.println("Writing output to "+ofile);
	// DataOutputStream cdo = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ofile),2*1024*1024));
	// CodedOutputStream cdo = CodedOutputStream.newInstance(System.out, 2*1024*1024);

	int counter=0;
	boolean endd=false;
	RHBytesWritable k=new RHBytesWritable();
	RHBytesWritable v=new RHBytesWritable();
	for(int i=0; i <infile.length;i++){
	    SequenceFile.Reader sqr = new SequenceFile.Reader(FileSystem.get(cfg) ,new Path(infile[i]), getConf());
	    while(true){
		boolean gotone = sqr.next((Writable)k,(Writable)v);
		// System.out.println("Key="+k);
		if(gotone){
		    counter++;
		    // cdo.writeRawVarint32(k.getLength()); cdo.writeRawBytes(k.getBytes(),0,k.getLength());
		    // cdo.writeRawVarint32(v.getLength()); cdo.writeRawBytes(v.getBytes(),0,v.getLength());
		    k.write(cdo);v.write(cdo);
		    // cdo.flush();
		    // System.err.println(counter+": Key:"+k.getLength()+" Value:"+v.getLength());
		}else break;
		if(maxnum >0 && counter >= maxnum) {
		    endd=true;
		    break;
		}
	    }
	    sqr.close();
	    if(endd) break;
	}
	WritableUtils.writeVInt(cdo,0);
	cdo.flush();
    }


    public void sequence2map(REXP rexp0) throws Exception{
	int n = rexp0.getStringValueCount();
	String[] infile = new String[n-2];
	String ofile = rexp0.getStringValue(n-2).getStrval();
	int local = Integer.parseInt(rexp0.getStringValue(n-1).getStrval());

	for(int i=0;i< n-2;i++) infile[i] = rexp0.getStringValue(i).getStrval();
	S2M s = new S2M();
	if(!s.runme( infile, ofile,local==1 ? true:false)){
	    throw new Exception("Could not convert sequence to mapfile");
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
    public void hdfsrename(REXP rexp0) throws Exception{
	REXP spaths = rexp0.getRexpValue(0); 
	REXP tpaths = rexp0.getRexpValue(1);
	int np = spaths.getRexpValueCount();
	FileSystem fs = FileSystem.get(getConf());
	for(int i=0;i<np;i++){
	    String s = spaths.getStringValue(1).getStrval();
	    String t = tpaths.getStringValue(1).getStrval();
	    Path dstPath = new Path(t);
	    Path srcPath = new Path(s);
	    if(!fs.rename(srcPath,dstPath)) throw new Exception("Error renaming "+s);
	}
    }
    public void getKeys(REXP rexp0) throws Exception{
	getKeys(rexp0,null,false);
    }
    public void getKeys(REXP rexp0, DataOutputStream out,boolean vint) throws Exception{
	REXP keys = rexp0.getRexpValue(0); //keys
	REXP paths = rexp0.getRexpValue(1); //paths to read from
	String tempdest = rexp0.getRexpValue(2).getStringValue(0).getStrval(); //tempdest
	REXP.RBOOLEAN b = rexp0.getRexpValue(3).getBooleanValue(0); //as sequence or binary
	Configuration c=getConf();
	c.setInt("io.map.index.skip",rexp0.getRexpValue(4).getIntValue(0)); //skipindex
	String[] pnames = new String[paths.getStringValueCount()];
	for(int i=0;i< pnames.length;i++){
	    pnames[i] = paths.getStringValue(i).getStrval();
	}
	MapFile.Reader[] mr = RHMapFileOutputFormat.getReaders(pnames,c);

	int numkeys = keys.getRexpValueCount();
	RHBytesWritable k = new RHBytesWritable();
	RHBytesWritable v = new RHBytesWritable();
	// System.out.println(rexp0);
	boolean closeOut = false;
	if(b==REXP.RBOOLEAN.F){ //binary style
	    if(out==null){
		closeOut=true;
		out = new DataOutputStream(new FileOutputStream(tempdest));
	    }
	    for(int i=0; i < numkeys; i++){
		k.set(keys.getRexpValue(i).getRawValue().toByteArray());
		RHMapFileOutputFormat.getEntry(mr,k,v);
		if(!vint){
		    k.writeAsInt(out);
		    v.writeAsInt(out);
		}else{
		    k.write(out);
		    v.write(out);
		}
		// MapFile.Reader rd = RHMapFileOutputFormat.getPartForKey(mr,k,v);
		// while(rd.next(k,v)){
		//     k.writeAsInt(out);
		//     v.writeAsInt(out);
		// }
	    }
	    if (closeOut) out.close();
	    else {
		WritableUtils.writeVInt(out,0);
		out.flush();
	    }
	}else{// these will be written out as a sequence file
	    RHWriter rw = new RHWriter(tempdest,getConf());
	    SequenceFile.Writer w = rw.getSQW();
	    for(int i=0; i < numkeys; i++){
		k.set(keys.getRexpValue(i).getRawValue().toByteArray());
		RHMapFileOutputFormat.getEntry(mr,k,v);
		w.append(k,v);
	    }
	    rw.close();
	}

    }

    
    public REXP getstatus(REXP r) throws Exception{
    	String  jd = r.getRexpValue(0).getStringValue(0).getStrval();
 	boolean  geterrors = r.getRexpValue(1).getIntValue(0)==1? true :false;
    	// org.apache.hadoop.mapreduce.JobID jid = org.apache.hadoop.mapreduce.JobID.forName(jd);
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
    	if(jj==null) 
    	    throw new IOException("Jobtracker could not find jobID: "+jd);
    	// org.apache.hadoop.mapred.JobClient jclient = new org.apache.hadoop.mapred.JobClient(
    	// 	  org.apache.hadoop.mapred.JobTracker.getAddress(c),c);
    	// org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.downgrade(jid);
    	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	if(rj==null)
	    throw new IOException("No such job: "+jd+" available, wrong job? or try the History Viewer (see the Web UI) ");
    	String jobfile = rj.getJobFile();
    	// cfg.addResource(new Path(jobfile));
    	org.apache.hadoop.mapred.Counters cc = rj.getCounters();
    	long startsec = getStart(jclient,jj);
    	double dura = ((double)System.currentTimeMillis()-startsec) /1000;
    	REXP ro = FileUtils.buildlistFromOldCounter(cc, dura );
    	int jobs = rj.getJobState();
    	String jobss = null;
    	if(jobs == JobStatus.FAILED) jobss = "FAILED";
    	else if(jobs == JobStatus.KILLED) jobss="KILLED";
    	else if(jobs == JobStatus.PREP) jobss="PREP";
    	else if(jobs == JobStatus.RUNNING) jobss="RUNNING";
    	else if(jobs == JobStatus.SUCCEEDED) jobss="SUCCEEDED";
    	float mapprog = rj.mapProgress(), reduprog = rj.reduceProgress();
	
    	org.apache.hadoop.mapred.TaskReport[] maptr = jclient.getMapTaskReports( jj);
    	org.apache.hadoop.mapred.TaskReport[] redtr = jclient.getReduceTaskReports( jj);
	
    	int totalmaps = maptr.length, totalreds = redtr.length;
    	int mappending =0,redpending=0, maprunning=0, redrunning=0, redfailed=0,redkilled=0,mapkilled=0, mapfailed=0,mapcomp=0,redcomp=0;
    	for(int i =0;i < maptr.length;i++){
    	    TIPStatus t = maptr[i].getCurrentStatus();
    	    switch(t){
    	    case COMPLETE:
    		mapcomp++;
    		break;
    	    case FAILED:
    		mapfailed++;
    		break;
    	    case PENDING:
    		mappending++;
    		break;
    	    case RUNNING:
    		maprunning++;
    		break;
	    case KILLED:
    		mapkilled++;
    		break;
    	    }
    	}
    	for(int i =0;i < redtr.length;i++){
    	    TIPStatus t = redtr[i].getCurrentStatus();
    	    switch(t){
    	    case COMPLETE:
    		redcomp++;
    		break;
    	    case FAILED:
    		redfailed++;
    		break;
    	    case PENDING:
    		redpending++;
    		break;
    	    case RUNNING:
    		redrunning++;
    		break;
	    case KILLED:
    		redkilled++;
    		break;
    	    }
    	}
	int reduceafails=0,reduceakilled=0,mapafails=0,mapakilled=0;
	int startfrom = 0;
	
	REXP.Builder errcontainer = REXP.newBuilder();
	errcontainer.setRclass(REXP.RClass.STRING);
	while(true){
	    org.apache.hadoop.mapred.TaskCompletionEvent[] events = 
		rj.getTaskCompletionEvents(startfrom); 
	    for(int i=0;i< events.length;i++){
		org.apache.hadoop.mapred.TaskCompletionEvent e =events[i];
		int f=0,k=0;
		switch(e.getTaskStatus()){
		case KILLED:
		    if(e.isMapTask()){mapakilled++;} else { reduceakilled++;} 
		    break;
		case TIPFAILED:
		case FAILED:
		    if(e.isMapTask()){mapafails++;} else { reduceafails++;} 
		    if(geterrors){
			REXPProtos.STRING.Builder content=REXPProtos.STRING.newBuilder();
			String[] s = rj.getTaskDiagnostics(e.getTaskAttemptId());
			if(s!=null && s.length>0){
			    content.setStrval(s[0]);
			    errcontainer.addStringValue(content.build());
			}
		    }
		    break;
		}
	    }
	    startfrom+=events.length;
	    if(events.length==0) break;
	}

    	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobss}));
    	thevals.addRexpValue( RObjects.buildDoubleVector( new double[]{ dura }));
    	thevals.addRexpValue( RObjects.buildDoubleVector( new double[]{ (double)mapprog, (double)reduprog}));
    	thevals.addRexpValue( RObjects.buildIntVector( new int[]{ totalmaps, mappending, maprunning, mapcomp,mapkilled,mapafails,mapakilled}));
    	thevals.addRexpValue( RObjects.buildIntVector( new int[]{ totalreds, redpending, redrunning, redcomp,redkilled,reduceafails,reduceakilled}));
    	thevals.addRexpValue(ro);
    	thevals.addRexpValue( errcontainer);
	thevals.addRexpValue( RObjects.makeStringVector(rj.getTrackingURL()));
    	return(thevals.build());
    }
	
	// System.out.println(rj.getJobName());
    public long getStart(org.apache.hadoop.mapred.JobClient jc,org.apache.hadoop.mapred.JobID jj) throws Exception{
	//this is not needed if i can get a reference to JobTracker (which rtruns the JobStatus for a given JobID)
	org.apache.hadoop.mapred.JobStatus[] jbs = jc.getAllJobs();
	for(int i=0; i< jbs.length;i++){
	    if(jbs[i].getJobID().toString().equals(jj.toString())){
		return(jbs[i].getStartTime());
	    }
	}
	return(0);
    }
    public void killjob(REXP r) throws Exception{
	String  jd = r.getRexpValue(0).getStringValue(0).getStrval();
	// org.apache.hadoop.mapreduce.JobID jid = org.apache.hadoop.mapreduce.JobID.forName(jd);
	// org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.downgrade(jid);
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	rj.killJob();
    }

    public REXP joinjob(REXP r) throws Exception{
	String  jd = r.getRexpValue(0).getStringValue(0).getStrval();
	boolean verbose = r.getRexpValue(1).getStringValue(0).getStrval().equals("TRUE")? true:false;
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	String jobfile = rj.getJobFile();
	// cfg.addResource(new Path(jobfile));
	boolean issuc  = jclient.monitorAndPrintJob(jobconf,rj);
	org.apache.hadoop.mapred.Counters cc = rj.getCounters();
	long startsec = getStart(jclient,jj);
	REXP ro = FileUtils.buildlistFromOldCounter(cc,((double)System.currentTimeMillis()-startsec) /1000);
	
	REXP.Builder thevals   = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	thevals.addRexpValue(ro);
	thevals.addRexpValue(RObjects.buildBooleanVector(new boolean[]{issuc}).build());
	return(thevals.build());
    }
    public static REXP buildlistFromOldCounter(org.apache.hadoop.mapred.Counters c,double dur){
	String[] groupnames = c.getGroupNames().toArray(new String[]{});
	String[] groupdispname = new String[groupnames.length+1];
	Vector<REXP> cn = new Vector<REXP>();
	for(int i=0;i < groupnames.length; i++){
	    org.apache.hadoop.mapred.Counters.Group
		cgroup = c.getGroup( groupnames[i]);
	    groupdispname[i] = cgroup.getDisplayName();
	    REXP.Builder cvalues = REXP.newBuilder();
	    Vector<String> cnames = new Vector<String>();
	    cvalues.setRclass(REXP.RClass.REAL);
	    for (org.apache.hadoop.mapred.Counters.Counter counter: cgroup){
		cvalues.addRealValue((double)counter.getValue());
		cnames.add(counter.getDisplayName());
	    }
	    cvalues.addAttrName("names");
	    cvalues.addAttrValue(RObjects.makeStringVector(cnames.toArray(new String[]{})));
	    cn.add( cvalues.build());
	}
	groupdispname[groupnames.length]="job_time";
	REXP.Builder cvalues = REXP.newBuilder();
	cvalues.setRclass(REXP.RClass.REAL);
	cvalues.addRealValue(dur);
	cn.add(cvalues.build());
	return(RObjects.makeList(groupdispname,cn));
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
		String[] result0 = fu.ls(r.getRexpValue(0),r.getRexpValue(1).getIntValue(0));
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
		for(int i=0;i< r.getStringValueCount();i++){
		    String s = r.getStringValue(i).getStrval();
		    fu.delete(s,true);
		}
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
	    case 7:
		r = fu.readInfo(args[1]);
		fu.getKeys(r);
		break;
	    case 8:
		r = fu.readInfo(args[1]);
		fu.sequence2map(r);
		break;
	    case 9:
		r = fu.readInfo(args[1]);
		fu.hdfsrename(r);
		break;
	    case 10:
		r = fu.readInfo(args[1]);
		b= fu.joinjob(r);
		fu.writeTo(args[1], b);
		break;
	    case 11:
		r = fu.readInfo(args[1]);
		b= fu.getstatus(r);
		fu.writeTo(args[1], b);
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
