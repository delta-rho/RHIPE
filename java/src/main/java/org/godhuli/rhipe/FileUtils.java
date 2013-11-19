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
import org.apache.hadoop.mapred.TaskReport;
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

    public String[] ls(String[] r,int f) throws IOException,FileNotFoundException,
    URISyntaxException{
	ArrayList<String> lsco = new ArrayList<String>();
	for(String path : r){
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
    public REXP getstatus(String jd, boolean geterrors) throws Exception{
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
    	if(jj==null) 
    	    throw new IOException("Jobtracker could not find jobID: "+jd);
    	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	if(rj==null)
	    throw new IOException("No such job: "+jd+" available, wrong job? or try the History Viewer (see the Web UI) ");
    	String jobfile = rj.getJobFile();
	String jobname = rj.getJobName();
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
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobname}));
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobfile}));
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
    public void killjob(String jd) throws Exception{
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	rj.killJob();
    }

    public REXP joinjob(String jd, boolean verbose) throws Exception{
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

    public byte[] getDetailedInfoForJob(String jd) throws Exception{
    	org.apache.hadoop.mapred.JobID jj = org.apache.hadoop.mapred.JobID.forName(jd);
    	if(jj==null) 
    	    throw new IOException("Jobtracker could not find jobID: "+jd);
    	org.apache.hadoop.mapred.RunningJob rj = jclient.getJob(jj);
	if(rj==null)
	    throw new IOException("No such job: "+jd+" available, wrong job? or try the History Viewer (see the Web UI) ");
    	String jobfile = rj.getJobFile();
	String jobname = rj.getJobName();
    	org.apache.hadoop.mapred.Counters cc = rj.getCounters();
    	long startsec = getStart(jclient,jj);
    	REXP allCounters = FileUtils.buildlistFromOldCounter(cc, 0 );
    	int jobs = rj.getJobState();
    	String jobss = null;
    	if(jobs == JobStatus.FAILED) jobss = "FAILED";
    	else if(jobs == JobStatus.KILLED) jobss="KILLED";
    	else if(jobs == JobStatus.PREP) jobss="PREP";
    	else if(jobs == JobStatus.RUNNING) jobss="RUNNING";
    	else if(jobs == JobStatus.SUCCEEDED) jobss="SUCCEEDED";
    	float mapprog = rj.mapProgress(), reduprog = rj.reduceProgress();
	
    	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobss}));
    	thevals.addRexpValue( RObjects.buildDoubleVector( new double[]{ startsec}));
    	thevals.addRexpValue( RObjects.buildDoubleVector( new double[]{ (double)mapprog, (double)reduprog}));
    	thevals.addRexpValue( allCounters );
	thevals.addRexpValue( RObjects.makeStringVector(rj.getTrackingURL()));
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobname}));
    	thevals.addRexpValue( RObjects.makeStringVector( new String[]{ jobfile}));
	
	org.apache.hadoop.mapred.TaskReport[] maptr = jclient.getMapTaskReports( jj);
    	REXP.Builder thevalsA   = REXP.newBuilder();
    	thevalsA.setRclass(REXP.RClass.LIST);
	for(TaskReport t : maptr){
	    thevalsA.addRexpValue( TaskReportToRexp( t));
	}
	thevals.addRexpValue( thevalsA.build() );



    	org.apache.hadoop.mapred.TaskReport[] redtr = jclient.getReduceTaskReports( jj);
    	REXP.Builder thevalsB   = REXP.newBuilder();
    	thevalsB.setRclass(REXP.RClass.LIST);
	for(TaskReport t : redtr){
	    thevalsB.addRexpValue( TaskReportToRexp( t));
	}
	thevals.addRexpValue( thevalsB.build() );

	return thevals.build().toByteArray();
    }

    private String convertTIP(TIPStatus t){
	return t.toString();
    }
	
    private REXP TaskReportToRexp(TaskReport t){
    	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
    	thevals.addRexpValue( FileUtils.buildlistFromOldCounter(t.getCounters(),0));
	thevals.addRexpValue( RObjects.makeStringVector( convertTIP(t.getCurrentStatus())));
	thevals.addRexpValue( RObjects.buildDoubleVector( new double[]{ t.getProgress(),t.getStartTime(),t.getFinishTime()}));
	thevals.addRexpValue( RObjects.makeStringVector( t.getTaskID().toString()));
	thevals.addRexpValue( RObjects.makeStringVector( t.getSuccessfulTaskAttempt().toString()));

	return thevals.build();
    }

    public static void main(String[] args) throws Exception{
    }
}
