package org.godhuli.rhipe;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.godhuli.rhipe.REXPProtos.REXP;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.net.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.IllegalArgumentException;
import java.lang.IllegalAccessException;
import java.lang.NoSuchMethodException;
import java.lang.SecurityException;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.google.common.cache.LoadingCache;
import com.google.common.cache.Cache;
import java.util.concurrent.Callable;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.CacheStats;

public class PersonalServer extends Configured implements Tool {
    protected static final Log LOG = LogFactory.getLog(PersonalServer.class
						       .getName());
    Configuration _configuration;
    FileSystem _filesystem;
    HashPartitioner<RHBytesWritable,RHBytesWritable> _hp;
    byte[] bbuf;
    DataOutputStream _err, _toR;
    DataInputStream _fromR;
    REXP yesalive;
    FileUtils fu;
    int buglevel;
    Hashtable<String, SequenceFile.Reader> seqhash; //why do i have it?
    Hashtable<String, MapFile.Reader[]> mrhash; //will be removed
    Hashtable<String,String[]> mapfilehash;
    Cache<ValuePair, RHBytesWritable> valueCache;
    Cache<String, MapFile.Reader> mapfileReaderCache;
    Hashtable<String,ArrayList<ValuePair>> mapToValueCacheKeys =  new Hashtable<String,ArrayList<ValuePair>>();
    Hashtable<String,ArrayList<String>> mapToValueCacheHandles  = new Hashtable<String,ArrayList<String>>();

    public PersonalServer(){}

    public void setUserInfo( int bugl) throws InterruptedException {
	bbuf = new byte[100];
	this.buglevel = bugl;
	seqhash = new Hashtable<String, SequenceFile.Reader>();
	mrhash = new Hashtable<String, MapFile.Reader[]>();
	mapfilehash = new Hashtable<String, String[]>();
	fu = new FileUtils(getConf());
    }

    public void rhmropts(REXP r) throws Exception {
	REXP b = fu.mapredopts();
	send_result(b);
    }

    public void rhcat(REXP r) throws Exception {
	final int buff = r.getRexpValue(2).getIntValue(0);
	final int mx = r.getRexpValue(3).getIntValue(0);
	final int whattype = r.getRexpValue(4).getIntValue(0);
	for (int i = 0; i < r.getRexpValue(1).getStringValueCount(); i++) {
	    Path srcPattern = new Path(r.getRexpValue(1).getStringValue(i)
				       .getStrval());
	    new DelayedExceptionThrowing() {
		void process(Path p, FileSystem srcFs) throws IOException {
		    if (srcFs.getFileStatus(p).isDir()) {
			throw new IOException("Source must be a file.");
		    }
		    // System.err.println("INPUT="+p);
		    InputStream ins = srcFs.open(p);
		    if (whattype == 1) {
			ins = new java.util.zip.GZIPInputStream(ins);
		    }
		    printToStdout(ins, buff, mx);
		}
	    }
		.globAndProcess(srcPattern, srcPattern.getFileSystem(fu
								     .getConf()));
	}
	_toR.writeInt(-1);
	_toR.flush();
    }

    private void printToStdout(InputStream in, int buffsize, int mx)
	throws IOException {
	try {
	    byte buf[] = new byte[buffsize];
	    int bytesRead = in.read(buf);
	    int totalread = bytesRead;
	    while (bytesRead >= 0) {
		// System.err.println("Wrote "+bytesRead);
		_toR.writeInt(bytesRead);
		_toR.write(buf, 0, bytesRead);
		_toR.flush();
		if (mx > -1 && totalread >= mx)
		    break;
		bytesRead = in.read(buf);
		totalread += bytesRead;
	    }
	} finally {
	    in.close();
	}
    }



    public void rhls(REXP r) throws Exception {
	String[] result0 = fu.ls(r.getRexpValue(1) // This is a string vector
				 , r.getRexpValue(2).getIntValue(0));
	REXP b = RObjects.makeStringVector(result0);
	send_result(b);

    }

    public void rhdel(REXP r) throws Exception {
	for (int i = 0; i < r.getRexpValue(1).getStringValueCount(); i++) {
	    String s = r.getRexpValue(1).getStringValue(i).getStrval();
	    fu.delete(s, true);
	}
	send_result("OK");
    }

    public void rhget(REXP r) throws Exception {
	String src = r.getRexpValue(1).getStringValue(0).getStrval();
	String dest = r.getRexpValue(2).getStringValue(0).getStrval();
	System.err.println("Copying " + src + " to " + dest);
	fu.copyMain(src, dest);
	send_result("OK");
    }

    
    public void rhput(REXP r) throws Exception {
	String[] locals = new String[r.getRexpValue(1).getStringValueCount()];
	for (int i = 0; i < locals.length; i++)
	    locals[i] = r.getRexpValue(1).getStringValue(i).getStrval();
	String dest2 = r.getRexpValue(2).getStringValue(0).getStrval();
	REXP.RBOOLEAN overwrite_ = r.getRexpValue(3).getBooleanValue(0);
	boolean overwrite;
	if (overwrite_ == REXP.RBOOLEAN.F)
	    overwrite = false;
	else if (overwrite_ == REXP.RBOOLEAN.T)
	    overwrite = true;
	else
	    overwrite = false;
	fu.copyFromLocalFile(locals, dest2, overwrite);
	send_result("OK");
    }

    public void sequenceAsBinary(REXP r) throws Exception { // works
	Configuration cfg = new Configuration();
	int n = r.getRexpValue(1).getStringValueCount();
	String[] infile = new String[n];
	for (int i = 0; i < n; i++) {
	    infile[i] = r.getRexpValue(1).getStringValue(i).getStrval();
	}
	int maxnum = r.getRexpValue(2).getIntValue(0);
	int bufsz = r.getRexpValue(3).getIntValue(0);
	// as this rexp is written into
	DataOutputStream cdo = new DataOutputStream(
						    new java.io.BufferedOutputStream(_toR, 1024 * 1024));
	int counter = 0;
	boolean endd = false;
	RHBytesWritable k = new RHBytesWritable();
	RHBytesWritable v = new RHBytesWritable();
	for (int i = 0; i < infile.length; i++) {
	    SequenceFile.Reader sqr = new SequenceFile.Reader(FileSystem
							      .get(cfg), new Path(infile[i]), cfg);
	    while (true) {
		boolean gotone = sqr.next((Writable) k, (Writable) v);
		if (gotone) {
		    counter++;
		    k.writeAsInt(cdo);
		    v.writeAsInt(cdo);
		    cdo.flush();
		} else
		    break;
		if (maxnum > 0 && counter >= maxnum) {
		    endd = true;
		    break;
		}
	    }
	    sqr.close();
	    if (endd)
		break;
	}
	cdo.writeInt(0);
	cdo.flush();
    }

    public void rhopensequencefile(REXP r) throws Exception {
	// System.out.println("----Called-----");
	String name = r.getRexpValue(1).getStringValue(0).getStrval();
	Configuration cfg = new Configuration();
	SequenceFile.Reader sqr = new SequenceFile.Reader(FileSystem.get(cfg),
							  new Path(name), cfg);
	seqhash.put(name, sqr);
	send_result("OK");
    }

    public void rhgetnextkv(REXP r) throws Exception {
	String name = r.getRexpValue(1).getStringValue(0).getStrval();
	int quant = r.getRexpValue(2).getIntValue(0);
	SequenceFile.Reader sqr = seqhash.get(name);
	RHBytesWritable k = new RHBytesWritable();
	RHBytesWritable v = new RHBytesWritable();
	DataOutputStream cdo = new DataOutputStream(
						    new java.io.BufferedOutputStream(_toR, 1024 * 1024));
	if (sqr != null) {
	    for (int i = 0; i < quant; i++) {
		boolean gotone = sqr.next((Writable) k, (Writable) v);
		if (gotone) {
		    k.writeAsInt(cdo);
		    v.writeAsInt(cdo);
		    cdo.flush();
		} else {
		    sqr.close();
		    seqhash.remove(name);
		    break;
		}
	    }
	}
	cdo.writeInt(0);
	cdo.flush();
    }

    public void rhclosesequencefile(REXP r) throws Exception {
	String name = r.getRexpValue(1).getStringValue(0).getStrval();
	SequenceFile.Reader sqr = seqhash.get(name);
	if (sqr != null) {
	    try {
		sqr.close();
		seqhash.remove(name);
	    } catch (Exception e) {
	    }
	}
	send_result("OK");
    }

    public void rhstatus(REXP r) throws Exception {
	REXP jid = r.getRexpValue(1);
	REXP result = fu.getstatus(jid);
	send_result(result);
    }

    public void rhjoin(REXP r) throws Exception {
	REXP result = fu.joinjob(r.getRexpValue(1));
	send_result(result);
    }

    public void rhkill(REXP r) throws Exception {
	REXP jid = r.getRexpValue(1);
	fu.killjob(jid);
	send_result("OK");
    }
    public void send_alive() throws Exception {
	try {

	    _toR.writeByte(1);
	    _toR.flush();
	} catch (IOException e) {
	    System.err.println("RHIPE: Could not tell R it is alive");
	    System.exit(1);
	}
    }

    public void send_error_message(Throwable e) {
	ByteArrayOutputStream bs = new ByteArrayOutputStream();
	e.printStackTrace(new PrintStream(bs));
	String s = bs.toString();
	send_error_message(s);
    }

    public void send_error_message(String s) {
	REXP clattr = RObjects.makeStringVector("worker_error");
	REXP r = RObjects
	    .addAttr(RObjects.buildStringVector(new String[] { s }),
		     "class", clattr).build();
	System.err.println(s);
	sendMessage(r, true);
    }

    public void sendMessage(REXP r) {
	sendMessage(r, false);
    }

    public void sendMessage(REXP r, boolean bb) {
	try {
	    byte[] b = r.toByteArray();
	    DataOutputStream dos = _toR;
	    // if(bb) dos = _err;
	    if (bb)
		dos.writeInt(-b.length);
	    else
		dos.writeInt(b.length);
	    dos.write(b, 0, b.length);
	    dos.flush();
	} catch (IOException e) {
	    System.err
		.println("RHIPE: Could not send data back to R master, sending to standard error");
	    System.err.println(r);
	    System.exit(1);
	}
    }

    public void send_result(String s) {
	REXP r = RObjects.makeStringVector(s);
	send_result(r);
    }

    public void send_result(REXP r) {
	// we create a list of class "worker_result"
	// it is a list of element given by s
	// all results are class worker_result and are a list
	REXP.Builder thevals = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	thevals.addRexpValue(r);
	RObjects.addAttr(thevals, "class", RObjects
			 .makeStringVector("worker_result"));
	sendMessage(thevals.build());
    }
    
    public byte[] send_back(REXP r){
	REXP.Builder thevals = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	thevals.addRexpValue(r);
	RObjects.addAttr(thevals, "class", RObjects
			 .makeStringVector("worker_result"));
	return thevals.build().toByteArray();
    }

    public void initializeCaches(REXP rexp) throws Exception{
	REXP rexp0 = rexp.getRexpValue(1);
	valueCache = CacheBuilder.newBuilder()
	    .maximumWeight(rexp0.getRexpValue(0).getIntValue(0)) //max MB in bytes, set to 100MB
	    .weigher(new Weigher<ValuePair, RHBytesWritable>() {
	    	    public int weigh(ValuePair k, RHBytesWritable g) {
	    		return k.getKey().getLength() + g.getLength();
	    	    }
	    	})
	    .recordStats()
	    .build();
	
	RemovalListener<String, MapFile.Reader> removalListener = new RemovalListener<String, MapFile.Reader>() {
	    public void onRemoval(RemovalNotification<String, MapFile.Reader> removal) throws RuntimeException{
		try{
		    MapFile.Reader conn = removal.getValue();
		    LOG.info("Closing a mapfile, emptied from cache:"+conn);
		    conn.close(); 
		}catch(IOException e){
		    throw new RuntimeException(e);
		}
	    }
	};
	mapfileReaderCache = CacheBuilder.newBuilder()
	    .maximumSize(rexp0.getRexpValue(0).getIntValue(1))
	    .removalListener(removalListener)
	    .recordStats()
	    .build();
	
	send_result("OK");
    }

    public void clearEntiresFor(String forkey) throws Exception{
	ArrayList<String> cachedHandle = mapToValueCacheHandles.get(forkey);
	ArrayList<ValuePair> cachedValues = mapToValueCacheKeys.get(forkey);
	mapfileReaderCache.invalidate(cachedHandle);
	valueCache.invalidate(cachedValues);
	cachedHandle.clear();
	cachedValues.clear();
    }
    public void initializeMapFile(REXP rexp) throws Exception{
	REXP rexp0 = rexp.getRexpValue(1);
	REXP paths = rexp0.getRexpValue(0); // paths to read from
	String akey = rexp0.getRexpValue(1).getStringValue(0).getStrval();
	String[] pathsForMap = new String[ paths.getStringValueCount() ];
	for (int i = 0; i < pathsForMap.length; i++) {
	    pathsForMap[i] = paths.getStringValue(i).getStrval();
	}
	if(mapfilehash.get(akey)!=null){
	    LOG.info("Clearing Caches for "+akey);
	    clearEntiresFor(akey);
	}else{
	    mapToValueCacheKeys.put(akey, new ArrayList<ValuePair>(500));
	    mapToValueCacheHandles.put(akey, new ArrayList<String>(100));
	}
	mapfilehash.put(akey, pathsForMap);
	send_result("OK");
    }

    public void rhgetkeys2(REXP rexp) throws Exception,IOException
    {
	REXP rexp0 = rexp.getRexpValue(1);
	final String key = rexp0.getRexpValue(0).getStringValue(0).getStrval();
	REXP keys = rexp0.getRexpValue(1);
	final String[] pathsForMap = mapfilehash.get(key);
	RHBytesWritable v = new RHBytesWritable();
	DataOutputStream out = _toR;
	final RHNull anull = new RHNull();
	for(int i=0; i < keys.getRexpValueCount();i++){
	    final RHBytesWritable k = new RHBytesWritable(keys.getRexpValue(i).getRawValue().toByteArray());
	    ValuePair vp = new ValuePair(key,k);
	    v = valueCache.getIfPresent(vp);
	    if(v==null){
		// LOG.info("Not found requested key:"+vp);
		RHBytesWritable a = new RHBytesWritable();
		final int which = _hp.getPartition(k,a,pathsForMap.length);
		MapFile.Reader f = mapfileReaderCache.getIfPresent(pathsForMap[which]);
		if(f==null){
		    f = new MapFile.Reader(_filesystem,pathsForMap[which],_configuration);
		    mapfileReaderCache.put(pathsForMap[which],f);
		    mapToValueCacheHandles.get(key).add(pathsForMap[which]);
		}
		RHBytesWritable ww= (RHBytesWritable)f.get(k,a);
		if(ww==null) v= anull; else v = ww;
		mapToValueCacheKeys.get(key).add(vp);
		valueCache.put(vp, v);
	    }
	    k.writeAsInt(out); v.writeAsInt(out);
	}
	out.writeInt(0);
	out.flush();
    }
    
    public void cacheStatistics(REXP rexp){
	REXP rexp0 = rexp.getRexpValue(1);
	int which = rexp0.getRexpValue(0).getIntValue(0);
	CacheStats c = null;
	if(which == 0)
	    c = mapfileReaderCache.stats();
	else if(which == 1)
	    c = valueCache.stats();
	REXP r = RObjects.buildDoubleVector(new double[]{
		((double)c.averageLoadPenalty())*1e-6,(double)c.evictionCount(),
		(double)c.hitCount(),c.hitRate(),(double)c.loadCount(),
		(double)c.loadExceptionCount(), c.loadExceptionRate(),
		(double)c.loadSuccessCount(),(double)c.missCount(),(double)c.missRate(),
		(double)c.requestCount(),((double)c.totalLoadTime())*1e-6
	    }).build();
	send_result(r);
    }
    public void rhgetkeys(REXP rexp00) throws Exception {
	REXP rexp0 = rexp00.getRexpValue(1);
	REXP keys = rexp0.getRexpValue(0); // keys
	REXP paths = rexp0.getRexpValue(1); // paths to read from
	String tempdest = rexp0.getRexpValue(2).getStringValue(0).getStrval(); // tempdest
	REXP.RBOOLEAN b = rexp0.getRexpValue(3).getBooleanValue(0); // as

	// sequence
	// or binary
	Configuration c = fu.getConf();
	DataOutputStream out = _toR;
	String akey = rexp0.getRexpValue(4).getStringValue(0).getStrval(); // tempdest
	String[] pnames = new String[paths.getStringValueCount()];
	for (int i = 0; i < pnames.length; i++) {
	    pnames[i] = paths.getStringValue(i).getStrval();
	}
	MapFile.Reader[] mr  = mrhash.get(akey);
	if(mr == null){
	    // LOG.info("Did not find in hashtable");
	    mr = RHMapFileOutputFormat.getReaders(pnames, c);
	    mrhash.put(akey, mr);
	}
	
	int numkeys = keys.getRexpValueCount();
	RHBytesWritable k = new RHBytesWritable();
	RHBytesWritable v = new RHBytesWritable();
	boolean closeOut = false;
	if (b == REXP.RBOOLEAN.F) { // binary style
	    if (out == null) {
		closeOut = true;
		out = new DataOutputStream(new FileOutputStream(tempdest));
	    }
	    for (int i = 0; i < numkeys; i++) {
		k.set(keys.getRexpValue(i).getRawValue().toByteArray());
		RHMapFileOutputFormat.getEntry(mr, k, v);
		k.writeAsInt(out);
		v.writeAsInt(out);
	    }
	    if (closeOut)
		out.close();
	    else {
		out.writeInt(0);
		out.flush();
	    }
	} else {// these will be written out as a sequence file
	    RHWriter rw = new RHWriter(tempdest, fu.getConf());
	    SequenceFile.Writer w = rw.getSQW();
	    for (int i = 0; i < numkeys; i++) {
		k.set(keys.getRexpValue(i).getRawValue().toByteArray());
		RHMapFileOutputFormat.getEntry(mr, k, v);
		w.append(k, v);
	    }
	    rw.close();
	}
    }


    public void rhex(REXP rexp0) throws Exception {
	String[] zonf = new String[] { rexp0.getRexpValue(1).getStringValue(0)
				       .getStrval() };
	int result = RHMR.fmain(zonf);
	send_result("" + result);
    }

    public void binaryAsSequence(REXP r) throws Exception { // works
	Configuration cfg = new Configuration();
	String ofolder = r.getRexpValue(1).getStringValue(0).getStrval();
	int groupsize = r.getRexpValue(2).getIntValue(0);
	int howmany = r.getRexpValue(3).getIntValue(0);
	int N = r.getRexpValue(4).getIntValue(0);
	DataInputStream in = _fromR;
	int count = 0;
	// System.out.println("Got"+r);
	// System.out.println("Waiting for input");
	for (int i = 0; i < howmany - 1; i++) {
	    String f = ofolder + "/" + i;
	    RHWriter w = new RHWriter(f, cfg);
	    w.doWriteFile(in, groupsize);
	    count = count + groupsize;
	    w.close();
	}

	if (count < N) {
	    count = N - count;
	    String f = ofolder + "/" + (howmany - 1);
	    RHWriter w = new RHWriter(f, cfg);
	    w.doWriteFile(in, count);
	    w.close();
	}
	send_result("OK");
    }
    public void binaryAsSequence2(REXP r) throws Exception { // works
	Configuration cfg = new Configuration();
	String ofolder = r.getRexpValue(1).getStringValue(0).getStrval();
	int numperfile = r.getRexpValue(2).getIntValue(0);
	int howmany = r.getRexpValue(3).getIntValue(0);
	DataInputStream in = _fromR;
	int count = 0, i=0;
	String f = ofolder + "/" + i;
	RHWriter w = new RHWriter(f, cfg);	
	while(i < howmany){
	    w.writeAValue(in);
	    i++;
	    if( i  % numperfile == 0){
		count ++;
		f = ofolder+"/"+count;
		w.close();
		if(i < howmany) 
		    w = new RHWriter(f, cfg);
	    }
	}
	try{
	    w.close();
	}catch(Exception e){}
	send_result("OK");
    }


    public int run(int buglevel) throws Exception {
	// Configuration processed by ToolRunner
	_configuration = getConf();
	_filesystem = FileSystem.get(_configuration);
	_hp = RHMapFileOutputFormat.getHP();
	setUserInfo(buglevel);
	return(0);
    }

    
    public abstract class DelayedExceptionThrowing {
	abstract void process(Path p, FileSystem srcFs) throws IOException;
	
	final void globAndProcess(Path srcPattern, FileSystem srcFs)
	    throws IOException {
	    ArrayList<IOException> exceptions = new ArrayList<IOException>();
	    for (Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern),
					      srcPattern))
		try {
		    process(p, srcFs);
		} catch (IOException ioe) {
		    exceptions.add(ioe);
		}

	    if (!exceptions.isEmpty())
		if (exceptions.size() == 1)
		    throw exceptions.get(0);
		else
		    throw new IOException("Multiple IOExceptions: "
					  + exceptions);
	}
    }

}
