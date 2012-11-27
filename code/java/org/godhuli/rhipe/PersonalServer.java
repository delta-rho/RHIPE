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

    public static String getPID() throws IOException, InterruptedException {
	// Taken from
	// http://www.coderanch.com/t/109334/Linux-UNIX/UNIX-process-ID-java-program
	Vector<String> commands = new Vector<String>();
	commands.add("/bin/bash");
	commands.add("-c");
	commands.add("echo $PPID");
	ProcessBuilder pb = new ProcessBuilder(commands);
	Process pr = pb.start();
	pr.waitFor();
	if (pr.exitValue() == 0) {
	    BufferedReader outReader = new BufferedReader(
							  new InputStreamReader(pr.getInputStream()));
	    return outReader.readLine().trim();
	} else {
	    throw new IOException("Problem getting PPID");
	}
    }
    
    public void docrudehack(String temp) throws IOException {
	FileWriter outFile = new FileWriter(temp);
	String x = "DONE";
	outFile.write(x, 0, x.length());
	outFile.flush();
	outFile.close();
    }
    public void setUserInfo(String ipaddress, String tempfile, String tempfile2,
			    int bugl) throws InterruptedException, FileNotFoundException,
					     UnknownHostException, SecurityException, IOException {
	bbuf = new byte[100];
	this.buglevel = bugl;
	seqhash = new Hashtable<String, SequenceFile.Reader>();
	mrhash = new Hashtable<String, MapFile.Reader[]>();
	mapfilehash = new Hashtable<String, String[]>();
	REXP.Builder thevals = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LOGICAL);
	thevals.addBooleanValue(REXP.RBOOLEAN.T);
	yesalive = thevals.build();
	if (buglevel > 10)
	    LOG.info("Calling FileUtils");
	fu = new FileUtils(getConf());
	if (buglevel > 10)
	    LOG.info("Got FileUtils object:" + fu);
		
	ServerSocket fromRsock, errsock, toRsock;
	if (buglevel > 10)
	    LOG.info("Creating listening and writing sockets");
	fromRsock = new ServerSocket(0, 0, InetAddress.getByName(ipaddress));
	toRsock = new ServerSocket(0);
	errsock = new ServerSocket(0);
	if (buglevel > 10)
	    LOG.info("Got fromRsock=" + fromRsock + " toRsock=" + toRsock
		     + " errsock=" + errsock);
	FileWriter outFile = new FileWriter(tempfile);
	if (buglevel > 10)
	    LOG.info("Writing information to file:" + outFile);
	String x = "fromR toR err PID\n";
	outFile.write(x, 0, x.length());
	x = fromRsock.getLocalPort() + " " + toRsock.getLocalPort() + " "
	    + errsock.getLocalPort() + " " + getPID() + "\n";
	outFile.write(x, 0, x.length());
	outFile.flush();
	outFile.close();
	docrudehack(tempfile2);
	if (buglevel > 10)
	    LOG.info("Finished with crudehack by creating a file called "
		     + tempfile2);
	Socket a = fromRsock.accept();
	_fromR = new DataInputStream(new BufferedInputStream(
							     a.getInputStream(), 1024));
	a = toRsock.accept();
	_toR = new DataOutputStream(new BufferedOutputStream(a
							     .getOutputStream(), 1024));
	a = errsock.accept();
	_err = new DataOutputStream(new BufferedOutputStream(a
							     .getOutputStream(), 1024));

	if(buglevel > 10)
	    LOG.info("Initializing Caches");
	if (buglevel > 10)
	    LOG.info("Now waiting on all sockets");
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

    public void initializeCaches(REXP rexp) throws Exception{
	REXP rexp0 = rexp.getRexpValue(1);
	// RemovalListener<ValuePair, RHBytesWritable> rl = new RemovalListener<ValuePair, RHBytesWritable>() {
	//     public void onRemoval(RemovalNotification<ValuePair, RHBytesWritable> removal) throws RuntimeException{
	// 	    LOG.info("Extterminate key, emptied from cache:"+removal.getKey());
	//     }
	// };
	valueCache = CacheBuilder.newBuilder()
	    .maximumWeight(rexp0.getRexpValue(0).getIntValue(0)) //max MB in bytes, set to 100MB
	    .weigher(new Weigher<ValuePair, RHBytesWritable>() {
	    	    public int weigh(ValuePair k, RHBytesWritable g) {
	    		return k.getKey().getLength() + g.getLength();
	    	    }
	    	})
	    // .removalListener(rl)
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
    public void shutdownJavaServer() throws Exception {
	//send_alive();  Actually, I will let R ask for send_alive().
	System.exit(0);
    }

    public void startme() {
	while (true) {
	    try {
		int size = _fromR.readInt();
		if (size > bbuf.length) {
		    bbuf = new byte[size];
		}
		if (size < 0)
		    break;
		else if (size == 0)
		    send_alive();
		else {
		    _fromR.readFully(bbuf, 0, size);
		    REXP r = REXP.newBuilder().mergeFrom(bbuf, 0, size).build();
		    if (r.getRclass() == REXP.RClass.NULLTYPE)
			send_alive();
		    // the first element of list is function, the rest are
		    // arguments
		    String tag = r.getRexpValue(0).getStringValue(0)
			.getStrval();
		    // if (tag.equals("rhmropts"))
		    // 	rhmropts(r);
		    // else if (tag.equals("rhls"))
		    // 	rhls(r);
		    // else if (tag.equals("rhget"))
		    // 	rhget(r);
		    // else if (tag.equals("rhput"))
		    // 	rhput(r);
		    // else if (tag.equals("rhdel"))
		    // 	rhdel(r);
		    // else if (tag.equals("rhgetkeys"))
		    // 	rhgetkeys(r);
		    // else if (tag.equals("binaryAsSequence"))
		    // 	binaryAsSequence(r);
		    // else if (tag.equals("sequenceAsBinary"))
		    // 	sequenceAsBinary(r);
		    // else if (tag.equals("rhstatus"))
		    // 	rhstatus(r);
		    // else if (tag.equals("rhjoin"))
		    // 	rhjoin(r);
		    // else if (tag.equals("rhkill"))
		    // 	rhkill(r);
		    // else if (tag.equals("rhex"))
		    // 	rhex(r);
		    // else if (tag.equals("rhcat"))
		    // 	rhcat(r);
		    // else if (tag.equals("rhopensequencefile"))
		    // 	rhopensequencefile(r);
		    // else if (tag.equals("rhgetnextkv"))
		    // 	rhgetnextkv(r);
		    // else if (tag.equals("initializeCaches"))
		    // 	initializeCaches(r);
		    // else if (tag.equals("initializeMapFile"))
		    // 	initializeMapFile(r);
		    // else if (tag.equals("rhgetkeys2"))
		    // 	rhgetkeys2(r);
		    // else if (tag.equals("rhclosesequencefile"))
		    // 	rhclosesequencefile(r);
		    if (tag.equals("shutdownJavaServer"))
			shutdownJavaServer();
		    else{
			Method method = Class.forName("org.godhuli.rhipe.PersonalServer").getMethod(tag, new Class[]{REXP.class});
			method.invoke(this, r);
		    }

		    // else if(tag.equals("rhcp")) rhcp(r);
		    // else if(tag.equals("rhmv")) rhmv(r);
		    // else if(tag.equals("rhmerge")) rhmerge(r);

		    // else
		    // 	send_error_message("Could not find method with name: "
		    // 			   + tag + "\n");
		}
	    } catch (EOFException e) {
		System.exit(0);
	    } catch (SecurityException e) {
		send_error_message(e.getCause());
	    } catch (RuntimeException e) {
		send_error_message(e);
	    } catch (IOException e) {
		send_error_message(e);
	    } catch(NoSuchMethodException e){
		send_error_message(e.getCause());
	    } catch(IllegalAccessException e){
		send_error_message(e.getCause());
	    } catch(InvocationTargetException e){
		send_error_message(e.getCause());
	    } catch (Exception e) {
		send_error_message(e);
	    }
	}
    }

    public int run(String[] args) throws Exception {
	// Configuration processed by ToolRunner
	Configuration conf = getConf();
	_configuration = getConf();
	_filesystem = FileSystem.get(_configuration);
	_hp = RHMapFileOutputFormat.getHP();
	int buglevel = Integer.parseInt(args[3]);
	setUserInfo(args[0], args[1], args[2],
		    buglevel);
	startme();

	// while (true) {
	//     try {
	//     } catch (Exception e) {
	// 	System.err.println(Thread.currentThread().getStackTrace());
	//     }
	// }
	return(0);
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new PersonalServer(), args);
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
