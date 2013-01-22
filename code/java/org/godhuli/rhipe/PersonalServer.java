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
import org.apache.hadoop.fs.FSDataInputStream;
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

public class PersonalServer {
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

    public void setUserInfo( int bugl) throws InterruptedException,IOException {
	bbuf = new byte[100];
	this.buglevel = bugl;
	seqhash = new Hashtable<String, SequenceFile.Reader>();
	mrhash = new Hashtable<String, MapFile.Reader[]>();
	mapfilehash = new Hashtable<String, String[]>();
	fu = new FileUtils(_configuration);
    }
    public FileUtils getFU(){
	return fu;
    }
    public FileSystem getFS(){
	return _filesystem;
    }
    public Configuration getConf(){
	return _configuration;
    }
    public byte[] rhmropts() throws Exception {
	REXP b = fu.mapredopts();
        return b.toByteArray();
    }

    public byte[] rhls(String p, int a) throws Exception {
	return rhls(new String[]{p}, a);
    }
    public byte[] rhls(String[] p, int a) throws Exception {
	String[] result0 = fu.ls(p, a);
	REXP b = RObjects.makeStringVector(result0);
	return b.toByteArray();
    }

	
    public void rhdel(String folder) throws Exception {
	rhdel(new String[] {folder});
    }
    public void rhdel(String[] folder) throws Exception {
	for (String s: folder){
	    fu.delete(s, true);
	}
    }

    public void rhget(String src, String dest) throws Exception {
	System.err.println("Copying " + src + " to " + dest);
	fu.copyMain(src, dest);
    }

    public void rhput(String local,String dest2, boolean overwrite) throws Exception {
	rhput(new String[] {local}, dest2, overwrite);
    }
    public void rhput(String[] locals,String dest2, boolean overwrite) throws Exception {
	fu.copyFromLocalFile(locals, dest2, overwrite);
    }


    public byte[] rhstatus(String s,boolean b) throws Exception {
	REXP result = fu.getstatus(s,b);
	return result.toByteArray();
    }

    public byte[] rhjoin(String a, boolean b) throws Exception {
	REXP result = fu.joinjob(a,b);
	return result.toByteArray();
    }

    public void rhkill(String s) throws Exception {
	fu.killjob(s);
    }

    public int rhex(String zonf) throws Exception {
	int result = RHMR.fmain(new String[] {zonf});
	return result;
    }
    public int rhex(String[] zonf) throws Exception {
	int result = RHMR.fmain(zonf);
	return result;
    }

    public void initializeCaches(int a, int b) throws Exception{
	valueCache = CacheBuilder.newBuilder()
	    .maximumWeight(a) //max MB in bytes, set to 100MB
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
	    .maximumSize(b)
	    .removalListener(removalListener)
	    .recordStats()
	    .build();
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
	String name = r.getRexpValue(1).getStringValue(0).getStrval();
	Configuration cfg = new Configuration();
	SequenceFile.Reader sqr = new SequenceFile.Reader(FileSystem.get(cfg),
							  new Path(name), cfg);
	seqhash.put(name, sqr);
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

    public void rhclosesequencefile(byte[] b) throws Exception {
	REXP r =  REXP.newBuilder().mergeFrom(b, 0,b.length).build();
	String name = r.getRexpValue(1).getStringValue(0).getStrval();
	SequenceFile.Reader sqr = seqhash.get(name);
	if (sqr != null) {
	    try {
		sqr.close();
		seqhash.remove(name);
	    } catch (Exception e) {
	    }
	}
    }

    public void clearEntiresFor(String forkey) throws Exception{
	ArrayList<String> cachedHandle = mapToValueCacheHandles.get(forkey);
	ArrayList<ValuePair> cachedValues = mapToValueCacheKeys.get(forkey);
	mapfileReaderCache.invalidate(cachedHandle);
	valueCache.invalidate(cachedValues);
	cachedHandle.clear();
	cachedValues.clear();
    }


    public void initializeMapFile(String pathsForMap, String akey) throws Exception{
	initializeMapFile(new String[] {pathsForMap}, akey);
    }
    public void initializeMapFile(String[] pathsForMap, String akey) throws Exception{
	if(mapfilehash.get(akey)!=null){
	    LOG.info("Clearing Caches for "+akey);
	    clearEntiresFor(akey);
	}else{
	    mapToValueCacheKeys.put(akey, new ArrayList<ValuePair>(500));
	    mapToValueCacheHandles.put(akey, new ArrayList<String>(100));
	}
	mapfilehash.put(akey, pathsForMap);
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
    
    public byte[] cacheStatistics(int which){
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
	return r.toByteArray();
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
    }
    public String[] readTextFile(String inp) throws Exception{
	FSDataInputStream in = _filesystem.open(new Path(inp));
	BufferedReader inb = new BufferedReader(new InputStreamReader(in));
	ArrayList<String> arl = new ArrayList<String>();

	while(true){
	    String s = inb.readLine();
	    if(s == null) break;
	    arl.add(s);
	}
	String[] s = new String[arl.size()];
	s = arl.toArray(s);
	return s;
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
    }


    public int run(int buglevel) throws Exception {
	_configuration = new Configuration();
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
