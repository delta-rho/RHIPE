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
// import org.xeustechnologies.jcl.JarClassLoader;
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
    // JarClassLoader jcl;

    public PersonalServer(){}

    public void setUserInfo( int bugl) throws InterruptedException,IOException {
	bbuf = new byte[100];
	this.buglevel = bugl;
	seqhash = new Hashtable<String, SequenceFile.Reader>();
	mrhash = new Hashtable<String, MapFile.Reader[]>();
	mapfilehash = new Hashtable<String, String[]>();
	fu = new FileUtils(_configuration);
	// jcl  = new JarClassLoader();
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

    // public void addJars(String s){
    // 	addJars(new String[]{s},null);
    // }
    // public void addJars(String[] jars,String folder){
    // 	if(jars!=null){
    // 	    for(String s: jars){
    // 		jcl.add(s);
    // 	    }
    // 	}
    // 	if(folder!=null){
    // 	    if(!folder.endsWith("/")) folder = folder+"/";
    // 	    jcl.add(folder);
    // 	}
    // }
    // public void addJars(String[] jars){
    // 	for(String s: jars){
    // 	    jcl.add(s);
    // 	}
    // }

    public int rhex(String zonf) throws Exception {
	int result = RHMR.fmain(new String[] {zonf});
	return result;
    }
    public int rhex(String[] zonf) throws Exception {
	int result = RHMR.fmain(zonf);
	return result;
    }
    
    public byte[] send_back(REXP r){
	REXP.Builder thevals = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	thevals.addRexpValue(r);
	RObjects.addAttr(thevals, "class", RObjects
			 .makeStringVector("worker_result"));
	return thevals.build().toByteArray();
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



    public void clearEntriesFor(String forkey) throws Exception{
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
	    clearEntriesFor(akey);
	}else{
	    mapToValueCacheKeys.put(akey, new ArrayList<ValuePair>(500));
	    mapToValueCacheHandles.put(akey, new ArrayList<String>(100));
	}
	mapfilehash.put(akey, pathsForMap);
    }

    public byte[] rhgetkeys2(String key, byte[] bb) throws Exception,IOException
    {
	final String[] pathsForMap = mapfilehash.get(key);

	RHBytesWritable v = new RHBytesWritable();
	RHBytesWritable k  = new RHBytesWritable();
	final RHNull anull = new RHNull();

	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
	REXP keys =  REXP.newBuilder().mergeFrom(bb, 0, bb.length).build();

	for(int i=0; i <  keys.getRexpValueCount();i++){
	    k.set( keys.getRexpValue(i).toByteArray());
	    
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
		REXP.Builder a   = REXP.newBuilder();
		// a.setRclass(REXP.RClass.LIST);
		// a.addRexpValue(  );
		// a.build();
		thevals.addRexpValue(RObjects.buildRawVector( v.getBytes(), 0, v.getLength()));
	}
	return thevals.build().toByteArray();
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


    public String[] readTextFile(String inp) throws Exception{
	return readTextFile(inp,-1);
    }
    public String[] readTextFile(String inp,int numlines) throws Exception{
	if(numlines <0)
	    numlines = java.lang.Integer.MAX_VALUE;
	FSDataInputStream in = _filesystem.open(new Path(inp));
	BufferedReader inb = new BufferedReader(new InputStreamReader(in));
	ArrayList<String> arl = new ArrayList<String>();
	int i=0;
	while(i < numlines){
	    String s = inb.readLine();
	    if(s == null) break;
	    arl.add(s);
	    i++;
	}
	String[] s = new String[arl.size()];
	s = arl.toArray(s);
	return s;
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
