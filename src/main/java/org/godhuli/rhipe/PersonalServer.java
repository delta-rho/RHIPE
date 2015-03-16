package org.godhuli.rhipe;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PersonalServer {
    protected static final Log LOG = LogFactory.getLog(PersonalServer.class.getName());
    private final Map<String, ArrayList<ValuePair>> mapToValueCacheKeys = new HashMap<String, ArrayList<ValuePair>>();
    private final Map<String, ArrayList<String>> mapToValueCacheHandles = new HashMap<String, ArrayList<String>>();
    private Configuration _configuration;
    private HashPartitioner<RHBytesWritable, RHBytesWritable> _hp;
    private Map<String, String[]> mapfilehash;
    private Cache<ValuePair, RHBytesWritable> valueCache;
    private Cache<String, MapFile.Reader> mapfileReaderCache;
    private FileUtils fu;


    public PersonalServer() {
    }

    public void logTest(final String someMessage) {
        LOG.trace(someMessage);
        LOG.debug(someMessage);
        LOG.info(someMessage);
        LOG.warn(someMessage);
        LOG.error(someMessage);
        LOG.fatal(someMessage);
    }
    public void setUserInfo(final int bugl) throws IOException {
        mapfilehash = new HashMap<String, String[]>();
        fu = new FileUtils(_configuration);
    }

    public FileUtils getFU() {
        return fu;
    }

    public Configuration getConf() {
        return _configuration;
    }

    public void setConf(final Configuration c) {
        _configuration = c;
    }

    public byte[] rhmropts() {
        final REXP b = fu.mapRedOpts();
        return b.toByteArray();
    }

    public byte[] rhls(final String p, final int a) throws Exception {
        return rhls(new String[]{p}, a);
    }

    public byte[] rhls(final String[] p, final int a) throws Exception {
        final String[] result0 = fu.ls(p, a);
        final REXP b = RObjects.makeStringVector(result0);
        return b.toByteArray();
    }

    public void rhdel(final String folder) throws Exception {
        rhdel(new String[]{folder});
    }

    public void rhdel(final String[] folder) throws Exception {
        for (final String s : folder) {
            fu.delete(s, true);
        }
    }

    public void rhget(final String src, final String dest) throws Exception {
        LOG.debug("Copying " + src + " to " + dest);
        fu.copyMain(src, dest);
    }

    public void rhGet(final String src, final String dest) throws Exception {
        LOG.debug("Copying " + src + " to " + dest);
        Path srcPath = new Path(src); // note, the default fs for a path is HDFS (or whatever your conf file says)
        Path destPath = new Path(dest); 
	final FileSystem hdfFS = srcPath.getFileSystem(_configuration);
        hdfFS.copyToLocalFile(false,srcPath,destPath);
    }

    public void rhput(final String local, final String dest2, final boolean overwrite) throws Exception {
        rhput(new String[]{local}, dest2, overwrite);
    }

    public void rhput(final String[] locals, final String dest2, final boolean overwrite) throws Exception {
        fu.copyFromLocalFile(locals, dest2, overwrite);
    }

    public byte[] rhstatus(final String s, final boolean b) throws Exception {
        final REXP result = fu.getStatus(s, b);
        return result.toByteArray();
    }

    public byte[] rhjoin(final String a, final boolean b) throws Exception {
        final REXP result = fu.joinJob(a, b);
        return result.toByteArray();
    }

    public void rhkill(final String s) throws Exception {
        fu.killJob(s);
    }

    public int rhex(final String zonf) throws Exception {
        return RHMR.fmain(new String[]{zonf});
    }

    public int rhex(final String zonf, final Configuration c) throws Exception {
        //System.out.println("backing up configuration");
//        DataOutput dataOutput = new ObjectOutputStream(new FileOutputStream("configuration-backup.bin"));
//        c.write(dataOutput);
//        c.writeXml(new FileOutputStream("configuration-backup.xml"));
        //org.apache.commons.io.FileUtils.copyFile(new File(zonf),new File("zonf"));
        //System.out.println("done");
        return RHMR.fmain(new String[]{zonf}, c);
    }

    public int rhex(final String[] zonf) throws Exception {
        return RHMR.fmain(zonf);
    }

    public byte[] send_back(final REXP r) {
        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        thevals.addRexpValue(r);
        RObjects.addAttr(thevals, "class", RObjects.makeStringVector("worker_result"));
        return thevals.build().toByteArray();
    }

    public void initializeCaches(final int a, final int b) {
        valueCache = CacheBuilder.newBuilder().maximumWeight(a) //max MB in bytes, set to 100MB
                .weigher(new Weigher<ValuePair, RHBytesWritable>() {
                    public int weigh(final ValuePair k, final RHBytesWritable g) {
                        return k.getKey().getLength() + g.getLength();
                    }
                }).recordStats().build();

        final RemovalListener<String, MapFile.Reader> removalListener = new RemovalListener<String, MapFile.Reader>() {
            public void onRemoval(final RemovalNotification<String, MapFile.Reader> removal) throws RuntimeException {
                try {
                    final MapFile.Reader conn = removal.getValue();
                    if (conn != null) {
                        LOG.info("Closing a mapfile, emptied from cache:" + conn);
                        conn.close();
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        mapfileReaderCache = CacheBuilder.newBuilder().maximumSize(b).removalListener(removalListener).recordStats().build();
    }

    public void clearEntriesFor(final String forkey) {
        final ArrayList<String> cachedHandle = mapToValueCacheHandles.get(forkey);
        final ArrayList<ValuePair> cachedValues = mapToValueCacheKeys.get(forkey);
        mapfileReaderCache.invalidate(cachedHandle);
        valueCache.invalidate(cachedValues);
        cachedHandle.clear();
        cachedValues.clear();
    }

    public void initializeMapFile(final String pathsForMap, final String akey) throws Exception {
        initializeMapFile(new String[]{pathsForMap}, akey);
    }

    public void initializeMapFile(final String[] pathsForMap, final String akey) {
        if (mapfilehash.get(akey) != null) {
            LOG.info("Clearing Caches for " + akey);
            clearEntriesFor(akey);
        }
        else {
            mapToValueCacheKeys.put(akey, new ArrayList<ValuePair>(500));
            mapToValueCacheHandles.put(akey, new ArrayList<String>(100));
        }
        mapfilehash.put(akey, pathsForMap);
    }

    public byte[] rhgetkeys2(final String key, final byte[] bb) throws Exception {
        final String[] pathsForMap = mapfilehash.get(key);

        RHBytesWritable v = new RHBytesWritable();
        final RHBytesWritable k = new RHBytesWritable();
        final RHNull anull = new RHNull();

        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        final REXP keys = REXP.newBuilder().mergeFrom(bb, 0, bb.length).build();

        for (int i = 0; i < keys.getRexpValueCount(); i++) {
            k.set(keys.getRexpValue(i).toByteArray());

            final ValuePair vp = new ValuePair(key, k);
            v = valueCache.getIfPresent(vp);
            if (v == null) {
                // LOG.info("Not found requested key:"+vp);
                final RHBytesWritable a = new RHBytesWritable();
                final int which = _hp.getPartition(k, a, pathsForMap.length);
                MapFile.Reader f = mapfileReaderCache.getIfPresent(pathsForMap[which]);
                if (f == null) {
                    f = new MapFile.Reader(new Path(pathsForMap[which]), _configuration);
                    mapfileReaderCache.put(pathsForMap[which], f);
                    mapToValueCacheHandles.get(key).add(pathsForMap[which]);
                }
                final RHBytesWritable ww = (RHBytesWritable) f.get(k, a);
                if (ww == null) {
                    v = anull;
                }
                else {
                    v = ww;
                }
                mapToValueCacheKeys.get(key).add(vp);
                valueCache.put(vp, v);
            }
            final REXP.Builder a = REXP.newBuilder();
            // a.setRclass(REXP.RClass.LIST);
            // a.addRexpValue(  );
            // a.build();
            thevals.addRexpValue(RObjects.buildRawVector(v.getBytes(), 0, v.getLength()));
        }
        return thevals.build().toByteArray();
    }

    public byte[] cacheStatistics(final int which) {
        CacheStats c = null;
        if (which == 0) {
            c = mapfileReaderCache.stats();
        }
        else if (which == 1) {
            c = valueCache.stats();
        }
        if (c != null) {
            final REXP r = RObjects.buildDoubleVector(new double[]{c.averageLoadPenalty() * 1e-6, (double) c.evictionCount(), (double) c.hitCount(), c.hitRate(), (double) c.loadCount(), (double) c.loadExceptionCount(), c.loadExceptionRate(), (double) c.loadSuccessCount(), (double) c.missCount(), c.missRate(), (double) c.requestCount(), ((double) c.totalLoadTime()) * 1e-6}).build();
            return r.toByteArray();
        }
        return new byte[]{};
    }

    public String[] readTextFile(final String inp) throws Exception {
        return readTextFile(inp, -1);
    }

    public String[] readTextFile(final String inp, int numlines) throws Exception {
        if (numlines < 0) {
            numlines = java.lang.Integer.MAX_VALUE;
        }
        final Path thisPath = new Path(inp);
        final FileSystem thisfs = thisPath.getFileSystem(_configuration);
        final FSDataInputStream in = thisfs.open(thisPath);
        final BufferedReader inb = new BufferedReader(new InputStreamReader(in));
        final ArrayList<String> arl = new ArrayList<String>();
        int i = 0;
        while (i < numlines) {
            final String s = inb.readLine();
            if (s == null) {
                break;
            }
            arl.add(s);
            i++;
        }
        String[] s = new String[arl.size()];
        s = arl.toArray(s);
        return s;
    }

    public int run(final int buglevel) throws Exception {
        _configuration = new Configuration();
        _hp = RHMapFileOutputFormat.getHP();
        setUserInfo(buglevel);
        return (0);
    }

    public void dumpConf(){
        dumpConf(_configuration);
    }

    public static void dumpConf(Configuration conf){
        try {
            Configuration.dumpConfiguration(conf,new FileWriter("conf-dump-" + System.currentTimeMillis() + ".txt"));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
    public abstract class DelayedExceptionThrowing {
        abstract void process(Path p, FileSystem srcFs) throws IOException;

        final void globAndProcess(final Path srcPattern, final FileSystem srcFs) throws IOException {
            final ArrayList<IOException> exceptions = new ArrayList<IOException>();
            for (final Path p : FileUtil.stat2Paths(srcFs.globStatus(srcPattern), srcPattern)) {
                try {
                    process(p, srcFs);
                }
                catch (IOException ioe) {
                    exceptions.add(ioe);
                }
            }

            if (!exceptions.isEmpty()) {
                if (exceptions.size() == 1) {
                    throw exceptions.get(0);
                }
                else {
                    throw new IOException("Multiple IOExceptions: " + exceptions);
                }
            }
        }
    }

}
