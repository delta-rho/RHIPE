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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class RHMR implements Tool {

    private static final Log log = LogFactory.getLog(RHMR.class);

    protected URLClassLoader urlloader;
    protected Environment env_;
    protected String[] argv_;
    protected Configuration config_;
    protected Hashtable<String, String> rhoptions_;
    protected Job job_;
    protected boolean debug_;
    public static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
    final static Log LOG = LogFactory.getLog(RHMR.class);

    public static void main(final String[] args) {
        int res;
        try {
            // (new RHMR()).doTest();
            // System.exit(0);
            final RHMR r = new RHMR();
            r.setConfig(new Configuration());
            res = ToolRunner.run(r.getConfig(), r, args);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            res = -2;
        }
        System.exit(res);
    }

    public static int fmain(final String[] args) throws Exception {
        final int res;
        // (new RHMR()).doTest();
        // System.exit(0);
        final RHMR r = new RHMR();
        r.setConfig(new Configuration());
        res = ToolRunner.run(r.getConfig(), r, args);
        return (res);
    }

    public static int fmain(final String[] args, final Configuration c) throws Exception {
        final int res;
        final RHMR r = new RHMR();
        r.setConfig(c);
        res = ToolRunner.run(r.getConfig(), r, args);
        return (res);
    }

    public void doTest() {
        int i;
        for (i = 0; i < 1; i++) {
            System.out.println("I=" + i);
        }
        System.out.println("Last I=" + i);
    }

    public Configuration getConfig() {
        return config_;
    }

    public void setConfig(final Configuration c) {
        config_ = c;
    }

    public void setConf(final Configuration c) {
    }

    protected void init() {
        try {
            debug_ = false;
            rhoptions_ = new Hashtable<String, String>();
            readParametersFromR(argv_[0]);
//            overrideParams();
            env_ = new Environment();
            job_ = Job.getInstance(config_);
            //get a reference to the job's new config object
            config_ = job_.getConfiguration();

            setConf();
            setJob();

        }
        catch (Exception io) {
            io.printStackTrace();
            throw new RuntimeException(io);
        }
    }

    private void overrideParams() {
//        rhoptions_.put("rhipe_input_folder","/Users/perk387/Projects/RHIPE/src/test/resources/hdfs/tmp/rhipeTest/irisData");
//        rhoptions_.put("rhipe_output_folder","/Users/perk387/out");
//        if(rhoptions_.containsKey("mapred.job.tracker"))
            rhoptions_.remove("mapred.job.tracker");
//        
//        if(rhoptions_.containsKey("fs.default.name"))
            rhoptions_.remove("fs.default.name");
//        
//        rhoptions_.put("rhipe_shared",""); //temp file
        rhoptions_.put("rhipe_job_async","FALSE");

//        String tmp = rhoptions_.put("rhipe_shared","");
//        try {
//            System.out.println("copying rhipe_shared file " + tmp);
//            File srcFile = new File(tmp);
//            if(srcFile.exists())
//                org.apache.commons.io.FileUtils.copyFile(srcFile,new File("temp_rhipe_shared"));
//            else {
//                rhoptions_.remove("fs.default.name");
//                log.warn("file " + tmp + " does note exist - ignoring");
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public Configuration getConf() {
        return config_;
    }

    public int run(final String[] args) throws Exception {
        this.argv_ = args;
        init();
        submitAndMonitorJob(argv_[0]);
        return 1;
    }

    public void setConf() throws IOException, URISyntaxException {
        final Enumeration keys = rhoptions_.keys();
        while (keys.hasMoreElements()) {
            final String key = (String) keys.nextElement();
            final String value = rhoptions_.get(key);
            config_.set(key, value);
            // System.out.println(key+"==="+value);
        }
        REXPHelper.setFieldSep(config_.get("mapred.field.separator", " "));
        // Shared is on the HDFS
        final String[] shared = config_.get("rhipe_shared").split(",");
        if (shared != null) {
            for (final String p : shared) {
                if (p.length() > 1) {
                    log.info("Adding to cache file:" + p);
//                    DistributedCache.addCacheFile(new URI(p), config_);
                    job_.addCacheFile(new URI(p));
                }
            }
        }
        // JARS are also on the HDFS
        final String[] jarfiles = config_.get("rhipe_jarfiles").split(",");
        if (jarfiles != null) {
            for (final String p : jarfiles) {
                // System.err.println("Adding "+ p +" to classpath");
                if (p.length() > 1) {
                    log.info("Adding to archive classpath:" + p);
//                    DistributedCache.addArchiveToClassPath(new Path(p), config_); //FileSystem.get(config_));
                    job_.addArchiveToClassPath(new Path(p));
                }
            }
        }
        final String[] zips = config_.get("rhipe_zips").split(",");
        if (zips != null) {
            for (final String p : zips) {
                // System.err.println("Adding zip "+ p +" to cache");
                if (p.length() > 1) {
                    log.info("Adding to cache archive:" + p);
//                    DistributedCache.addCacheArchive(new URI(p), config_);
                    job_.addCacheArchive(new URI(p));
                }
            }
        }

//        DistributedCache.createSymlink(config_);
        // if (!rhoptions_.get("rhipe_classpaths").equals("")) {
        // 	String[] cps = rhoptions_.get("rhipe_jarfiles").split(",");
        // 	for(String s : cps)
        // 	    DistributedCache.addFileToClassPath(new Path(s), config_);
        // }

        if (!rhoptions_.get("rhipe_classpaths").equals("")) {
            final String[] cps = rhoptions_.get("rhipe_classpaths").split(",");
            final URL[] us = new URL[cps.length];
            for (int i = 0; i < cps.length; i++) {
                try {
                    log.info("Adding to classpath: " + cps[i]);
                    us[i] = (new File(cps[i])).toURI().toURL();
                }
                catch (java.net.MalformedURLException e) {
                    throw new IOException(e);
                }
            }
            LOG.debug("Adding " + us.length + " JAR(s)");
            config_.setClassLoader(new URLClassLoader(us, config_.getClassLoader()));
            Thread.currentThread().setContextClassLoader(new URLClassLoader(us, Thread.currentThread().getContextClassLoader()));
        }

    }

    public void setJob() throws ClassNotFoundException, IOException, URISyntaxException {
        final Calendar cal = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
        final String jname = rhoptions_.get("rhipe_jobname");
        boolean uscomb = false;
//        job_.setUserClassesTakesPrecedence(true);
        //this will work on non-cloudera versions
        job_.getConfiguration().setBoolean("mapreduce.task.classpath.user.precedence",true);
        if (jname.equals("")) {
            job_.setJobName(sdf.format(cal.getTime()));
        }
        else {
            job_.setJobName(jname);
        }
        job_.setJarByClass(RHMR.class);
        // System.err.println(rhoptions_.get("rhipe_outputformat_class"));
        final Class<?> clz = config_.getClassByName(rhoptions_.get("rhipe_outputformat_class"));
        final Class<? extends OutputFormat> ofc = clz.asSubclass(OutputFormat.class);
        job_.setOutputFormatClass(ofc);

        final Class<?> clz2 = config_.getClassByName(rhoptions_.get("rhipe_inputformat_class"));
        final Class<? extends InputFormat> ifc = clz2.asSubclass(InputFormat.class);
        job_.setInputFormatClass(ifc);

        if (!rhoptions_.get("rhipe_input_folder").equals("")) {
            FileInputFormat.setInputPaths(job_, rhoptions_.get("rhipe_input_folder"));
        }

        // System.out.println("FOO");
        // System.out.println(rhoptions_.get("rhipe.use.hadoop.combiner"));

        if (rhoptions_.get("rhipe.use.hadoop.combiner").equals("TRUE")) {
            uscomb = true;
        }
        final String[] output_folder = rhoptions_.get("rhipe_output_folder").split(",");
        for (int i = 0; i < output_folder.length; i++) {
            log.debug(output_folder[i]);
        }
        // System.exit(0);
        if (!rhoptions_.get("rhipe_partitioner_class").equals("none")) {
            RHMRHelper.PARTITION_START = Integer.parseInt(rhoptions_.get("rhipe_partitioner_start")) - 1;
            RHMRHelper.PARTITION_END = Integer.parseInt(rhoptions_.get("rhipe_partitioner_end")) - 1;
            final Class<?> clz3 = config_.getClassByName(rhoptions_.get("rhipe_partitioner_class"));
            final Class<? extends org.apache.hadoop.mapreduce.Partitioner> pc = clz3.asSubclass(org.apache.hadoop.mapreduce.Partitioner.class);
            job_.setPartitionerClass(pc);
        }

        if (!output_folder[0].equals("")) {
            final Path ofp = new Path(output_folder[0]);
            final FileSystem srcFs = ofp.getFileSystem(job_.getConfiguration());
            srcFs.delete(ofp, true);
            if (rhoptions_.get("rhipe_outputformat_class").equals("org.apache.hadoop.mapreduce.lib.output.NullOutputFormat")) {
                srcFs.mkdirs(ofp);
            }
            FileOutputFormat.setOutputPath(job_, ofp);
        }
        job_.setMapOutputKeyClass(Class.forName(rhoptions_.get("rhipe_map_output_keyclass")));
        job_.setMapOutputValueClass(Class.forName(rhoptions_.get("rhipe_map_output_valueclass")));

        job_.setOutputKeyClass(Class.forName(rhoptions_.get("rhipe_outputformat_keyclass")));
        job_.setOutputValueClass(Class.forName(rhoptions_.get("rhipe_outputformat_valueclass")));


        job_.setMapperClass(RHMRMapper.class);
        if (uscomb) {
            job_.setCombinerClass(RHMRReducer.class);
        }
        job_.setReducerClass(RHMRReducer.class);

        // System.out.println("Conf done");

    }

    public int runasync(final String configfile) throws Exception {
        //this method overwrites the zonf file, not sure what it really does
        final FileOutputStream out = new FileOutputStream(configfile);
        final DataOutputStream fout = new DataOutputStream(out);
        final String[] arl = new String[4];
        // LOG.info("RUNNING ASYNC NOW");
        arl[0] = job_.getTrackingURL();
        arl[1] = job_.getJobName();
        // System.out.println(job_.getJobID()); // returns null ??
        final String[] split = arl[0].split("=");
//        System.out.println("job id url string:" + arl[0]);
        arl[2] = split.length > 1 ? split[1] : job_.getJobID().toString();  //this does not work in standalone mode
        arl[3] = RHMR.now();
        final REXP b = RObjects.makeStringVector(arl);
        final RHBytesWritable rb = new RHBytesWritable(b.toByteArray());
        rb.writeAsInt(fout);
        return (0);
    }

    public static String now() {
        final Calendar cal = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
        return sdf.format(cal.getTime());
    }

    public int submitAndMonitorJob(final String configfile) throws Exception {
        int k = 0;
        // LOG.info("submitting job");
//        job_.waitForCompletion(true);
        job_.submit();
        if (rhoptions_.get("rhipe_job_async").equals("TRUE")) {
            return (runasync(configfile));
        }
        LOG.info("Tracking URL ----> " + job_.getTrackingURL());
        final boolean verb = rhoptions_.get("rhipe_job_verbose").equals("TRUE");
        final long now = System.currentTimeMillis();
        final boolean result = job_.waitForCompletion(verb);
        final double tt = (System.currentTimeMillis() - now) / 1000.0;
        // We will overwrite the input configfile
        final FileOutputStream out = new FileOutputStream(configfile);
        final DataOutputStream fout = new DataOutputStream(out);
        try {
            if (!result) {
                k = -1;
            }
            else {
                k = 0;
                final org.apache.hadoop.mapreduce.Counters counter = job_.getCounters();
                final REXP r = RHMR.buildListFromCounters(counter, tt);
                final RHBytesWritable rb = new RHBytesWritable(r.toByteArray());
                rb.writeAsInt(fout);
            }
        }
        catch (Exception e) {
            k = -1;
        }
        finally {
            fout.close();
            out.close();
        }
        return k;
    }

    public static REXP buildListFromCounters(final org.apache.hadoop.mapreduce.Counters counters, final double tt) {
        //		String[] groupnames = counters.getGroupNames().toArray(new String[] {});
        final List<String> list = new ArrayList<String>();
        for (final String groupName : counters.getGroupNames()) {
            list.add(groupName);
        }
        String[] groupnames = new String[list.size()];
        groupnames = list.toArray(groupnames);

        final String[] groupdispname = new String[groupnames.length + 1];
        final Vector<REXP> cn = new Vector<REXP>();
        for (int i = 0; i < groupnames.length; i++) {
            final org.apache.hadoop.mapreduce.CounterGroup cgroup = counters.getGroup(groupnames[i]);
            groupdispname[i] = cgroup.getDisplayName();
            final REXP.Builder cvalues = REXP.newBuilder();
            final Vector<String> cnames = new Vector<String>();
            cvalues.setRclass(REXP.RClass.REAL);
            for (final org.apache.hadoop.mapreduce.Counter counter : cgroup) {
                cvalues.addRealValue((double) counter.getValue());
                cnames.add(counter.getDisplayName());
            }
            cvalues.addAttrName("names");
            cvalues.addAttrValue(RObjects.makeStringVector(cnames.toArray(new String[cnames.size()])));
            cn.add(cvalues.build());
        }
        groupdispname[groupnames.length] = "job_time";
        final REXP.Builder cvalues = REXP.newBuilder();
        cvalues.setRclass(REXP.RClass.REAL);
        cvalues.addRealValue(tt);
        cn.add(cvalues.build());
        return (RObjects.makeList(groupdispname, cn));
    }

    public void readParametersFromR(final String configFile) throws IOException {
        final FileInputStream in = new FileInputStream(configFile);
        final DataInputStream fin = new DataInputStream(in);
        byte[] d;
        String key, value;
        final int n0 = fin.readInt();
        int n;
        for (int i = 0; i < n0; i++) {
            // R Writes Null Terminated Strings(when I dont use char2Raw)
            try {
                n = fin.readInt();
                d = new byte[n];
                fin.readFully(d, 0, d.length);
                key = new String(d);

                n = fin.readInt();
                d = new byte[n];
                fin.readFully(d, 0, d.length);
                value = new String(d);
                rhoptions_.put(key, value);
            }
            catch (EOFException e) {
                throw new IOException(e);
            }
        }
        fin.close();
        if (debug_) {
            final Enumeration keys = rhoptions_.keys();
            while (keys.hasMoreElements()) {
                final String key0 = (String) keys.nextElement();
                final String value0 = rhoptions_.get(key0);
                log.debug(key0 + "=" + value0);
            }
        }
    }
}
