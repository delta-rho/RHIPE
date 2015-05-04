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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.StringUtils;
import org.godhuli.rhipe.REXPProtos.REXP;

import com.google.common.collect.ImmutableSet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

public class RHMRHelper {
    
    private static final Log LOG = LogFactory.getLog(RHMRHelper.class.getName());
    private static int BUFFER_SIZE = 10 * 1024;
    private static final long REPORTER_OUT_DELAY = 10 * 1000L;
    private static final long REPORTER_ERR_DELAY = 10 * 1000L;
    private static final String ERROR_OUTPUT_DIR = "map-reduce-error";
    protected static boolean ENCODE_NULLS_IN_TEXT = false;
    protected static int PARTITION_START = 0, PARTITION_END = 0;
    private boolean copyFile;
    private Environment env_;
    private final String callID;
    private final String taskId;
    private final String jobId;
    private String errorOutputPath;
    private long numRecWritten_ = 0;
    private long joinDelay_;
    private boolean doPipe_;
    private boolean nonZeroExitIsFailure_;
    private FileSystem fileSystem;
    private Path outputFolder;
    private String copyExcludeRegex;
    private Process sim;
    private Class<? extends WritableComparable> keyclass;
    private MROutputThread outThread_;
    private MRErrorThread errThread_;
    private volatile DataOutputStream clientOut_;
    private volatile DataInputStream clientErr_;
    private volatile DataInputStream clientIn_;
    private boolean writeErr;
    private volatile Throwable outerrThreadsThrowable;
    private Configuration _cfg;
    
    private final static ImmutableSet<String> rhipeKeys = ImmutableSet.of("rhipe_setup_map", "rhipe_map", "rhipe_cleanup_map", "rhipe_setup_reduce", "rhipe_reduce_prekey", "rhipe_reduce", "rhipe_reduce_postkey", "rhipe_cleanup_reduce");

    public RHMRHelper(String fromWHo, String jobId, String taskId) {
        this.callID = fromWHo;
        this.jobId = jobId;
        this.taskId = taskId;
        this.errorOutputPath = "/tmp/" + ERROR_OUTPUT_DIR;
    }

    public int exitval() {
        int exitVal = 0;
        try {
            exitVal = sim.waitFor();
        }
        catch (InterruptedException e) {
            exitVal = -99;
        }
        return exitVal;
    }

    private void addJobConfToEnvironment(final Configuration conf, final Properties env) {
        for (final Object aConf : conf) {
            final Map.Entry en = (Map.Entry) aConf;
            final String name = (String) en.getKey();
            if (name.equals("mapred.input.dir") || name.equals("rhipe_input_folder")) {
                continue;
            }
            String value;
            if(name.equals("HADOOP.TMP.FOLDER")){
                String t = conf.get(name);
                if(t != null && t.length() > 0)
                    errorOutputPath = t + (t.endsWith("/") ? "" : "/") + ERROR_OUTPUT_DIR;
            }
            if (!(name.equals("LD_LIBRARY_PATH") || name.equals("PATH"))) {
                value = conf.get(name); // does variable expansion
            }
            else {
                value = conf.getRaw(name);
            }
            if(rhipeKeys.contains(name)){
                try {
                    File file = new File(name);
                    org.apache.commons.io.FileUtils.writeStringToFile(file,value,"UTF-8");
                    env.put(name, file.getAbsolutePath());
                    LOG.info(name + "::writing to file:" + file.getAbsolutePath());
                } catch (IOException e) {
                    LOG.error("error writing R code to file - this job may fail",e);
                }
            }
            else {
                env.put(name, value);
            }
        }
    }

    private void doPartitionRelatedSetup(final Configuration cfg) {
        if (!cfg.get("rhipe_partitioner_class").equals("none")) {
            RHMRHelper.PARTITION_START = Integer.parseInt(cfg.get("rhipe_partitioner_start")) - 1;
            RHMRHelper.PARTITION_END = Integer.parseInt(cfg.get("rhipe_partitioner_end")) - 1;
        }
    }

    public void setup(final Configuration cfg, final String argv, final boolean doPipe) {
        try {
            // 	    InetAddress addr = InetAddress.getLocalHost();
            // 	    hostname = addr.getHostName();
            doPartitionRelatedSetup(cfg);
            String squote = cfg.get("rhipe_string_quote");
            if (squote == null) {
                squote = "";
            }

	    String encodenull =cfg.get("rhipe_textinputformat_encode_null");
	    if(encodenull == null || encodenull.equals("TRUE")){
		RHMRHelper.ENCODE_NULLS_IN_TEXT=true;
	    }else{
		RHMRHelper.ENCODE_NULLS_IN_TEXT=false;
	    }
            REXPHelper.setFieldSep(cfg.get("mapred.field.separator", " "));
            REXPHelper.setStringQuote(squote);

            writeErr = cfg.get("rhipe_test_output") != null && cfg.get("rhipe_test_output").equals("TRUE");

            BUFFER_SIZE = cfg.getInt("rhipe_stream_buffer", 10 * 1024);
            joinDelay_ = cfg.getLong("rhipe_joindelay_milli", 0);
            nonZeroExitIsFailure_ = cfg.getBoolean("rhipe_non_zero_exit_is_failure", true);
            doPipe_ = doPipe;
            fileSystem = FileSystem.get(cfg);

            Class<?> _kc = null;

            if (callID.equals("Mapper")) {
                if (cfg.getInt("mapred.reduce.tasks", 0) == 0) {
                    _kc = Class.forName(cfg.get("rhipe_outputformat_keyclass"));
                }
                else {
                    _kc = Class.forName(cfg.get("rhipe_map_output_keyclass"));
                }
            }
            else {
                _kc = Class.forName(cfg.get("rhipe_outputformat_keyclass"));
            }
            keyclass = _kc.asSubclass(WritableComparable.class);

            copyFile = cfg.get("rhipe_copy_file").equals("TRUE");

            if (cfg.get("rhipe_output_folder") != null) {
                String subp = "_outputs";
                if (cfg.get("rhipe_copyfile_folder") != null) {
                    subp = cfg.get("rhipe_copyfile_folder");
                }
                if (copyFile) {
                    outputFolder = new Path(cfg.get("rhipe_output_folder") + "/" + subp);
                    outputFolder.getFileSystem(cfg).mkdirs(outputFolder);
                    copyExcludeRegex = cfg.get("rhipe_copy_excludes");
                }
            }
            if (!doPipe_) {
                return;
            }
            final String[] argvSplit = argv.split(" ");
            final String prog = argvSplit[0];
            final Environment childEnv = (Environment) env().clone();
            cfg.set("io_sort_mb", cfg.get("io.sort.mb"));
            addJobConfToEnvironment(cfg, childEnv);
            childEnv.put("TMPDIR", System.getProperty("java.io.tmpdir"));
            // Start the process

            long totalLength = 0;
            System.out.println("arg count: " + argvSplit.length);
            for (String s : argvSplit) {
              //get byte count - only works this way with UTF-8
              totalLength += s.length();
              System.out.println(s);
            }
            System.out.println("total bytes of args:" + Long.toString(totalLength));
            
            final ProcessBuilder builder = new ProcessBuilder(argvSplit);
            builder.environment().putAll(childEnv.toMap());
            sim = builder.start();
            clientOut_ = new DataOutputStream(new BufferedOutputStream(sim.getOutputStream(), BUFFER_SIZE));
            clientIn_ = new DataInputStream(new BufferedInputStream(sim.getInputStream(), BUFFER_SIZE));
            clientErr_ = new DataInputStream(new BufferedInputStream(sim.getErrorStream()));
//            startTime_ = System.currentTimeMillis();
            LOG.info(callID + ":" + "Started external program:" + argv);
            errThread_ = new MRErrorThread();
            LOG.info(callID + ":" + "Started Error Thread");
            errThread_.start();
            this._cfg = cfg;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("configuration exception", e);
        }
    }


    public void startOutputThreads(final TaskInputOutputContext<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> ctx) {
        outThread_ = new MROutputThread(ctx, true);
        outThread_.start();
        errThread_.setContext(ctx);
        LOG.info(callID + ":" + "Started Output Thread");
    }


    public void mapRedFinished(final TaskInputOutputContext<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> ctx) {
        try {
            if (!doPipe_) {
                return;
            }
            try {
                if (clientOut_ != null) {
                    clientOut_.flush();
                    clientOut_.close();
                }
            }
            catch (IOException io) {
                //ignore
            }

            waitOutputThreads(ctx);
            if (sim != null) {
                sim.destroy();
            }
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }
    }


    private void waitOutputThreads(final TaskInputOutputContext<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> ctx) {
        try {
            final int exitVal = sim.waitFor();
            if (exitVal != 0) {
                if (nonZeroExitIsFailure_) {
                    ctx.getCounter("R_ERRORS", "subprocess failed with code: " + exitVal).increment(1);
                }
                else {
                    ctx.getCounter("R_SUBPROCESS", "subprocess failed with code: " + exitVal).increment(1);
                }
            }
            if (outThread_ != null) {
                outThread_.join(joinDelay_);
            }
            if (errThread_ != null) {
                errThread_.join(joinDelay_);
            }
        }
        catch (InterruptedException e) {
            //ignore
        }
    }


    public void checkOuterrThreadsThrowable() throws IOException {
        if (outerrThreadsThrowable != null) {
            String rdaDumpPath = null;
            try {
                //find the local rda file
                final String workingDir = System.getProperty("user.dir");
                final File file = new File(workingDir,"last.dump.rda");
                //if it exists copy it to hdfs
                if(file.exists()){
                    LOG.info("moving last.dump.rda");
                    //save the rda file if there is one
                    //create the path on hdfs to save it
                    Path destPath = new Path(errorOutputPath + jobId, taskId);
                    FileSystem fs = destPath.getFileSystem(this._cfg);
                    if(!fs.exists(destPath)){
                        fs.mkdirs(destPath);
                    }
                    Path rdaFile = new Path(workingDir,"last.dump.rda");
                    fs.copyFromLocalFile(false, rdaFile, destPath);
                    rdaDumpPath = destPath.toString();
                }
                else{
                    LOG.warn("No last.dump.rda file found");
                }
            }
            catch (Exception e) {
                LOG.error("There was an error while processing an R error (ironic) but it will be ignored.\nFor reference:",e);
            }
            
            String rError = StringUtils.stringifyException(outerrThreadsThrowable);
            
            if(rdaDumpPath != null) {
                rError = "\nrda dump file path:" + rdaDumpPath + "\n" + rError;
            }
            throw new IOException("MROutput/MRErrThread failed:" + rError);
        }
    }

    public String getSimExitInfo() throws IllegalThreadStateException {
        String extraInfo = "";
        final int exitVal = sim.exitValue();
        if (exitVal == 0) {
            extraInfo = "subprocess exited successfully\n";
        }
        else {
            extraInfo = "subprocess exited with error code " + exitVal + "\n";
        }
        return (extraInfo);
    }

    public void writeCMD(final int s) throws IOException {
        WritableUtils.writeVInt(clientOut_, s);
    }

    public void write(final RHBytesWritable c) throws IOException {
        c.write(clientOut_);
    }

    public void write(final WritableComparable c) throws IOException {
        c.write(clientOut_);
    }


    class MROutputThread extends Thread {
        volatile TaskInputOutputContext<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> ctx;
        // volatile TaskInputOutputContext <Object,Object,
        //     Object,Object> ctx;

        long lastStdoutReport = 0;

        MROutputThread(final TaskInputOutputContext<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> ctx, final boolean isD) {
            setDaemon(isD);
            this.ctx = ctx;
        }

        boolean readRecord(final WritableComparable k, final Writable v) {
            try {
                k.readFields(clientIn_);
                v.readFields(clientIn_);

            }
            catch (IOException e) {
                return (false);
            }
            return (true);
        }

        public void run() {
            final RHBytesWritable value = new RHBytesWritable();
            WritableComparable key = null;
            try {
                key = keyclass.newInstance();
            }
            catch (InstantiationException e) {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            try {
                while (readRecord(key, value)) {
                    ctx.write(key, value);
                    numRecWritten_++;
                    final long now = System.currentTimeMillis();
                    if (now - lastStdoutReport > REPORTER_OUT_DELAY) {
                        lastStdoutReport = now;
                        ctx.setStatus("R/W merrily moving along: W=" + numRecWritten_ + " ");
                    }
                }
                if (clientIn_ != null) {
                    clientIn_.close();
                    clientIn_ = null;
                    LOG.info(callID + ":" + "MROutputThread done");
                }
            }
            catch (EOFException e) {
                LOG.info("Acchoo");
            }
            catch (Throwable th) {
                outerrThreadsThrowable = th;
                LOG.warn(callID + ":" + StringUtils.stringifyException(th));
                try {
                    if (clientIn_ != null) {
                        clientIn_.close();
                        clientIn_ = null;
                    }
                }
                catch (IOException io) {
                    LOG.info(StringUtils.stringifyException(io));
                }
                // throw new IOException(th);
            }
        }
    }


    class MRErrorThread extends Thread {
        long lastStderrReport = 0;
        volatile TaskInputOutputContext ctx;

        public MRErrorThread() {
            setDaemon(true);
        }

        public void setContext(final TaskInputOutputContext ctx) {
            this.ctx = ctx;
        }

        public void run() {
            try {
                try {
                    int ln;
                    byte[] k;
                    while (true) {
                        final int cmd = clientErr_.readByte();
                        switch (cmd) {
                            case RHTypes.ERROR_MSG:
                                ln = clientErr_.readInt();
                                k = new byte[ln];
                                clientErr_.readFully(k, 0, ln);
                                final String errmsg = new String(k);
                                ctx.getCounter("R_ERRORS", errmsg).increment(1);
                                final int y = errmsg.length();
                                throw new RuntimeException(errmsg);
                            case RHTypes.PRINT_MSG:
                                ln = clientErr_.readInt();
                                k = new byte[ln];
                                clientErr_.readFully(k, 0, ln);
                                final String pmsg = new String(k);
                                LOG.info(pmsg);
                                break;
                            case RHTypes.SET_STATUS:
                                ln = clientErr_.readInt();
                                k = new byte[ln];
                                clientErr_.readFully(k, 0, ln);
                                final String status = new String(k);
                                ctx.setStatus(status);
                                break;
                            case RHTypes.SET_COUNTER:
                                ln = RHBytesWritable.readVInt(clientErr_);
                                k = new byte[ln];
                                clientErr_.readFully(k, 0, ln);
                                final REXP r = REXP.parseFrom(k);
                                final String grcnt = REXPHelper.toString_(r.getRexpValue(0));
                                final String subcnt = REXPHelper.toString_(r.getRexpValue(1));
                                final long value = (long) (Double.parseDouble(REXPHelper.toString_(r.getRexpValue(2))));
                                ctx.getCounter(grcnt, subcnt).increment(value);
                                break;
                            default:
                                if (writeErr) {
                                    final BufferedReader d = new BufferedReader(new InputStreamReader(clientErr_));
                                    int l = 0;
                                    while (true) {
                                        final String line = d.readLine();
                                        if (line == null) {
                                            break;
                                        }
                                        LOG.debug("RHIPE Runner Output[" + l + "]: " + line);
                                        l++;
                                    }
                                }

                        }
                        final long now = System.currentTimeMillis();
                        if (now - lastStderrReport > REPORTER_ERR_DELAY) {
                            lastStderrReport = now;
                            if (ctx != null) {
                                ctx.progress();
                            }
                        }
                    }
                }
                catch (EOFException e) {
                    if (clientErr_ != null) {
                        clientErr_.close();
                        clientErr_ = null;

                        LOG.info(callID + ":" + "MRErrorThread done");
                    }
                }
            }
            catch (Throwable th) {
                outerrThreadsThrowable = th;
                LOG.warn(callID + ":" + StringUtils.stringifyException(th));
                try {
                    if (clientErr_ != null) {
                        clientErr_.close();
                        clientErr_ = null;
                    }
                }
                catch (IOException io) {
                    LOG.info(callID + ":" + StringUtils.stringifyException(io));
                }
            }
        }
    }

    private Environment env() {
        if (env_ != null) {
            return env_;
        }
        try {
            env_ = new Environment();
        }
        catch (IOException io) {
            io.printStackTrace();
        }
        return env_;
    }

    private void _walk(final String path, final ArrayList<Path> a, final String ex) {
        final File root = new File(path);
        final File[] list = root.listFiles();
        if (list != null) {
            for (final File f : list) {
                if (f.toString().matches(ex)) {
                    continue;
                }
                if (f.isDirectory() && f.listFiles().length > 0) {
                    _walk(f.getAbsolutePath(), a, ex);
                }
                else {
                    if (f.length() > 0) {
                        a.add(new Path(f.toString()));
                    }
                }
            }
        }
    }

    public void copyFiles(final String dirfrom) throws IOException {
        if (copyFile) {
            final ArrayList<Path> lop = new ArrayList<Path>();
            _walk(dirfrom, lop, copyExcludeRegex);
            FileSystem fs = outputFolder.getFileSystem(this._cfg);
            if (lop.size() > 0) {
                fs.copyFromLocalFile(false, true, lop.toArray(new Path[lop.size()]), outputFolder);
            }
        }
    }
}
