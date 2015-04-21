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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;

public class FileUtils {

    private static final Log log = LogFactory.getLog(FileUtils.class);
    private static final String COPY_TO_LOCAL = "_copyToLocal_";
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final String fsep = "\t";
    private final FsShell fsshell;
    private final Configuration cfg;
    private final JobConf jobconf;
    private JobClient jclient;

    public FileUtils(final Configuration cfg) throws IOException {
        this.cfg = cfg;
        fsshell = new FsShell(cfg);
        jobconf = new JobConf(cfg);
        jclient = new JobClient(jobconf);
    }

    public static String getStackTrace(final Throwable throwable) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        throwable.printStackTrace(printWriter);
        return result.toString();
    }

    public static REXP buildListFromOldCounter(final Counters c, final double dur) {

        final Collection<String> groupNamesCollection = c.getGroupNames();
        final String[] groupNames = new String[groupNamesCollection.size()];
        groupNamesCollection.toArray(groupNames);

        final String[] groupDisplayName = new String[groupNames.length + 1];

        final List<REXP> cn = new ArrayList<REXP>();
        for (int i = 0; i < groupNames.length; i++) {
            final Counters.Group cgroup = c.getGroup(groupNames[i]);
            groupDisplayName[i] = cgroup.getDisplayName();
            final REXP.Builder cValues = REXP.newBuilder();
            final List<String> cNames = new ArrayList<String>();
            cValues.setRclass(REXP.RClass.REAL);
            for (final Counters.Counter counter : cgroup) {
                cValues.addRealValue((double) counter.getValue());
                cNames.add(counter.getDisplayName());
            }
            cValues.addAttrName("names");
            cValues.addAttrValue(RObjects.makeStringVector(cNames.toArray(new String[cNames.size()])));
            cn.add(cValues.build());
        }
        groupDisplayName[groupNames.length] = "job_time";
        final REXP.Builder cValues = REXP.newBuilder();
        cValues.setRclass(REXP.RClass.REAL);
        cValues.addRealValue(dur);
        cn.add(cValues.build());
        return (RObjects.makeList(groupDisplayName, cn));
    }

    public Configuration getConf() {
        return (cfg);
    }

    public FsShell getFsShell() {
        return fsshell;
    }

    public void copyFromLocalFile(final String[] src, final String dst, final boolean overwrite) throws IOException {
        final Path dstPath = new Path(dst);
        final FileSystem dstFs = dstPath.getFileSystem(cfg);
        final Path[] srcp = new Path[src.length];
        for (int i = 0; i < src.length; i++) {
            srcp[i] = new Path(src[i]);
        }
        dstFs.copyFromLocalFile(false, overwrite, srcp, dstPath);
    }

    public void copyToLocal(final FileSystem srcFS, final Path src, final File dst) throws IOException {
        if (!srcFS.getFileStatus(src).isDirectory()) {
            final File tmp = FileUtil.createLocalTempFile(dst.getAbsoluteFile(), COPY_TO_LOCAL, true);
            if (!FileUtil.copy(srcFS, src, tmp, false, srcFS.getConf())) {
                throw new IOException("Failed to copy " + src + " to " + dst);
            }
            if (!tmp.renameTo(dst)) {
                throw new IOException("Failed to rename tmp file " + tmp + " to local destination \"" + dst + "\".");
            }
        }
        else {
            if (dst.mkdirs()) {
                for (final FileStatus path : srcFS.listStatus(src)) {
                    copyToLocal(srcFS, path.getPath(), new File(dst, path.getPath().getName()));
                }
            }
        }
    }

    public void makeFolderToDelete(final String s) throws IOException {
        final Path p = new Path(s);
        final FileSystem fs = p.getFileSystem(getConf());
        fs.mkdirs(p);
        fs.deleteOnExit(p);
    }

    public void copyMain(final String src, final String dest) throws IOException {
        final File dst = new File(dest);
        final Path srcpath = new Path(src);
        final FileSystem srcFS = srcpath.getFileSystem(getConf());
        final FileStatus[] srcs = srcFS.globStatus(srcpath);
        final boolean dstIsDir = dst.isDirectory();
        if (srcs.length > 1 && !dstIsDir) {
            throw new IOException("When copying multiple files, " + "destination should be a directory.");
        }
        for (final FileStatus status : srcs) {
            final Path p = status.getPath();
            final File f = dstIsDir ? new File(dst, p.getName()) : dst;
            copyToLocal(srcFS, p, f);
        }
    }

    public String[] ls(final String[] r, final int f) throws IOException, URISyntaxException {
        final ArrayList<String> lsco = new ArrayList<String>();
        for (final String path : r) {
            ls__(path, lsco, f > 0);
        }
        return (lsco.toArray(new String[lsco.size()]));
    }

    private void ls__(final String path, final ArrayList<String> lsco, final boolean dorecurse) throws IOException, URISyntaxException {

        final Path spath = new Path(path);
        final FileSystem srcFS = spath.getFileSystem(getConf());
        FileStatus[] srcs;

        final URI fsUri = new URI(getConf().get("fs.default.name"));
        final String fsUriScheme = fsUri.getScheme();

        srcs = srcFS.globStatus(spath);
        if (srcs == null || srcs.length == 0) {
            throw new FileNotFoundException("Cannot access " + path +
                    ": No such file or directory.");
        }
        if (srcs.length == 1 && srcs[0].isDirectory()) {
            srcs = srcFS.listStatus(srcs[0].getPath());
        }

        for (final FileStatus status : srcs) {
            final StringBuilder sb = new StringBuilder();
            final boolean idir = status.isDirectory();
            final String x = idir ? "d" : "-";
            if (dorecurse && idir) {
                ls__(status.getPath().toUri().toString(), lsco, dorecurse);
            }
            else {
                sb.append(x);
                sb.append(status.getPermission().toString());
                sb.append(fsep);

                sb.append(status.getOwner());
                sb.append(fsep);

                sb.append(status.getGroup());
                sb.append(fsep);

                sb.append(status.getLen());
                sb.append(fsep);

                final Date d = new Date(status.getModificationTime());
                sb.append(formatter.format(d));
                sb.append(fsep);

                final String curScheme = status.getPath().toUri().getScheme();
                if(fsUriScheme.equals(curScheme)) {
                    sb.append(status.getPath().toUri().getPath());
                } else {
                    sb.append(status.getPath().toUri().toString());
                }
                lsco.add(sb.toString());
            }
        }
    }

    public void delete(final String srcf, final boolean recursive) throws IOException {
        final Path srcPattern = new Path(srcf);
        new DelayedExceptionThrowing() {
            @Override
            void process(final Path p, final FileSystem srcFs) throws IOException {
                delete(p, srcFs, recursive);
            }
        }.globAndProcess(srcPattern, srcPattern.getFileSystem(getConf()));
    }

    /* delete a file */
    private void delete(final Path src, final FileSystem srcFs, final boolean recursive) throws IOException {

        if (srcFs.getFileStatus(src).isDirectory() && !recursive) {
            throw new IOException("Cannot remove directory \"" + src + "\", use -rmr instead");
        }
        final Trash trashTmp = new Trash(srcFs, getConf());
        if (trashTmp.moveToTrash(src)) {
            log.info("Moved to trash: " + src);
            return;
        }
        if (srcFs.delete(src, true)) {
            log.info("Deleted " + src);
        }
        else {
            if (!srcFs.exists(src)) {
                throw new FileNotFoundException("cannot remove " + src + ": No such file or directory.");
            }
            throw new IOException("Delete failed " + src);
        }
    }

    public REXP readInfo(final String file) throws IOException {
        final DataInputStream in = new DataInputStream(new FileInputStream(file));
        return (REXP.parseFrom(in));
    }

    public REXP mapRedOpts() {
        final Iterator<Map.Entry<String, String>> iter = getConf().iterator();
        final List<REXP> ent = new ArrayList<REXP>();
        final List<String> str = new ArrayList<String>();
        while (iter.hasNext()) {
            final Map.Entry<String, String> c = iter.next();
            final String key = c.getKey();
            final String value = c.getValue();
            str.add(key);
            ent.add(RObjects.makeStringVector(value));
        }
        return (RObjects.makeList(str, ent));
    }

    public void hdfsRename(final REXP rexp0) throws Exception {
        final REXP spaths = rexp0.getRexpValue(0);
        final REXP tpaths = rexp0.getRexpValue(1);
        final int np = spaths.getRexpValueCount();
        final FileSystem fs = FileSystem.get(getConf());
        for (int i = 0; i < np; i++) {
            final String s = spaths.getStringValue(1).getStrval();
            final String t = tpaths.getStringValue(1).getStrval();
            final Path dstPath = new Path(t);
            final Path srcPath = new Path(s);
            if (!fs.rename(srcPath, dstPath)) {
                throw new Exception("Error renaming " + s);
            }
        }
    }

    public REXP getStatus(final String jd, final boolean getErrors) throws Exception {
        final JobID jj = JobID.forName(jd);
        if (jj == null) {
            throw new IOException("Jobtracker could not find jobID: " + jd);
        }
        final RunningJob rj = jclient.getJob(jj);
        if (rj == null) {
            log.info("No such job: " + jd + " available, wrong job? or try the History Viewer (see the Web UI) ");
//            final REXP.Builder theValsMock = REXP.newBuilder();
//            theValsMock.setRclass(REXP.RClass.LIST);
//            theValsMock.addRexpValue(RObjects.makeStringVector(new String[]{"SUCCEEDED"}));
//            theValsMock.addRexpValue(RObjects.buildDoubleVector(new double[]{0.0}));
//            theValsMock.addRexpValue(RObjects.buildDoubleVector(new double[]{(double) 0.0, (double) 0.0}));
//            theValsMock.addRexpValue(RObjects.buildIntVector(new int[]{0, 0, 0, 0, 0, 0, 0}));
//            theValsMock.addRexpValue(RObjects.buildIntVector(new int[]{0, 0, 0, 0, 0, 0, 0}));
//            theValsMock.addRexpValue(FileUtils.buildListFromOldCounter(new Counters(), 0.0));
//            final REXP.Builder errcontainer = REXP.newBuilder();
//            errcontainer.setRclass(REXP.RClass.STRING);
//            theValsMock.addRexpValue(errcontainer);
//            theValsMock.addRexpValue(RObjects.makeStringVector("http://localhost:8080"));
//            theValsMock.addRexpValue(RObjects.makeStringVector(new String[]{"test-mock-job"}));
//            theValsMock.addRexpValue(RObjects.makeStringVector(new String[]{"mock-job-file"}));
//            return theValsMock.build();
            throw new IOException("No such job: " + jd + " available, wrong job? or try the History Viewer (see the Web UI) ");
        }
        final String jobfile = rj.getJobFile();
        final String jobname = rj.getJobName();
        final Counters cc = rj.getCounters();
        final long startsec = getStart(jclient, jj);
        final double dura = ((double) System.currentTimeMillis() - startsec) / 1000;

        final REXP ro = FileUtils.buildListFromOldCounter(cc, dura);
        final int jobs = rj.getJobState();
        String jobss = null;
        if (jobs == JobStatus.FAILED) {
            jobss = "FAILED";
        }
        else if (jobs == JobStatus.KILLED) {
            jobss = "KILLED";
        }
        else if (jobs == JobStatus.PREP) {
            jobss = "PREP";
        }
        else if (jobs == JobStatus.RUNNING) {
            jobss = "RUNNING";
        }
        else if (jobs == JobStatus.SUCCEEDED) {
            jobss = "SUCCEEDED";
        }
        final float mapprog = rj.mapProgress();
        final float reduprog = rj.reduceProgress();

        final TaskReport[] maptr = jclient.getMapTaskReports(jj);
        final TaskReport[] redtr = jclient.getReduceTaskReports(jj);

        final int totalmaps = maptr.length;
        final int totalreds = redtr.length;
        int mappending = 0, redpending = 0, maprunning = 0, redrunning = 0, redfailed = 0, redkilled = 0, mapkilled = 0, mapfailed = 0, mapcomp = 0, redcomp = 0;
        for (final TaskReport aMaptr : maptr) {
            final TIPStatus t = aMaptr.getCurrentStatus();
            switch (t) {
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
        for (final TaskReport aRedtr : redtr) {
            final TIPStatus t = aRedtr.getCurrentStatus();
            switch (t) {
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
        int reduceafails = 0, reduceakilled = 0, mapafails = 0, mapakilled = 0;
        int startfrom = 0;

        final REXP.Builder errcontainer = REXP.newBuilder();
        errcontainer.setRclass(REXP.RClass.STRING);
        while (true) {
            final TaskCompletionEvent[] events = rj.getTaskCompletionEvents(startfrom);
            for (final TaskCompletionEvent e : events) {
                switch (e.getTaskStatus()) {
                    case KILLED:
                        if (e.isMapTask()) {
                            mapakilled++;
                        }
                        else {
                            reduceakilled++;
                        }
                        break;
                    case TIPFAILED:
                    case FAILED:
                        if (e.isMapTask()) {
                            mapafails++;
                        }
                        else {
                            reduceafails++;
                        }
                        if (getErrors) {
                            final REXPProtos.STRING.Builder content = REXPProtos.STRING.newBuilder();
                            final String[] s = rj.getTaskDiagnostics(e.getTaskAttemptId());
                            if (s != null && s.length > 0) {
                                content.setStrval(s[0]);
                                errcontainer.addStringValue(content.build());
                            }
                        }
                        break;
                }
            }
            startfrom += events.length;
            if (events.length == 0) {
                break;
            }
        }

        final REXP.Builder theVals = REXP.newBuilder();
        theVals.setRclass(REXP.RClass.LIST);
        theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobss})); //1
        theVals.addRexpValue(RObjects.buildDoubleVector(new double[]{dura})); //2
        theVals.addRexpValue(RObjects.buildDoubleVector(new double[]{(double) mapprog, (double) reduprog})); //3
        theVals.addRexpValue(RObjects.buildIntVector(new int[]{totalmaps, mappending, maprunning, mapcomp, mapkilled, mapafails, mapakilled})); //4
        theVals.addRexpValue(RObjects.buildIntVector(new int[]{totalreds, redpending, redrunning, redcomp, redkilled, reduceafails, reduceakilled})); //5
        theVals.addRexpValue(ro); //6
        theVals.addRexpValue(errcontainer); //7
        theVals.addRexpValue(RObjects.makeStringVector(rj.getTrackingURL())); //8
        theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobname})); //9
        theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobfile})); //10
	theVals.addRexpValue(RObjects.makeStringVector(rj.getID().toString())); //11
        return (theVals.build());
    }

    public long getStart(final JobClient jc, final JobID jj) throws Exception {
        //this is not needed if i can get a reference to JobTracker (which rtruns the JobStatus for a given JobID)
        final JobStatus[] jbs = jc.getAllJobs();
        for (final JobStatus jb : jbs) {
            if (jb.getJobID().toString().equals(jj.toString())) {
                return (jb.getStartTime());
            }
        }
        return (0);
    }

    public void killJob(final String jd) throws Exception {
        final JobID jj = JobID.forName(jd);
        final RunningJob rj = jclient.getJob(jj);
        rj.killJob();
    }

    public REXP joinJob(final String jd, final boolean verbose) throws Exception {

        final JobID jj = JobID.forName(jd);
        final RunningJob rj = jclient.getJob(jj);

        // cfg.addResource(new Path(jobfile));
        final boolean issuc = jclient.monitorAndPrintJob(jobconf, rj);
        final Counters cc = rj.getCounters();
        final long startsec = getStart(jclient, jj);
        final REXP ro = FileUtils.buildListFromOldCounter(cc, ((double) System.currentTimeMillis() - startsec) / 1000);

        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        thevals.addRexpValue(ro);
        thevals.addRexpValue(RObjects.buildBooleanVector(new boolean[]{issuc}).build());
        return (thevals.build());
    }

    public byte[] getDetailedInfoForJob(final String jd) throws Exception {
        try {
            final JobID jj = JobID.forName(jd);

            if (jj == null) {
                throw new IOException("Jobtracker could not find jobID: " + jd);
            }
            final RunningJob rj = jclient.getJob(jj);
            if (rj == null) {
                throw new IOException("No such job: " + jd + " available, wrong job? or try the History Viewer (see the Web UI) ");
            }
            final String jobFile = rj.getJobFile();
            final String jobName = rj.getJobName();
            final Counters cc = rj.getCounters();
            final long startSec = getStart(jclient, jj);
            final REXP allCounters = FileUtils.buildListFromOldCounter(cc, 0);
            final int jobs = rj.getJobState();
            String jobStatus = null;
            if (jobs == JobStatus.FAILED) {
                jobStatus = "FAILED";
            }
            else if (jobs == JobStatus.KILLED) {
                jobStatus = "KILLED";
            }
            else if (jobs == JobStatus.PREP) {
                jobStatus = "PREP";
            }
            else if (jobs == JobStatus.RUNNING) {
                jobStatus = "RUNNING";
            }
            else if (jobs == JobStatus.SUCCEEDED) {
                jobStatus = "SUCCEEDED";
            }

            final float mapProg = rj.mapProgress();
            final float reduceProg = rj.reduceProgress();

            final REXP.Builder theVals = REXP.newBuilder();

            theVals.setRclass(REXP.RClass.LIST);
            theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobStatus}));
            theVals.addRexpValue(RObjects.buildDoubleVector(new double[]{startSec}));
            theVals.addRexpValue(RObjects.buildDoubleVector(new double[]{(double) mapProg, (double) reduceProg}));
            theVals.addRexpValue(allCounters);
            theVals.addRexpValue(RObjects.makeStringVector(rj.getTrackingURL()));
            theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobName}));
            theVals.addRexpValue(RObjects.makeStringVector(new String[]{jobFile}));

            final TaskReport[] mapTaskReports = jclient.getMapTaskReports(jj);
            final REXP.Builder theValsA = REXP.newBuilder();
            theValsA.setRclass(REXP.RClass.LIST);
            for (final TaskReport t : mapTaskReports) {
                theValsA.addRexpValue(TaskReportToRexp(t));
            }
            theVals.addRexpValue(theValsA.build());


            final TaskReport[] redtr = jclient.getReduceTaskReports(jj);
            final REXP.Builder theValsB = REXP.newBuilder();
            theValsB.setRclass(REXP.RClass.LIST);
            for (final TaskReport t : redtr) {
                theValsB.addRexpValue(TaskReportToRexp(t));
            }

            theVals.addRexpValue(theValsB.build());

            return theVals.build().toByteArray();
        } catch (Throwable e) {
            e.printStackTrace();
            log.fatal("error:", e);
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            throw new Exception(e);
        }
    }

    private String convertTIP(final TIPStatus tipStatus) {
        return tipStatus.toString();
    }

    private REXP TaskReportToRexp(final TaskReport taskReport) {
        final REXP.Builder theVals = REXP.newBuilder();
        theVals.setRclass(REXP.RClass.LIST);
        theVals.addRexpValue(FileUtils.buildListFromOldCounter(taskReport.getCounters(), 0));
        theVals.addRexpValue(RObjects.makeStringVector(convertTIP(taskReport.getCurrentStatus())));
        theVals.addRexpValue(RObjects.buildDoubleVector(new double[]{taskReport.getProgress(), taskReport.getStartTime(), taskReport.getFinishTime()}));
        theVals.addRexpValue(RObjects.makeStringVector(taskReport.getTaskID().toString()));
        //work-around for cdh5 which throws a NPE on this call due to some enumerated type not existing deep in the toString() operation
        String successfulTaskAttemptId;
        try {
//            not compatible with cdh3
//            if(taskReport.getSuccessfulTaskAttempt().getTaskID().getTaskType() != null)
                successfulTaskAttemptId = taskReport.getSuccessfulTaskAttempt().toString();
//            else
//                successfulTaskAttemptId = "not available";
//
            log.debug("successfulTaskAttemptId = " + successfulTaskAttemptId);

        } catch (Throwable e) {
            log.debug("Exception Ignored",e);
            successfulTaskAttemptId = "not available";
        }
        theVals.addRexpValue(RObjects.makeStringVector(successfulTaskAttemptId));

        return theVals.build();
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
