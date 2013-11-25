/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class RHMRReducer extends Reducer<WritableComparable, RHBytesWritable, WritableComparable, RHBytesWritable> {
    protected static final Log LOG = LogFactory.getLog(RHMRReducer.class.getName());
    boolean isAMap;
    RHMRHelper helper;
    boolean doPipe_;
    boolean justCollect;
    Class<?> _kc = null;
    Class<? extends RHBytesWritable> keyclass;

    WritableComparable wck = null;

    String getPipeCommand(final Configuration cfg) {
        String str = System.getenv("RHIPECOMMAND");
        if (str == null) {
            str = cfg.get("rhipe_command");
            if (str == null) {
                System.err.println("No rhipe_command");
            }
        }
        return (str);

    }

    boolean getDoPipe(final Configuration cfg) {
        final String argv = getPipeCommand(cfg);
        doPipe_ = getPipeCommand(cfg) != null && cfg.getInt("mapred.reduce.tasks", 0) != 0;
        return (!justCollect);
    }

    public void run(final Context context) throws IOException, InterruptedException {
        helper = new RHMRHelper("Reduce");
        justCollect = context.getConfiguration().get("rhipe_reduce_justcollect").equals("TRUE");

        if (!justCollect) {
            setup(context);
            while (context.nextKey()) {
                pipereduce(context.getCurrentKey(), context.getValues(), context);
            }
            LOG.info("CHECKING OUTER_THREADS NOW!");
            cleanup(context);
            helper.checkOuterrThreadsThrowable();

        }
        else {

            try {
                _kc = Class.forName(context.getConfiguration().get("rhipe_outputformat_keyclass"));
                keyclass = _kc.asSubclass(RHBytesWritable.class);
                wck = keyclass.newInstance();
            }
            catch (InstantiationException e) {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            while (context.nextKey()) {
                simplereduce(context.getCurrentKey(), context.getValues(), context);
            }
        }
    }

    public void setup(final Context ctx) {
        final Configuration cfg = ctx.getConfiguration();
        cfg.set("RHIPEWHAT", "1");
        helper.setup(cfg, getPipeCommand(cfg), getDoPipe(cfg));
        isAMap = cfg.getBoolean("mapred.task.is.map", true);
        helper.startOutputThreads(ctx);
        try {
            if (!justCollect) {
                helper.writeCMD(RHTypes.EVAL_SETUP_REDUCE);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void pipereduce(final WritableComparable key, final Iterable<RHBytesWritable> values, final Context ctx) throws IOException, InterruptedException {
        try {
            helper.writeCMD(RHTypes.EVAL_REDUCE_THEKEY);
            helper.write(key);
            helper.writeCMD(RHTypes.EVAL_REDUCE_PREKEY);
            for (final RHBytesWritable val : values) {
                helper.checkOuterrThreadsThrowable();
                helper.write(val);
            }
            helper.writeCMD(RHTypes.EVAL_REDUCE_POSTKEY);
        }
        catch (IOException io) {
            String extraInfo = "";
            try {
                extraInfo = helper.getSimExitInfo();
            }
            catch (IllegalThreadStateException e) {
                extraInfo = "subprocess still running\n";
            }
            helper.mapRedFinished(ctx);
            throw new IOException(extraInfo + "::" + io.getMessage());
        }
    }


    public void simplereduce(final WritableComparable key, final Iterable<RHBytesWritable> values, final Context ctx) throws IOException, InterruptedException {
        // wck = keyclass.cast(key);
        // System.out.println("Class of key = "+wck.getClass().getName());
        // RHBytesWritable awb = RHBytesWritable.getClass().cast(key);
        // System.out.println("Class of key = "+awb.getClass().getName());

        try {
            for (final RHBytesWritable val : values) {
                ctx.write(key, val);
            }

        }
        catch (IOException io) {
            final String extraInfo = "";
            // try {
            //     extraInfo = helper.getSimExitInfo();
            // } catch (IllegalThreadStateException e) {
            //     extraInfo = "subprocess still running\n";
            // };
            helper.mapRedFinished(ctx);
            throw new IOException(extraInfo + "::" + io.getMessage());
        }
    }


    public void cleanup(final Context ctx) {
        try {
            if (!justCollect) {
                if (!isAMap) {
                    helper.writeCMD(RHTypes.EVAL_CLEANUP_REDUCE);
                }
                helper.writeCMD(RHTypes.EVAL_FLUSH);
            }

            helper.mapRedFinished(ctx);
            if (!isAMap) {
                helper.copyFiles(System.getProperty("java.io.tmpdir"));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


}
