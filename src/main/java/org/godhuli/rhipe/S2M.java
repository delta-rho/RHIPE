package org.godhuli.rhipe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class S2M {

    public static class IDMapper extends Mapper<RHBytesWritable, RHBytesWritable, RHBytesWritable, RHBytesWritable> {

        public void map(final RHBytesWritable key, final RHBytesWritable value, final Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static boolean runme(final String[] ipath, final String opath, final boolean local) throws Exception {
        final Configuration conf = new Configuration();
        if (local) {
            conf.set("mapred.job.tracker", "local");
        }
        conf.set("mapred.job.reuse.jvm.num.tasks", "-1");
        final Job job = Job.getInstance(conf, "Sequence To Map");
//        final Job job = new Job(conf, "Sequence To Map");
        job.setJarByClass(S2B.class);
        job.setMapperClass(IDMapper.class);
        job.setOutputKeyClass(RHBytesWritable.class);
        job.setOutputValueClass(RHBytesWritable.class);
        // job.setNumReduceTasks(0);
        for (int i = 0; i < ipath.length; i++) {
            FileInputFormat.addInputPath(job, new Path(ipath[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(opath));
        job.setOutputFormatClass(RHMapFileOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.submit();
        final boolean result = job.waitForCompletion(true);
        return (result);
    }
}
