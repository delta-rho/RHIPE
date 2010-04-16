package  org.godhuli.rhipe;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class S2B {

  public static class IDMapper 
       extends Mapper<RHBytesWritable, RHBytesWritable, RHBytesWritable
	       , RHBytesWritable>{
      private int counter;
      private boolean head;
      public void setup(Context context) { 
	  counter = context.getConfiguration().getInt("rhipe_maxnum",-1);
	  head = (counter <0) ? false: true;
      }
      public void map(RHBytesWritable key, RHBytesWritable value, Context context
		      ) throws IOException, InterruptedException {
	  if(head){
	      if(counter > 0 ) {
		  context.write(key, value);
		  counter--;
	      }
	  }else  context.write(key, value);
      }
  }

    public static boolean runme(String[] ipath,String opath, boolean local,int maxnum) throws Exception {
	Configuration conf = new Configuration();
	if(local) conf.set("mapred.job.tracker","local");
	conf.set("mapred.job.reuse.jvm.num.tasks","-1");
	conf.setInt("rhipe_maxnum", maxnum);

	Job job = new Job(conf, "Sequence To Binary");
	job.setJarByClass(S2B.class);
	job.setMapperClass(IDMapper.class);
	job.setOutputKeyClass(RHBytesWritable.class);
	job.setOutputValueClass(RHBytesWritable.class);
	job.setNumReduceTasks(0);
	for(int i=0;i< ipath.length;i++)
	    FileInputFormat.addInputPath(job, new Path(ipath[i]));
	FileOutputFormat.setOutputPath(job, new Path(opath));
	job.setOutputFormatClass(RXBinaryOutputFormat.class);
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.submit();
	boolean result = job.waitForCompletion(true) ? true : false;
	return(result);
    }
}
