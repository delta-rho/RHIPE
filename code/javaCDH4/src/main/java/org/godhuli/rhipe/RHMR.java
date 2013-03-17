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

import java.util.Collection;
import java.util.Vector;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.EOFException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Calendar;
import java.util.List;
import java.net.URI;

import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import java.net.URLClassLoader;
import java.net.URL;
import java.io.File;

import org.godhuli.rhipe.REXPProtos.REXP;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class RHMR implements Tool {
	protected URLClassLoader urlloader;
	protected Environment env_;
	protected String[] argv_;
	protected Configuration config_;
	protected Hashtable<String, String> rhoptions_;
	protected Job job_;
	protected boolean debug_;
	public static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
	final static Log LOG = LogFactory.getLog(RHMR.class);

	public static void main(String[] args) {
		int res;
		try {
			// (new RHMR()).doTest();
			// System.exit(0);
			RHMR r = new RHMR();
			r.setConfig(new Configuration());
			res = ToolRunner.run(r.getConfig(), r, args);
		} catch (Exception ex) {
			ex.printStackTrace();
			res = -2;
		}
		System.exit(res);
	}

	public static int fmain(String[] args) throws Exception {
		int res;
		// (new RHMR()).doTest();
		// System.exit(0);
		RHMR r = new RHMR();
		r.setConfig(new Configuration());
		res = ToolRunner.run(r.getConfig(), r, args);
		return (res);
	}

	public void doTest() {
		int i;
		for (i = 0; i < 1; i++) {
			System.out.println("I=" + i);
		}
		;
		System.out.println("Last I=" + i);
	}

	public Configuration getConfig() {
		return config_;
	}

	public void setConfig(Configuration c) {
		config_ = c;
	}

	public void setConf(Configuration c) {
	}

	protected void init() {
		try {
			debug_ = false;
			rhoptions_ = new Hashtable<String, String>();
			readParametersFromR(argv_[0]);
			env_ = new Environment();
			// config_ = new Configuration();
			setConf();

			job_ = new Job(config_);
			setJob();

		} catch (Exception io) {
			io.printStackTrace();
			throw new RuntimeException(io);
		}
	}

	public Configuration getConf() {
		return config_;
	}

	public int run(String[] args) throws Exception {
		this.argv_ = args;
		init();
		submitAndMonitorJob(argv_[0]);
		return 1;
	}

	public void setConf() throws IOException, URISyntaxException {
		Enumeration keys = rhoptions_.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = (String) rhoptions_.get(key);
			config_.set(key, value);
			// System.out.println(key+"==="+value);
		}
		REXPHelper.setFieldSep(config_.get("mapred.field.separator", " "));

		String[] shared = config_.get("rhipe_shared").split(",");
		if (shared != null) {
			for (String p : shared)
				if (p.length() > 1)
					DistributedCache.addCacheFile(new URI(p), config_);
		}
		String[] jarfiles = config_.get("rhipe_jarfiles").split(",");
		if (jarfiles != null) {
			for (String p : jarfiles) {
				// System.err.println("Adding "+ p +" to classpath");
				if (p.length() > 1)
					DistributedCache
							.addArchiveToClassPath(new Path(p), config_);
			}
		}
		String[] zips = config_.get("rhipe_zips").split(",");
		if (zips != null) {
			for (String p : zips) {
				// System.err.println("Adding zip "+ p +" to cache");
				if (p.length() > 1)
					DistributedCache.addCacheArchive(new URI(p), config_);
			}
		}

		DistributedCache.createSymlink(config_);
		// if (!rhoptions_.get("rhipe_classpaths").equals("")) {
		// 	String[] cps = rhoptions_.get("rhipe_jarfiles").split(",");
		// 	for(String s : cps)
		// 	    DistributedCache.addFileToClassPath(new Path(s), config_);
		// }
		
		if (!rhoptions_.get("rhipe_classpaths").equals("")) {
		    String[] cps = rhoptions_.get("rhipe_classpaths").split(",");
			URL[] us = new URL[cps.length];
			for (int i = 0; i < cps.length; i++) {
			    try {
				us[i] = (new File(cps[i])).toURI().toURL();
			    } catch (java.net.MalformedURLException e) {
				throw new IOException(e);
			    }
			}
			System.err.println("Adding "+us.length+" JAR(s)");
			config_.setClassLoader(new URLClassLoader(us, config_
								  .getClassLoader()));
			Thread.currentThread().setContextClassLoader(
								     new URLClassLoader(us, Thread.currentThread()
											.getContextClassLoader()));
		}

	}

	public void setJob() throws ClassNotFoundException, IOException,
			URISyntaxException {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		String jname = rhoptions_.get("rhipe_jobname");
		boolean uscomb = false;

		if (jname.equals(""))
			job_.setJobName(sdf.format(cal.getTime()));
		else
			job_.setJobName(jname);
		job_.setJarByClass(RHMR.class);
		// System.err.println(rhoptions_.get("rhipe_outputformat_class"));
		Class<?> clz = config_.getClassByName(rhoptions_
				.get("rhipe_outputformat_class"));
		Class<? extends OutputFormat> ofc = clz.asSubclass(OutputFormat.class);
		job_.setOutputFormatClass(ofc);

		Class<?> clz2 = config_.getClassByName(rhoptions_
				.get("rhipe_inputformat_class"));
		Class<? extends InputFormat> ifc = clz2.asSubclass(InputFormat.class);
		job_.setInputFormatClass(ifc);

		if (!rhoptions_.get("rhipe_input_folder").equals(""))
			FileInputFormat.setInputPaths(job_, rhoptions_
					.get("rhipe_input_folder"));

		// System.out.println("FOO");
		// System.out.println(rhoptions_.get("rhipe.use.hadoop.combiner"));

		if (rhoptions_.get("rhipe.use.hadoop.combiner").equals("TRUE"))
			uscomb = true;
		String[] output_folder = rhoptions_.get("rhipe_output_folder").split(
				",");
		for (int i = 0; i < output_folder.length; i++)
			System.out.println(output_folder[i]);
		// System.exit(0);
		if (!rhoptions_.get("rhipe_partitioner_class").equals("none")) {
		    RHMRHelper.PARTITION_START = Integer.parseInt(rhoptions_
								  .get("rhipe_partitioner_start")) - 1;
		    RHMRHelper.PARTITION_END = Integer.parseInt(rhoptions_
								.get("rhipe_partitioner_end")) - 1;
		    Class<?> clz3 = config_.getClassByName(rhoptions_
							   .get("rhipe_partitioner_class"));
		    Class<? extends org.apache.hadoop.mapreduce.Partitioner> pc = clz3
			.asSubclass(org.apache.hadoop.mapreduce.Partitioner.class);
		    job_.setPartitionerClass(pc);
		}
		
		if (!output_folder[0].equals("")) {
			Path ofp = new Path(output_folder[0]);
			FileSystem srcFs = FileSystem.get(job_.getConfiguration());
			srcFs.delete(ofp, true);
			if (rhoptions_.get("rhipe_outputformat_class").equals("org.apache.hadoop.mapreduce.lib.output.NullOutputFormat")) {
			    srcFs.mkdirs(ofp);
			}
			FileOutputFormat.setOutputPath(job_, ofp);
		} 
		job_.setMapOutputKeyClass(Class.forName(rhoptions_
							.get("rhipe_map_output_keyclass")));
		job_.setMapOutputValueClass(Class.forName(rhoptions_
							  .get("rhipe_map_output_valueclass")));
		
		job_.setOutputKeyClass(Class.forName(rhoptions_
						     .get("rhipe_outputformat_keyclass")));
		job_.setOutputValueClass(Class.forName(rhoptions_
						       .get("rhipe_outputformat_valueclass")));
		

		job_.setMapperClass(RHMRMapper.class);
		if (uscomb)
			job_.setCombinerClass(RHMRReducer.class);
		job_.setReducerClass(RHMRReducer.class);
		// System.out.println("Conf done");

	}

	public int runasync(String configfile) throws Exception {
		FileOutputStream out = new FileOutputStream(configfile);
		DataOutputStream fout = new DataOutputStream(out);
		String[] arl = new String[4];
		arl[0] = job_.getTrackingURL().toString();
		arl[1] = job_.getJobName().toString();
		// System.out.println(job_.getJobID()); // returns null ??
		arl[2] = arl[0].split("=")[1]; // job_.getJobID().toString();
		arl[3] = RHMR.now();
		REXP b = RObjects.makeStringVector(arl);
		RHBytesWritable rb = new RHBytesWritable(b.toByteArray());
		rb.writeAsInt(fout);
		return (0);
	}

	public static String now() {
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		return sdf.format(cal.getTime());
	}

	public int submitAndMonitorJob(String configfile) throws Exception {
		int k = 0;
		job_.submit();
		if (rhoptions_.get("rhipe_job_async").equals("TRUE")) {
			return (runasync(configfile));
		}
		LOG.info("Tracking URL ----> " + job_.getTrackingURL());
		boolean verb = rhoptions_.get("rhipe_job_verbose").equals("TRUE") ? true
				: false;
		long now = System.currentTimeMillis();
		boolean result = job_.waitForCompletion(verb);
		double tt = (System.currentTimeMillis() - now) / 1000.0;
		// We will overwrite the input configfile
		FileOutputStream out = new FileOutputStream(configfile);
		DataOutputStream fout = new DataOutputStream(out);
		try {
			if (!result) {
				k = -1;
			} else {
				k = 0;
				org.apache.hadoop.mapreduce.Counters counter = job_
						.getCounters();
				REXP r = RHMR.buildListFromCounters(counter, tt);
				RHBytesWritable rb = new RHBytesWritable(r.toByteArray());
				rb.writeAsInt(fout);
			}
		} catch (Exception e) {
			k = -1;
		} finally {
			fout.close();
			out.close();
		}
		return k;
	}

	public static REXP buildListFromCounters(
			org.apache.hadoop.mapreduce.Counters counters, double tt) {
//		String[] groupnames = counters.getGroupNames().toArray(new String[] {});
		List<String> list = new ArrayList<String>();
		for(String groupName:  counters.getGroupNames()){
			list.add(groupName);
		}
		String[] groupnames = new String[list.size()];
		groupnames = list.toArray(groupnames);
		
		String[] groupdispname = new String[groupnames.length + 1];
		Vector<REXP> cn = new Vector<REXP>();
		for (int i = 0; i < groupnames.length; i++) {
			org.apache.hadoop.mapreduce.CounterGroup cgroup = counters
					.getGroup(groupnames[i]);
			groupdispname[i] = cgroup.getDisplayName();
			REXP.Builder cvalues = REXP.newBuilder();
			Vector<String> cnames = new Vector<String>();
			cvalues.setRclass(REXP.RClass.REAL);
			for (org.apache.hadoop.mapreduce.Counter counter : cgroup) {
				cvalues.addRealValue((double) counter.getValue());
				cnames.add(counter.getDisplayName());
			}
			cvalues.addAttrName("names");
			cvalues.addAttrValue(RObjects.makeStringVector(cnames
					.toArray(new String[] {})));
			cn.add(cvalues.build());
		}
		groupdispname[groupnames.length] = "job_time";
		REXP.Builder cvalues = REXP.newBuilder();
		cvalues.setRclass(REXP.RClass.REAL);
		cvalues.addRealValue(tt);
		cn.add(cvalues.build());
		return (RObjects.makeList(groupdispname, cn));
	}

	public void readParametersFromR(String configfile) throws IOException {
		FileInputStream in = new FileInputStream(configfile);
		DataInputStream fin = new DataInputStream(in);
		byte[] d;
		String key, value;
		int n0 = fin.readInt(), n;
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
			} catch (EOFException e) {
				throw new IOException(e);
			}
		}
		fin.close();
		if (debug_) {
			Enumeration keys = rhoptions_.keys();
			while (keys.hasMoreElements()) {
				String key0 = (String) keys.nextElement();
				String value0 = (String) rhoptions_.get(key0);
				System.out.println(key0 + "=" + value0);
			}
		}
	}
}
