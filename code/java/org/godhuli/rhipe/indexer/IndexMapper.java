
// package org.godhuli.rhipe.index;

// import java.io.IOException;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

// import org.apache.hadoop.io.BytesWritable;
// import java.io.UnsupportedEncodingException;
// import org.godhuli.rhipe.RHBytesWritable;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.hbase.HBaseConfiguration;
// import org.apache.hadoop.hbase.KeyValue;
// import org.apache.hadoop.hbase.client.HTable;
// import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
// import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
// import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.hbase.client.Put;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.conf.Configured;
// import org.apache.hadoop.util.Tool;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
// import java.util.UUID;

// public  class IndexMapper extends  Mapper< RHBytesWritable, 
// 				     BytesWritable, 
// 				     ImmutableBytesWritable, 
// 				     Put> {
//     ImmutableBytesWritable hKey = new ImmutableBytesWritable();
//     Put put;
//     byte[] family,offset, sequence; 
//     final static Log LOG = LogFactory.getLog(IndexMapper.class);
	
//     protected void setup(Context context) throws IOException,
// 	InterruptedException {
// 	Configuration c = context.getConfiguration();
// 	try{
// 	    family = "data".getBytes("UTF-8");
// 	    offset = ("o_"+c.get("rhipe_index_prefix")).getBytes("UTF-8");
// 	    sequence = ("s_"+c.get("rhipe_index_prefix")).getBytes("UTF-8");
// 	}catch(UnsupportedEncodingException e){
// 	    throw new IOException(e);
// 	}
//     }
	
//     protected void map(RHBytesWritable key, BytesWritable value, Context context)
// 	throws IOException, InterruptedException {
// 	byte[] ab = key.getActualBytes();
// 	hKey.set(ab);
	    

// 	byte[] b = value.getBytes();
// 	int l = value.getLength();
	    
// 	byte[] theoffset=new byte[ IndexService.SIZEOF_LONG ];
// 	System.arraycopy(b, 0, theoffset, 0, IndexService.SIZEOF_LONG);
	    
// 	byte[] thefile = new byte[ l - IndexService.SIZEOF_LONG];
// 	System.arraycopy(b, IndexService.SIZEOF_LONG, thefile,0, l - IndexService.SIZEOF_LONG);

// 	// LOG.info("Key = "+key+" Offset="+IndexService.toLong(b,0,IndexService.SIZEOF_LONG)+" Filename:"+new String( b, IndexService.SIZEOF_LONG,l - IndexService.SIZEOF_LONG,"UTF-8" ));
	    
// 	put = new Put(ab);
// 	put.add(family, offset, theoffset);
// 	put.add(family,sequence, thefile);
// 	context.write(hKey,put);
//     }
// }
