
// package org.godhuli.rhipe.index;

// import java.io.IOException;
// import java.util.Hashtable;
// import java.util.Arrays;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

// import org.apache.hadoop.io.BytesWritable;
// import java.io.UnsupportedEncodingException;
// import org.godhuli.rhipe.RHBytesWritable;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.mapreduce.Mapper;
// import org.apache.hadoop.mapreduce.Job;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
// import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.hbase.client.Put;
// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.conf.Configured;
// import org.apache.hadoop.util.Tool;
// import org.apache.hadoop.util.ToolRunner;
// import org.apache.hadoop.fs.FileUtil;
// import org.apache.hadoop.fs.FsShell;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.FSDataOutputStream;
// import org.apache.hadoop.io.SequenceFile;
// import org.apache.hadoop.io.Writable;
// import java.util.UUID;

// import org.apache.hadoop.hbase.client.HBaseAdmin;
// import org.apache.hadoop.hbase.util.Bytes;
// import org.apache.hadoop.hbase.HColumnDescriptor;
// import org.apache.hadoop.hbase.HTableDescriptor;
// import org.apache.hadoop.hbase.HBaseConfiguration;
// import org.apache.hadoop.hbase.KeyValue;
// import org.apache.hadoop.hbase.client.Result;
// import org.apache.hadoop.hbase.client.Get;

// import org.apache.hadoop.hbase.client.HTable;
// import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
// import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
// import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

// public class IndexService extends Configured implements Tool {
//     public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
//     static Hashtable<String,HTable> tables = new Hashtable<String, HTable>();
//     static Hashtable<String, SequenceFile.Reader> seqhash=new Hashtable<String, SequenceFile.Reader>();;

//     static byte[] family = Bytes.toBytes("data");
//     public static long toLong(byte[] bytes) {
// 	return IndexService.toLong(bytes,0,bytes.length);
//     }
//     public static long toLong(byte[] bytes, int offset, final int length) {
// 	// from http://goo.gl/2XQp2
// 	long l = 0;
// 	for(int i = offset; i < offset + length; i++) {
// 	    l <<= 8;
// 	    l ^= bytes[i] & 0xFF;
// 	}
// 	return l;
//     }
//     public static byte[] toBytes(long val) {
// 	byte [] b = new byte[8];
// 	for (int i = 7; i > 0; i--) {
// 	    b[i] = (byte) val;
// 		val >>>= 8;
// 	}
// 	b[0] = (byte) val;
// 	return b;
//     }
	
//     public static void main(String[] args) throws Exception {
//         int exitCode = ToolRunner.run(new IndexService(), args);
//         System.exit(exitCode);
//     }

//     public static HTable createTable(HBaseAdmin hba, String tname)
// 	throws IOException{
// 	try{
// 	    if(!hba.tableExists(tname)){
// 		HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes(tname));
// 		htd.addFamily(new HColumnDescriptor(Bytes.toBytes("data")));
// 		hba.createTable(htd);
// 	    }
// 	    return(new HTable(tname));
// 	}catch(Exception e){
// 	    throw new IOException(e);
// 	}
//     }
//     public static RHBytesWritable getForQuery(byte[] row,String tablename,String offset, String file)
//     throws Exception{
// 	Configuration  c = new Configuration();
// 	c = HBaseConfiguration.addHbaseResources(c);
// 	return IndexService.getForQuery(row, tablename, Bytes.toBytes(offset), Bytes.toBytes(file), c);
//     }

//     public static RHBytesWritable getForQuery(byte[] row,String tablename,byte[] offset, byte[] file,Configuration cfg)
//     throws Exception
//     {
// 	HTable ht = null;
// 	ht =  tables.get(tablename);
// 	if(ht == null){
// 	    ht = new HTable(tablename);
// 	    tables.put(tablename, ht);
// 	}
// 	Get g= new Get(row);
// 	g.addColumn(family, offset);
// 	g.addColumn(family, file);
// 	Result r = ht.get(g);
// 	System.err.println("Checking for ! ");
// 	if(r.isEmpty()) return(null);
// 	KeyValue k=r.getColumnLatest(family,offset);
// 	long off = IndexService.toLong( k.getValue());
// 	k=r.getColumnLatest(family,file);
// 	String filename = new String( k.getValue(), "UTF-8");
// 	System.err.println("Found! "+filename+":"+off);
// 	SequenceFile.Reader sqr = seqhash.get(filename);
// 	if(sqr == null){
// 	    System.err.println("Creating new SQ Reader for "+filename);
// 	    Configuration c = new Configuration();
// 	    sqr = new SequenceFile.Reader(FileSystem.get(c), new Path(filename), c);    
// 	    seqhash.put(filename,sqr);
// 	}
// 	sqr.sync(off);
// 	RHBytesWritable key = new RHBytesWritable();
// 	RHBytesWritable v = new RHBytesWritable();
// 	byte[] found = null;
// 	while( sqr.next(key,v)) {
// 	    System.err.println("Position is now:"+sqr.getPosition());
// 	    System.err.println("Size Of Value ="+v.getLength());
// 	    found = v.getActualBytes();
// 	    if( Arrays.equals(row,found )) {
// 	    	break;
// 	    }
// 	    System.out.println(v);
// 	}
// 	if(found!=null){
// 	    System.out.println(v);
// 	    return(v);
// 	}
// 	System.out.println("Not found");
// 	return(null);
//     }

//     public int run(String[] args) throws Exception {

// 	    Configuration conf = new Configuration();
// 	    //args: jobname, tablename, input-source, output-path,tmp
// 	    // Load hbase-site.xml
// 	    conf = HBaseConfiguration.addHbaseResources(conf);
// 	    conf.set("rhipe_index_prefix",UUID.randomUUID().toString());
// 	    Job job = new Job(conf, args[0]);
	    
// 	    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
// 	    job.setMapOutputValueClass(Put.class);
// 	    job.setMapperClass(IndexMapper.class);
	    
	    
// 	    job.setJarByClass(IndexMapper.class);
// 	    job.setInputFormatClass(IndexerInputFormat.class);
// 	    FileInputFormat.addInputPath(job, new Path(args[2]));
// 	    FileOutputFormat.setOutputPath(job, new Path(args[3]));
	    
// 	    HBaseAdmin hba = new HBaseAdmin(conf);
// 	    HTable hTable = IndexService.createTable(hba,args[1]); 
	    
// 	    // Auto configure partitioner and reducer
// 	    job.setWorkingDirectory(new Path(args[4]));
// 	    HFileOutputFormat.configureIncrementalLoad(job, hTable);

// 	    int rcode=0;
// 	    if(job.waitForCompletion(true)) rcode=1; else rcode=0;
// 	    FsShell shell = new FsShell(conf);
// 	    shell.run( new String[]{"-chmod","-R","775",args[3]});
// 	    if(rcode==1){
// 		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
// 		loader.doBulkLoad(new Path(args[3]), hTable);
// 		Path p = new Path(args[2],"rhipe_index_label");
// 		FSDataOutputStream out = FileSystem.get(conf).create(p,true);
// 		out.write((conf.get("rhipe_index_prefix")+"\n").getBytes("UTF-8"));
// 		out.close();
// 	    }
//     	    return(rcode);
//     }
// }
