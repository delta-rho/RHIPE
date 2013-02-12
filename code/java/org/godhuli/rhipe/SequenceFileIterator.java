package org.godhuli.rhipe;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SequenceFileIterator  {
    protected static final Log LOG = LogFactory.getLog(SequenceFileIterator.class
						       .getName());

    String[] files;
    int current;
    int chunk;
    RHBytesWritable k,v;
    SequenceFile.Reader sqr;
    boolean notcomplete;
    FileSystem fs;
    Configuration cfg;
     int mnum;
    int numreadtill=0;
    public SequenceFileIterator(){}
    public void init(String filenames, int chunksize, int maxn, PersonalServer s)
	 throws IOException
    {
	init(new String[] {filenames}, chunksize,maxn,s);
    }
    public void init(String[] filenames, int chunksize,int maxn, PersonalServer s)
	 throws IOException
    {
	files = filenames;
	chunk = chunksize;
	current = 0;
	fs= s.getFS();
	cfg=s.getConf();
	notcomplete = true;
	mnum = maxn;
	sqr = new SequenceFile.Reader(fs, new Path(files[current]), cfg);
	k = new RHBytesWritable();
	v = new RHBytesWritable();
    }
    public boolean hasMoreElements(){
	return notcomplete && (mnum <0 || numreadtill < mnum);
    }
    public byte[] nextElement() throws Exception{
	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
	boolean gotone = false;
	for(int i=0;i< chunk && (mnum<0 || numreadtill < mnum);i++){
	    gotone = sqr.next((Writable) k, (Writable) v);
	    if(gotone){
		numreadtill++;
		REXP.Builder a   = REXP.newBuilder();
		a.setRclass(REXP.RClass.LIST);
		a.addRexpValue( RObjects.buildRawVector( k.getBytes(), 0, k.getLength()) );
		a.addRexpValue( RObjects.buildRawVector( v.getBytes(), 0, v.getLength()) );
		a.build();
		thevals.addRexpValue(a);
	    }else {
		sqr.close();
		current++;
		if(current == files.length){
		    notcomplete=false;
		    break;
		}
		// LOG.info("switching to next file: "+files[current]);
		// LOG.info(fs);
		// LOG.info(cfg);
		sqr = new SequenceFile.Reader(fs,new Path( files[current]), cfg);
		// LOG.info(sqr);
	    }
	}
	return thevals.build().toByteArray();
    }
    public byte[] nextChunk() throws Exception{
	REXP.Builder thevals   = REXP.newBuilder();
    	thevals.setRclass(REXP.RClass.LIST);
	boolean gotone = false;
	int bread = 0;
	while(true){
	    gotone = sqr.next((Writable) k, (Writable) v);
	    if(gotone){
		numreadtill++;
		bread+= k.getLength()+v.getLength();
		REXP.Builder a   = REXP.newBuilder();
		a.setRclass(REXP.RClass.LIST);
		a.addRexpValue( RObjects.buildRawVector( k.getBytes(), 0, k.getLength()) );
		a.addRexpValue( RObjects.buildRawVector( v.getBytes(), 0, v.getLength()) );
		a.build();
		thevals.addRexpValue(a);
	    }else {
		sqr.close();
		current++;
		if(current == files.length){
		    notcomplete=false;
		    break;
		}
		// LOG.info("switching to next file: "+files[current]);
		sqr = new SequenceFile.Reader(fs,new Path( files[current]), cfg);
	    }
	    if(bread> chunk || numreadtill == mnum)
		break;
	}
	return thevals.build().toByteArray();
    }


}
