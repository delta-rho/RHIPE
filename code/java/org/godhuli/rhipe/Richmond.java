package org.godhuli.rhipe;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.godhuli.rhipe.REXPProtos.REXP;
import java.io.*;
import java.util.*;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;

public class Richmond {
    DataOutputStream _toR;
    DataInputStream _fromR;
    DataOutputStream _error;
    PrintStream out;
    byte[] bbuf;
    Hashtable<String,Method> memdic;
    public Richmond(PrintStream ps,String fromR,String toR,String error) throws FileNotFoundException,IOException{
	out = ps;
	_toR = new DataOutputStream(new FileOutputStream(toR));
	_toR.write(new byte[]{0x9});
	_error = new DataOutputStream(new FileOutputStream(error));
	_fromR = new DataInputStream(new FileInputStream(fromR));
	bbuf = new byte[100];
	memdic = new Hashtable<String, Method>();
	insertIntomemdic();
    }
    private void insertIntomemdic(){
	try{
	    Class[] param = new Class[]{Class.forName("org.godhuli.rhipe.REXPProtos$REXP")};
	    memdic.put("rhmropts",this.getClass().getMethod("rhmropts",param));
	    memdic.put("rhls",this.getClass().getMethod("rhls",param));
	    memdic.put("rhget",this.getClass().getMethod("rhget",param));
	    memdic.put("rhput",this.getClass().getMethod("rhput",param));
	    memdic.put("rhdel",this.getClass().getMethod("rhdel",param));
	    memdic.put("rhgetkeys",this.getClass().getMethod("rhgetkeys",param));
	    memdic.put("binaryAsSequence",this.getClass().getMethod("binaryAsSequence",param));
	    memdic.put("sequenceAsBinary",this.getClass().getMethod("sequenceAsBinary",param));

	}catch (java.lang.ClassNotFoundException e) {
	    send_error_message(e);
	}catch (SecurityException e) {
	    send_error_message(e);
	}catch (NoSuchMethodException e) {
	    send_error_message(e);
	}
	send_result("OK\n");
    }

    public void sendMessage(REXP r){
	sendMessage(r, false);
    }
    public void sendMessage(REXP r,boolean bb) {
	try{
	    byte[] b = r.toByteArray();
	    // _toR.writeInt(b.length);
	    // _toR.write(b);
	    // _toR.flush();
	    DataOutputStream dos = _toR;
	    if(bb) dos = _error;
	    WritableUtils.writeVInt(dos,b.length);
	    dos.write(b,0,b.length);
	    dos.flush();
	}catch(IOException e){
	    System.err.println("RHIPE: Could not send data back to R master, sending to standard error");
	    System.err.println(r);
	    System.exit(1);
	}
    }
    public void send_error_message(Exception e){
	ByteArrayOutputStream bs = new ByteArrayOutputStream();
	e.printStackTrace(new PrintStream(bs));
	String s = bs.toString();
	send_error_message(s);
    }
    public void send_error_message(String s){
	REXP clattr = RObjects.makeStringVector("worker_error");
	REXP r = RObjects.addAttr(RObjects.buildStringVector(new String[]{s}), "class",clattr).build();
	// System.err.println(s);
	sendMessage(r,true);
    }
    public void send_result(String s){
	REXP r = RObjects.makeStringVector(s);
	send_result(r);
    }
    public void send_result(REXP r){
	// we create a list of class "worker_result"
	// it is a list of element given by s
	// all results are class worker_result and are a list
	REXP.Builder thevals   = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LIST);
	thevals.addRexpValue(r);
	RObjects.addAttr(thevals,"class",RObjects.makeStringVector("worker_result"));
	sendMessage(thevals.build());
    }

    public void rhmropts(REXP r) throws Exception{ //works
	FileUtils fu= new FileUtils(new Configuration());
	REXP b = fu.mapredopts();
	send_result(b);
    }

    public void rhls(REXP r) throws IOException{ //works
	FileUtils fu= new FileUtils(new Configuration());
	String[] result0 = fu.ls(r.getRexpValue(1) // This is a string vector
				 ,r.getRexpValue(2).getIntValue(0));
	REXP b = RObjects.makeStringVector(result0);
	send_result(b);
    }
    public void rhput(REXP r) throws IOException{ //works
	FileUtils fu= new FileUtils(new Configuration());
	String[] locals = new String[r.getRexpValue(1).getStringValueCount()];
	for(int i=0;i<locals.length;i++) 
	    locals[i] = r.getRexpValue(1).getStringValue(i).getStrval();
	String dest2 = r.getRexpValue(2).getStringValue(0).getStrval();
	REXP.RBOOLEAN overwrite_ = r.getRexpValue(3).getBooleanValue(0);
	boolean overwrite;
	if(overwrite_==REXP.RBOOLEAN.F)
	    overwrite=false;
	else if(overwrite_==REXP.RBOOLEAN.T)
	    overwrite=true;
	else
	    overwrite=false;
	fu.copyFromLocalFile(locals,dest2,overwrite);
	send_result("OK");
    }

    public void rhget(REXP r) throws IOException{ //works
	FileUtils fu= new FileUtils(new Configuration());
	String src = r.getRexpValue(1).getStringValue(0).getStrval();
	String dest = r.getRexpValue(2).getStringValue(0).getStrval();
	System.err.println("Copying "+src+" to "+dest);
	fu.copyMain(src,dest);
	send_result("OK");
    }

    public void rhdel(REXP r) throws IOException{ //works
	FileUtils fu= new FileUtils(new Configuration());
	for(int i = 0;i <r.getRexpValue(1).getStringValueCount();i++){
	    String s = r.getRexpValue(1).getStringValue(i).getStrval();
	    fu.delete(s,true);
	}
	send_result("OK");
    }

    public void rhgetkeys(REXP r) throws Exception{
	FileUtils fu= new FileUtils(new Configuration());
	fu.getKeys(r.getRexpValue(1), _toR, true);
    }

    public void binaryAsSequence(REXP r) throws Exception{ //works
	Configuration cfg = new Configuration();
	String ofolder= r.getRexpValue(1).getStringValue(0).getStrval();
	int groupsize = r.getRexpValue(2).getIntValue(0);
	int howmany = r.getRexpValue(3).getIntValue(0);
	int N = r.getRexpValue(4).getIntValue(0);
	DataInputStream  in = _fromR;
	int count=0;
	// System.out.println("Got"+r);
	// System.out.println("Waiting for input");
	for(int i=0;i < howmany-1;i++){
	    String f = ofolder+"/"+i;
	    RHWriter w = new RHWriter(f,cfg);
	    w.doWriteFile(in,groupsize);
	    count=count+groupsize;
	    w.close();
	}

	if(count < N){
	    count=N-count;
	    String f = ofolder+"/"+(howmany-1);
	    RHWriter w = new RHWriter(f,cfg);
	    w.doWriteFile(in,count);
	    w.close();
	}
	send_result("OK");
    }
    
    public void sequenceAsBinary(REXP r) throws Exception{ //works
	Configuration cfg = new Configuration();
	int n = r.getRexpValue(1).getStringValueCount();
	String[] infile = new String[n];
	for(int i=0;i< n;i++) {
	    infile[i] = r.getRexpValue(1).getStringValue(i).getStrval();
	}
	int maxnum = r.getRexpValue(2).getIntValue(0);
	// as this rexp is written into
	DataOutputStream cdo = _toR;
	int counter=0;
	boolean endd=false;
	RHBytesWritable k=new RHBytesWritable();
	RHBytesWritable v=new RHBytesWritable();
	for(int i=0; i <infile.length;i++){
	    SequenceFile.Reader sqr = new SequenceFile.Reader(FileSystem.get(cfg) ,new Path(infile[i]), cfg);
	    while(true){
		boolean gotone = sqr.next((Writable)k,(Writable)v);
		if(gotone){
		    counter++;
		    k.write(cdo);
		    v.write(cdo);
		    cdo.flush();
		}else break;
		if(maxnum >0 && counter >= maxnum) {
		    endd=true;
		    break;
		}
	    }
	    sqr.close();
	    if(endd) break;
	}
	WritableUtils.writeVInt(cdo,0);
	cdo.flush();
    }
	
    public void rhstatus(REXP r) throws Exception{
	FileUtils fu= new FileUtils(new Configuration());
	REXP jid = r.getRexpValue(1);
	REXP result = fu.joinjob(jid);
	send_result(result);
    }

    public void rhjoin(REXP r) throws Exception{
	FileUtils fu= new FileUtils(new Configuration());
	REXP result = fu.joinjob(r.getRexpValue(1));
	send_result(result);
    }

    public void startme(){
	while(true){
	    try{
	 	int size= _fromR.readInt();
		if(size> bbuf.length){
		    bbuf = new byte[size];
		}
		_fromR.readFully(bbuf,0,size);
		REXP r = REXP.newBuilder().mergeFrom(bbuf,0,size).build();
		// THIS is not high performance
		// am going to use a hash table lookup on strings
		// the first element of list is function, the rest are arguments
		String tag = r.getRexpValue(0).getStringValue(0).getStrval();
		// Method m = memdic.get(tag);
		// if(m== null) send_error_message("Could not find method with name:"+tag+"\n");
		// else m.invoke(this, r);
		if(tag.equals("rhmropts")) rhmropts(r);
		else if(tag.equals("rhls")) rhls(r);
		else if(tag.equals("rhget")) rhget(r);
		else if(tag.equals("rhput")) rhput(r);
		else if(tag.equals("rhdel")) rhdel(r);
		else if(tag.equals("rhgetkeys")) rhgetkeys(r);
		else if(tag.equals("binaryAsSequence")) binaryAsSequence(r);
		else if(tag.equals("sequenceAsBinary")) sequenceAsBinary(r);
		else if(tag.equals("rhstatus")) rhstatus(r);
		else if(tag.equals("rhjoin")) rhjoin(r);
		else send_error_message("Could not find method with name: "+tag+"\n");
	}catch (SecurityException e) {
	    send_error_message(e);
	}catch (IllegalArgumentException e) {
	    send_error_message(e);
	}catch (IllegalAccessException e) {
	    send_error_message(e);
	}catch (InvocationTargetException e) {
	    send_error_message(e);
	}catch(RuntimeException e){
	    send_error_message(e);
	}catch(IOException e){
	    send_error_message(e);
	}catch(Exception e){
	    send_error_message(e);
	}
	}
    }


    public static void main(String[] args) throws Exception{
	FileOutputStream outFile = new FileOutputStream("/tmp/fox");
	PrintStream out = new PrintStream(outFile);
	Richmond r = new Richmond(out,args[0],args[1],args[2]);
	while(true){
	    try{
		r.startme();
	    }catch(Exception e){
		out.println(Thread.currentThread().getStackTrace());
	    }
	}
    }

}