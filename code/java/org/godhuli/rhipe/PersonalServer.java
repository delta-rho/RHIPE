package org.godhuli.rhipe;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.godhuli.rhipe.REXPProtos.REXP;
import java.io.*;
import java.util.*;
import java.net.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;

public class PersonalServer {
    byte[] bbuf;
    DataOutputStream _err,_toR;
    DataInputStream _fromR;
    REXP yesalive;
    FileUtils fu;
    public static String getPID() throws IOException,InterruptedException {
	Vector<String> commands=new Vector<String>();
	commands.add("/bin/bash");
	commands.add("-c");
	commands.add("echo $PPID");
	ProcessBuilder pb=new ProcessBuilder(commands);
	Process pr=pb.start();
	pr.waitFor();
	if (pr.exitValue()==0) {
	    BufferedReader outReader=new BufferedReader(new InputStreamReader(pr.getInputStream()));
	    return outReader.readLine().trim();
	} else {
	    throw new IOException("Problem getting PPID");
	}
    }
    public void docrudehack(String temp) throws IOException{
	FileWriter outFile = new FileWriter(temp);
	String x = "DONE";
	outFile.write(x,0,x.length());
	outFile.flush();outFile.close();
    }
	
    public PersonalServer(String ipaddress,String tempfile,String tempfile2) throws InterruptedException,
								   FileNotFoundException,UnknownHostException, SecurityException,IOException{
	bbuf = new byte[100];
	REXP.Builder thevals   = REXP.newBuilder();
	thevals.setRclass(REXP.RClass.LOGICAL);
	thevals.addBooleanValue( REXP.RBOOLEAN.T);
	yesalive = thevals.build();
	fu = new FileUtils(new Configuration());
	ServerSocket fromRsock,errsock,toRsock;
	fromRsock = new ServerSocket(0,0,InetAddress.getByName(ipaddress));
	toRsock  = new ServerSocket(0);
	errsock = new ServerSocket(0);
	FileWriter outFile = new FileWriter(tempfile);
	String x = "fromR toR err PID\n";
	outFile.write(x,0,x.length());
	x = fromRsock.getLocalPort()+" "+toRsock.getLocalPort()+" "+errsock.getLocalPort()+" "+ getPID()+"\n";
	outFile.write(x,0,x.length());
	outFile.flush();outFile.close();
	docrudehack(tempfile2);
	Socket a = fromRsock.accept();
	_fromR = new DataInputStream(new BufferedInputStream(a.getInputStream(),1024));
	 a = toRsock.accept();
	_toR = new DataOutputStream(new BufferedOutputStream(a.getOutputStream(),1024));
	 a = errsock.accept();
	 _err = new DataOutputStream(new BufferedOutputStream(a.getOutputStream(),1024));
    }

    public void send_error_message(Exception e){
    }
    public void send_error_message(String e){
    }

    public void rhmropts(REXP r) throws Exception{
	REXP b = fu.mapredopts();
	send_result(b);
    }
    public void rhls(REXP r) throws Exception{
	String[] result0 = fu.ls(r.getRexpValue(1) // This is a string vector
				 ,r.getRexpValue(2).getIntValue(0));
	REXP b = RObjects.makeStringVector(result0);
	send_result(b);

    }
    public void rhdel(REXP r) throws Exception{
	for(int i = 0;i <r.getRexpValue(1).getStringValueCount();i++){
	    String s = r.getRexpValue(1).getStringValue(i).getStrval();
	    fu.delete(s,true);
	}
	send_result("OK");
    }

    public void rhget(REXP r) throws Exception{
    }
    public void rhput(REXP r) throws Exception{
    }
    public void rhgetkeys(REXP r) throws Exception{
    }
    public void sequenceAsBinary(REXP r) throws Exception{ //works
	Configuration cfg = new Configuration();
	int n = r.getRexpValue(1).getStringValueCount();
	String[] infile = new String[n];
	for(int i=0;i< n;i++) {
	    infile[i] = r.getRexpValue(1).getStringValue(i).getStrval();
	}
	int maxnum = r.getRexpValue(2).getIntValue(0);
	int bufsz = r.getRexpValue(3).getIntValue(0);
	// as this rexp is written into
	DataOutputStream cdo = new DataOutputStream(new java.io.BufferedOutputStream(_toR,1024*1024));
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
		    k.writeAsInt(cdo);
		    v.writeAsInt(cdo);
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
	cdo.writeInt(0);
	cdo.flush();
    }

    public void rhstatus(REXP r) throws Exception{
	REXP jid = r.getRexpValue(1);
	REXP result = fu.joinjob(jid);
	send_result(result);
    }

    public void rhjoin(REXP r) throws Exception{
	REXP result = fu.joinjob(r.getRexpValue(1));
	send_result(result);
    }

    public void rhkill(REXP r) throws Exception{
	REXP jid = r.getRexpValue(1);
	fu.killjob(jid);
	send_result("OK");
    }

    
    public void send_alive() throws Exception{
	try{
	    
	    _toR.writeByte(1);
	    _toR.flush();
	}catch(IOException e){
	    System.err.println("RHIPE: Could not tell R it is alive");
	    System.exit(1);
	}
    }
    
    public void sendMessage(REXP r){
	sendMessage(r, false);
    }
    public void sendMessage(REXP r,boolean bb) {
	try{
	    byte[] b = r.toByteArray();
	    DataOutputStream dos = _toR;
	    if(bb) dos = _err;
	    dos.writeInt(b.length);
	    dos.write(b,0,b.length);
	    dos.flush();
	}catch(IOException e){
	    System.err.println("RHIPE: Could not send data back to R master, sending to standard error");
	    System.err.println(r);
	    System.exit(1);
	}
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

    public void startme(){
	while(true){
	    try{
	 	int size= _fromR.readInt();
		if(size> bbuf.length){
		    bbuf = new byte[size];
		}
		if(size == 0)
		    send_alive();
		else {
		_fromR.readFully(bbuf,0,size);
		REXP r = REXP.newBuilder().mergeFrom(bbuf,0,size).build();
		if(r.getRclass() == 	REXP.RClass.NULLTYPE)
		    send_alive();
		// the first element of list is function, the rest are arguments
		String tag = r.getRexpValue(0).getStringValue(0).getStrval();
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
		else if(tag.equals("rhkill")) rhkill(r);

		else send_error_message("Could not find method with name: "+tag+"\n");
		}
	}catch (SecurityException e) {
	    send_error_message(e);
	}catch (IllegalArgumentException e) {
	    send_error_message(e);
	}catch (IllegalAccessException e) {
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
	PersonalServer r = new PersonalServer(args[0],args[1],args[2]);
	while(true){
	    try{
		r.startme();
	    }catch(Exception e){
		System.err.println(Thread.currentThread().getStackTrace());
	    }
	}
    }

}