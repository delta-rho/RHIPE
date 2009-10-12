
// package org.godhuli.rhipe;
// import org.slf4j.Logger;  
// import org.slf4j.LoggerFactory; 
// import org.apache.mina.core.session.IoSession;
// import org.apache.hadoop.conf.Configuration;
// import org.godhuli.rhipe.REXPProtos.REXP;
// import org.godhuli.rhipe.REXPProtos.REXP.RClass;
// import java.util.*;
// import java.io.IOException;
// import java.io.DataInputStream;
// import java.io.FileInputStream;

// public class CommandExecution {
//     private int cID;
//     private byte[] bytes;
//     private int length;
//     private byte type;
//     private static final int kHADOOPOPTS=1;
//     private static final int kDELETABLEFOLDER=2;
//     private static final int kCOPYFROMLOCALFILE=3;
//     private static final int kCOPYMAIN=4;
//     private static final int kLS=5;
//     private static final int kDEL=6;
//     private static final int kBinToSeq=7;
//     private static final int kSeqToBin=8;

//     private static CommandExecution emptyResp;
//     private static final String WRITER_KEY= "WRITER";
//     private static final String READER_KEY= "READER";

//     public CommandExecution(){
//     }
//     public CommandExecution(int commandID, byte[] serializeBytes) {
//         this.cID = commandID;
//         this.bytes = serializeBytes;
// 	this.length=this.bytes.length;
// 	this.type=0x01;
//     }
//     public String toString(){
// 	return("cId="+cID+" length="+length+" type="+type);
//     }
//     public CommandExecution anException(String s) {
// 	CommandExecution c=new CommandExecution(-2,s.getBytes());
// 	c.setType((byte)0x00); //Error
// 	return(c);
//     }
//     public CommandExecution anEiyOkay(byte[] b) {
// 	CommandExecution c=new CommandExecution(0,b);
// 	return(c);
//     }
//     public CommandExecution anEiyOkay() {
// 	if(emptyResp==null){
// 	    emptyResp = new CommandExecution(0, new byte[]{});
// 	    emptyResp.setType((byte)0x02);
// 	}
// 	return(emptyResp);
//     }

//     void _kHADOOPOPTS(IoSession sess,FileUtils fileutil) throws Exception{
// 	System.out.println("HELLO");
// 	Configuration cfg = fileutil.getConf();
// 	Iterator<Map.Entry<String,String>> iter = cfg.iterator();
// 	REXP.Builder thevals   = REXP.newBuilder();
// 	REXP.Builder thenames  = REXP.newBuilder();
// 	thevals.setRclass(REXP.RClass.LIST);
// 	thenames.setRclass(REXP.RClass.STRING);
// 	while(iter.hasNext()){

// 	    Map.Entry<String,String> c = iter.next();
// 	    String key = c.getKey();
// 	    String value = c.getValue();
// 	    REXPProtos.STRING.Builder skey,svalue;
// 	    REXP.Builder aval   = REXP.newBuilder();
// 	    aval.setRclass(REXP.RClass.STRING);
// 	    svalue=REXPProtos.STRING.newBuilder();
// 	    skey  =REXPProtos.STRING.newBuilder();
// 	    skey.setStrval(key);
// 	    svalue.setStrval(value);
// 	    thenames.addStringValue(skey.build());
// 	    aval.addStringValue(svalue.build());
// 	    thevals.addRexpValue(aval.build());
// 	}
// 	REXP names1=thenames.build();
// 	thevals.addAttrName("names");
// 	thevals.addAttrValue(names1);
// 	REXP retval = thevals.build();
// 	//phew!
// 	byte[] b = retval.toByteArray();
// 	sess.write(anEiyOkay(b));
//     }

//     private void _kLS(IoSession sess,FileUtils fileutil) throws Exception{
// 	    try{
// 		REXP rexp0 = REXP.parseFrom(bytes);
// 		String s = rexp0.getStringValue(0).getStrval();
// 		String bb = fileutil.ls(s);
// 		REXP.Builder thevals   = REXP.newBuilder();
// 		thevals.setRclass(REXP.RClass.STRING);
// 		REXPProtos.STRING.Builder skey=REXPProtos.STRING.newBuilder();
// 		skey.setStrval(bb);
// 		thevals.addStringValue(skey.build());
// 		REXP fin = thevals.build();
// 		sess.write(anEiyOkay(fin.toByteArray()));
// 	    }catch(Exception fe){
// 		sess.write(anException(fe.toString()));
// 	    }
// 	}
//     private void _kCOPYMAIN(IoSession sess,FileUtils fileutil) throws Exception{
// 	    try{
// 		REXP rexp0 = REXP.parseFrom(bytes);
// 		String s1 = rexp0.getStringValue(0).getStrval();
// 		String s2 = rexp0.getStringValue(1).getStrval();
// 		fileutil.copyMain(s1,s2);
// 		sess.write(anEiyOkay());
// 	    }catch(Exception fe){
// 		sess.write(anException(fe.toString()));
// 	    }
// 	}
//     private void _kDEL(IoSession sess,FileUtils fileutil) throws Exception{
// 	    try{
// 		REXP rexp0 = REXP.parseFrom(bytes);
// 		String s = rexp0.getStringValue(0).getStrval();
// 		fileutil.delete(s,true);
// 		sess.write(anEiyOkay());
// 	    }catch(Exception fe){
// 		sess.write(anException(fe.toString()));
// 	    }
// 	}

//     private void _kDELETABLEFOLDER(IoSession sess,FileUtils fileutil) throws Exception{
// 	    try{
// 		REXP rexp0 = REXP.parseFrom(bytes);
// 		String s = rexp0.getStringValue(0).getStrval();
// 		fileutil.makeFolderToDelete(s);
// 		sess.write(anEiyOkay());
// 	    }catch(Exception fe){
// 		sess.write(anException(fe.toString()));
// 	    }
// 	}

//     public void _kCOPYFROMLOCALFILE(IoSession sess,FileUtils fileutil) throws Exception{
// 	try{
// 	    // list of array of files, destination, boolean
// 	    REXP rexp0 = REXP.parseFrom(bytes);
// // 	    System.out.println(rexp0);
// 	    String[] s = new String[rexp0.getRexpValue(0).getStringValueCount()];
// 	    for(int i=0;i<s.length;i++) s[i] = rexp0.getRexpValue(0).
// 					    getStringValue(i).getStrval();
// 	    String s2 = rexp0.getRexpValue(1).getStringValue(0).getStrval();
// 	    REXP.RBOOLEAN b = rexp0.getRexpValue(2).getBooleanValue(0);
// 	    boolean bb;
// 	    if(b==REXP.RBOOLEAN.F)
// 		bb=false;
// 	    else if(b==REXP.RBOOLEAN.T)
// 		bb=true;
// 	    else
// 		bb=false;
	    
// 	    fileutil.copyFromLocalFile(s,s2,bb);
// 	    sess.write(anEiyOkay());
// 	}catch(Exception fe){
// // 	    fe.printStackTrace();
// 	   sess.write(anException(fe.toString()));
// 	}
//     }


//     public void _kBin2Sequence(IoSession sess, FileUtils fileutil){
// 	try{
// 	    REXP rexp0 = REXP.parseFrom(bytes);
// 	    String tf= rexp0.getStringValue(0).getStrval();
// 	    String ofold= rexp0.getStringValue(1).getStrval();
// 	    int groupsize = Integer.parseInt(rexp0.getStringValue(2).getStrval());
// 	    int howmany = Integer.parseInt(rexp0.getStringValue(3).getStrval());
// 	    int N = Integer.parseInt(rexp0.getStringValue(4).getStrval());
	    
// 	    Configuration cfg = fileutil.getConf();
// 	    DataInputStream in = new 
// 	    DataInputStream(new FileInputStream(tf));
// 	    int count=0;
// 	    for(int i=0;i < howmany-1;i++){
// 		String f = ofold+"/"+i;
// 		RHWriter w = new RHWriter(f,cfg);
// 		w.doWriteFile(in,groupsize);
// 		count=count+groupsize;
// 		w.close();
// 	    }
// 	    if(count < N){
// 		count=N-count;
// 		String f = ofold+"/"+(howmany-1);
		
// 		RHWriter w = new RHWriter(f,cfg);
// 		w.doWriteFile(in,count);
// 		w.close();
// 	    }
// 	    sess.write(anEiyOkay());
// 	}catch(Exception e){
// 	     sess.write(anException(e.toString()));
// 	}
//     }

//     public void _kSeq2Bin(IoSession sess, FileUtils fileutil){
// 	try{
// 	    REXP rexp0 = REXP.parseFrom(bytes);
// 	    int n = rexp0.getStringValueCount();
// 	    String[] infile = new String[n-2];
// 	    String ofile = rexp0.getStringValue(n-2).getStrval();
// 	    int local = Integer.parseInt(rexp0.getStringValue(n-1).getStrval());
// 	    for(int i=0;i< n-2;i++) infile[i] = rexp0.getStringValue(i).getStrval();
// 	    S2B s = new S2B();
// 	    if(s.runme( infile, ofile,local==1 ? true:false)){
// 		sess.write(anEiyOkay());
// 	    }else{
// 		sess.write(anException("Could Not Convert Sequence To Binary"));
// 	    }
// 	}catch(Exception e){
// 	    sess.write(anException(e.toString()));
// 	}
//     }


//     void perform(IoSession session,FileUtils fileutil) throws Exception{
// 	switch(cID){
// 	case kHADOOPOPTS:
// 	    _kHADOOPOPTS(session,fileutil);
// 	    break;
// 	case kDELETABLEFOLDER:
// 	    _kDELETABLEFOLDER(session,fileutil);
// 	    break;
// 	case kCOPYFROMLOCALFILE:
// 	    _kCOPYFROMLOCALFILE(session,fileutil);
// 	    break;
// 	case kCOPYMAIN:
// 	    _kCOPYMAIN(session,fileutil);
// 	    break;
// 	case kLS:
// 	    _kLS(session,fileutil);
// 	    break;
// 	case kDEL:
// 	    _kDEL(session,fileutil);
// 	    break;
// 	case kBinToSeq:
// 	    _kBin2Sequence(session,fileutil);
// 	    break;
// 	case kSeqToBin:
// 	    _kSeq2Bin(session,fileutil);
// 	    break;
// 	}
//     }





//     public int getcID() {
//         return cID;
//     }
//     public int getLength() {
//         return this.length;
//     }
//     public byte[] getBytes() {
//         return bytes;
//     }
//     public void setBytes(byte[] b){
// 	this.length=b.length;
// 	this.bytes = b;
//     }
//     public void setLength(int l){
// 	this.length=l;
//     }
//     public void setType(byte b){
// 	this.type=b;
//     }
//     public byte getType(){
// 	return type;
//     }

// }

