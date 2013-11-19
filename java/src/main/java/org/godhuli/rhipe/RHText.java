package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparator;

public class RHText extends RHBytesWritable{

    

    private Text textcontainer;
    private static REXP template;

    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.STRING);
	template = templatebuild.build();
    } 

    public RHText(){
	super();
	textcontainer = new Text();
    }

    public RHText(String x){
	this();
	set(x);
    }
    public RHText(Text x){
	this(x.toString());
    }

    public void set(String x){
	textcontainer.set(x);
    }

    public void setAndFinis(String x){
	textcontainer.set(x);
	finis();
    }

    public void set(Text x){
	set(x.toString());
    }

    public Text getText(){
	return textcontainer;
    }
    protected void finis(){
	REXPProtos.STRING.Builder srb= REXPProtos.STRING.newBuilder();
	srb.setStrval(textcontainer.toString());
	
	REXP.Builder b = REXP.newBuilder(template);
	b.addStringValue(srb.build());
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }


    public void readFields(DataInput in) throws IOException{
	
	super.readFields(in);
	try{
	    REXP rexp0= getParsed();
	    textcontainer.set(rexp0.getStringValue(0).getStrval());
	}catch(com.google.protobuf.InvalidProtocolBufferException e){
	    throw new IOException(e);
	}
    }

    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(RHText.class);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	 	    // return comparator.compare(b1, s1, l1, b2, s2, l2);
	    int off1= decodeVIntSize(b1[s1]), off2 = decodeVIntSize(b2[s2]);
	    REXP tir=null,thr=null;
	    byte[] thisValue,thatValue;
	    thisValue=thatValue=null;
	    try{
		// System.err.println("The barray started at:"+ s1+", the REXP started "+off1+" bytes later and continued for "+l1+"from the begining of byte array: "+RHBytesWritable.bytesPretty(b1,s1,l1));
		// System.err.println("The barray started at:"+ s2+", the REXP started "+off2+" bytes later and continued for "+l2+"from the begining of byte array: "+RHBytesWritable.bytesPretty(b2,s2,l2));
		// System.err.println("-----------------------------");
		tir = REXP.newBuilder().mergeFrom(b1, s1+off1, l1-off1).build();
	        thr = REXP.newBuilder().mergeFrom(b2, s2+off2, l2-off2).build();
	    }catch(com.google.protobuf.InvalidProtocolBufferException e){
		throw new RuntimeException("RHIPE String Comparator:"+e);
	    }
	    int til=tir.getStringValueCount(), thl=thr.getStringValueCount();
	    int minl = til < thl? til: thl;
	    for(int i=0; i< minl;i++){
		thisValue = tir.getStringValue(i).getStrval().getBytes();
		thatValue = thr.getStringValue(i).getStrval().getBytes();
		int a = compareBytes(thisValue,0,thisValue.length,thatValue,0,thatValue.length);
		if(a !=0) return(a);
	    }
	    return(0);
	}
    }
    static { // register this comparator
	WritableComparator.define(RHText.class, new Comparator());
    }

}
