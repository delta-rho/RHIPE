package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparator;

public class RHInteger extends RHBytesWritable {
    private int dl;
    // private REXP rexp0 = null;

    private static REXP template;
    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.INTEGER);
	template = templatebuild.build();
    }
	
    public RHInteger(){
	super();
    }
    public RHInteger(int l){
	super();
	this.dl =l;
    }

  
    public void set(int l){
	this.dl = l;
    }

    public void finis(){
	REXP.Builder b = REXP.newBuilder(template); 
	b.addIntValue(dl);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void setAndFinis(int l){
	this.dl = l;
	REXP.Builder b = REXP.newBuilder(template); 
	b.addIntValue(l);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void readFields(DataInput in) throws IOException{
	super.readFields(in);
	try{
	    REXP rexp0 = getParsed();
	    // System.err.println(rexp0.getRclass());
	    // System.err.println(rexp0);
	    this.dl = rexp0.getIntValue(0);
	}catch(com.google.protobuf.InvalidProtocolBufferException e){
	    throw new IOException(e);
	}
    }

    // new additions
    // public int hashCode() {
    // 	int v=(int)this.dl ;//need to change for vectors ...
    // 	return(v);
    // }

    // WRONG
    public boolean equals(Object other) {
	if (!(other instanceof RHInteger))
	    return false;
	RHInteger that = (RHInteger)other;
	return this.dl == that.dl;
    }


    // public int compareTo(RHInt other) {
    // 	return (dl < other.dl ? -1 : (dl == other.dl ? 0 : 1));
    // }

    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(RHInteger.class);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	 	    // return comparator.compare(b1, s1, l1, b2, s2, l2);
	    int off1= decodeVIntSize(b1[s1]), off2 = decodeVIntSize(b2[s2]);
	    REXP tir=null,thr=null;
	    int thisValue,thatValue;
	    thisValue=thatValue=0;
	    try{
		tir = REXP.newBuilder().mergeFrom(b1, s1+off1, l1-off1).build();
	        thr = REXP.newBuilder().mergeFrom(b2, s2+off2, l2-off2).build();
	    }catch(com.google.protobuf.InvalidProtocolBufferException e){
		throw new RuntimeException("RHIPE Integer Comparator:"+e);
	    }
	    int til=tir.getIntValueCount(), thl=thr.getIntValueCount();
	    int minl = til < thl? til: thl;
	    for(int i=0; i< minl;i++){
		thisValue = tir.getIntValue(i); thatValue = thr.getIntValue(i);
		if (thisValue < thatValue) return(-1);
		if (thisValue > thatValue) return(1);
	    }
	    return(0);
	    // if( til < thl) return( -1 );
	    // if( til > thl) return( 1 );
	    
	    // for(int i=0;i< til;i++){
	    // 	thisValue = tir.getRealValue(i); thatValue = thr.getRealValue(i);
	    // 	if (thisValue < thatValue) return(-1);
	    // 	if (thisValue > thatValue) return(1);
	    // }
	    // return(0);
	}
    }
    static { // register this comparator
	WritableComparator.define(RHInteger.class, new Comparator());
    }
}
