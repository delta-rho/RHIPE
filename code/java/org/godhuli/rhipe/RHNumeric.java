package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparator;

public class RHNumeric extends RHBytesWritable {
    private long l;
    private double dl;
    private static REXP template;
    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.REAL);
	template = templatebuild.build();
    }
	
    public RHNumeric(){
	super();
    }
    public RHNumeric(long l){
	super();
	this.l = l;
	this.dl = (double)l;
    }

    public long getLong(){
	return(l);
    }
    public void set(long l){
	this.l = l;
	this.dl = (double)l;
    }

    public void finis(){
	REXP.Builder b = REXP.newBuilder(template); 
	b.addRealValue((double)l);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void setAndFinis(long l){
	this.l = l;
	this.dl = (double)l;
	REXP.Builder b = REXP.newBuilder(template); 
	b.addRealValue((double)l);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void readFields(DataInput in) throws IOException{
	// System.out.println("READREAD");
	super.readFields(in);
	// System.out.println(toString());
	try{
	    REXP rexp0 = getParsed();
	    // System.out.println(rexp0);
	    this.dl = rexp0.getRealValue(0);
	    this.l = (long)this.dl;
	}catch(com.google.protobuf.InvalidProtocolBufferException e){
	    throw new IOException(e);
	}
    }

    // new additions
    public int hashCode() {
	return (int)Double.doubleToLongBits(dl); //change!!!!
    }
 
    // public String toString(){
    // 	return "NUMERIC="+this.l;
    // }

    public boolean equals(Object other) {
	if (!(other instanceof RHNumeric))
	    return false;
	RHNumeric that = (RHNumeric)other;
	return this.dl == that.dl;
    }


    // public int compareTo(RHNumeric other) {
    // 	return (dl < other.dl ? -1 : (dl == other.dl ? 0 : 1));
    // }

    public static class Comparator extends WritableComparator {
	public Comparator() {
	    super(RHNumeric.class);
	}

	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	 	    // return comparator.compare(b1, s1, l1, b2, s2, l2);
	    int off1= decodeVIntSize(b1[s1]), off2 = decodeVIntSize(b2[s2]);
	    REXP tir=null,thr=null;
	    double thisValue,thatValue;
	    thisValue=thatValue=0;
	    try{
		tir = REXP.newBuilder().mergeFrom(b1, s1+off1, l1-off1).build();
	        thr = REXP.newBuilder().mergeFrom(b2, s2+off1, l2-off1).build();
	    }catch(com.google.protobuf.InvalidProtocolBufferException e){
		System.err.println(e);
	    }
	    int til=tir.getRealValueCount(), thl=thr.getRealValueCount();
	    int minl = til < thl? til: thl;
	    for(int i=0; i< minl;i++){
		thisValue = tir.getRealValue(i); thatValue = thr.getRealValue(i);
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
	WritableComparator.define(RHNumeric.class, new Comparator());
    }
}
