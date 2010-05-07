package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

public class RHNumeric extends RHBytesWritable {
    private long l;
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
    }

    public long getLong(){
	return(l);
    }
    public void set(long l){
	this.l = l;
    }

    public void finis(){
	REXP.Builder b = REXP.newBuilder(template); 
	b.addRealValue((double)l);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void setAndFinis(long l){
	this.l = l;
	REXP.Builder b = REXP.newBuilder(template); 
	b.addRealValue((double)l);
	REXP rexp0=b.build();
	super.set(rexp0.toByteArray());
    }

    public void readFields(DataInput in) throws IOException{
	super.readFields(in);
	try{
	    REXP rexp0 = getParsed();
	    this.l = (long)rexp0.getRealValue(0);
	}catch(com.google.protobuf.InvalidProtocolBufferException e){
	    throw new IOException(e);
	}
    }
}
