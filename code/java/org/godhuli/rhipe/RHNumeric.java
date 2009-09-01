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
	rexp=b.build();
    }

    public void setAndFinis(long l){
	this.l = l;
	REXP.Builder b = REXP.newBuilder(template); 
	b.addRealValue((double)l);
	rexp=b.build();
    }

    public void write(DataOutput out) throws IOException{
	super.set(rexp.toByteArray());
	super.write(out);
    }

    public void readFields(DataInput in) throws IOException{
	super.readFields(in);
	rexp = REXP.parseFrom(bytes);
	this.l = (long)rexp.getRealValue(0);
    }
}
