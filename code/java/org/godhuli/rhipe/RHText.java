package org.godhuli.rhipe;
import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;

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
}
