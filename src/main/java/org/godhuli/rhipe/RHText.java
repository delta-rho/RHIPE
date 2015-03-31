package org.godhuli.rhipe;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.DataInput;
import java.io.IOException;

public class RHText extends RHBytesWritable {


    private final Text textcontainer;
    private static final REXP template;

    static {
        final REXP.Builder templatebuild = REXP.newBuilder();
        templatebuild.setRclass(REXP.RClass.STRING);
        template = templatebuild.build();
    }

    public RHText() {
        super();
        textcontainer = new Text();
    }

    public RHText(final String x) {
        this();
        set(x);
    }

    public RHText(final Text x) {
        this(x.toString());
    }

    public void set(final String x) {
        textcontainer.set(x);
    }

    public void setAndFinis(final String x) throws IOException{
	set(x);
        finis();
    }

    public void set(final Text x) {
        set(x.toString());
    }

    public Text getText() {
        return textcontainer;
    }

    protected void finis() throws IOException{
        final REXPProtos.STRING.Builder srb = REXPProtos.STRING.newBuilder();
	if(RHMRHelper.ENCODE_NULLS_IN_TEXT == false){
	    srb.setStrval(textcontainer.toString());
	}else{
	    try{
		// see http://stackoverflow.com/questions/18760041/encoding-string-to-modified-utf-8-for-the-datainput?rq=1
		String ss = textcontainer.toString();
		byte[] tb = ss.getBytes("UTF-8");
		int nulCount = 0;
		for (int i = 0; i < tb.length; ++i) {
		    if (tb[i] == 0) {
			++nulCount;
		    }
		}
		if(nulCount == 0){
		    srb.setStrval(ss);
		}else{
		    byte[] convertedBytes = new byte[tb.length + nulCount];
		    for (int i = 0, j = 0; i < tb.length; ++i, ++j) {
			convertedBytes[j] = tb[i];
			if (tb[i] == 0) {
			    convertedBytes[j] = (byte)0xC0;
			    ++j;
			    convertedBytes[j] = (byte)0x80;
			}
		    }
		    srb.setStrvalBytes(com.google.protobuf.ByteString.copyFrom(convertedBytes));
		}
	    }catch( java.io.UnsupportedEncodingException e){
		throw new IOException(e);
	    }
	}
        final REXP.Builder b = REXP.newBuilder(template);
        b.addStringValue(srb.build());
        final REXP rexp0 = b.build();
        super.set(rexp0.toByteArray());
    }


    public void readFields(final DataInput in) throws IOException {

        super.readFields(in);
        try {
            final REXP rexp0 = getParsed();
            textcontainer.set(rexp0.getStringValue(0).getStrval());
        }
        catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(RHText.class);
        }

        public int compare(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2, final int l2) {
            // return comparator.compare(b1, s1, l1, b2, s2, l2);
            final int off1 = decodeVIntSize(b1[s1]);
            final int off2 = decodeVIntSize(b2[s2]);
            REXP tir = null, thr = null;
            byte[] thisValue, thatValue;
            thisValue = thatValue = null;
            try {
                // System.err.println("The barray started at:"+ s1+", the REXP started "+off1+" bytes later and continued for "+l1+"from the begining of byte array: "+RHBytesWritable.bytesPretty(b1,s1,l1));
                // System.err.println("The barray started at:"+ s2+", the REXP started "+off2+" bytes later and continued for "+l2+"from the begining of byte array: "+RHBytesWritable.bytesPretty(b2,s2,l2));
                // System.err.println("-----------------------------");
                tir = REXP.newBuilder().mergeFrom(b1, s1 + off1, l1 - off1).build();
                thr = REXP.newBuilder().mergeFrom(b2, s2 + off2, l2 - off2).build();
            }
            catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new RuntimeException("RHIPE String Comparator:" + e);
            }
            final int til = tir.getStringValueCount();
            final int thl = thr.getStringValueCount();
            final int minl = til < thl ? til : thl;
            for (int i = 0; i < minl; i++) {
                thisValue = tir.getStringValue(i).getStrval().getBytes();
                thatValue = thr.getStringValue(i).getStrval().getBytes();
                final int a = compareBytes(thisValue, 0, thisValue.length, thatValue, 0, thatValue.length);
                if (a != 0) {
                    return (a);
                }
            }
            return (0);
        }
    }

    static { // register this comparator
        WritableComparator.define(RHText.class, new Comparator());
    }

}
