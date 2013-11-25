package org.godhuli.rhipe;

import org.apache.hadoop.io.WritableComparator;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.io.DataInput;
import java.io.IOException;

public class RHInteger extends RHBytesWritable {
    private int dl;
    // private REXP rexp0 = null;

    private static final REXP template;

    static {
        final REXP.Builder templatebuild = REXP.newBuilder();
        templatebuild.setRclass(REXP.RClass.INTEGER);
        template = templatebuild.build();
    }

    public RHInteger() {
        super();
    }

    public RHInteger(final int l) {
        super();
        this.dl = l;
    }


    public void set(final int l) {
        this.dl = l;
    }

    public void finis() {
        final REXP.Builder b = REXP.newBuilder(template);
        b.addIntValue(dl);
        final REXP rexp0 = b.build();
        super.set(rexp0.toByteArray());
    }

    public void setAndFinis(final int l) {
        this.dl = l;
        final REXP.Builder b = REXP.newBuilder(template);
        b.addIntValue(l);
        final REXP rexp0 = b.build();
        super.set(rexp0.toByteArray());
    }

    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        try {
            final REXP rexp0 = getParsed();
            // System.err.println(rexp0.getRclass());
            // System.err.println(rexp0);
            this.dl = rexp0.getIntValue(0);
        }
        catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }

    // new additions
    // public int hashCode() {
    // 	int v=(int)this.dl ;//need to change for vectors ...
    // 	return(v);
    // }

    // WRONG
    public boolean equals(final Object other) {
        if (!(other instanceof RHInteger)) {
            return false;
        }
        final RHInteger that = (RHInteger) other;
        return this.dl == that.dl;
    }


    // public int compareTo(RHInt other) {
    // 	return (dl < other.dl ? -1 : (dl == other.dl ? 0 : 1));
    // }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(RHInteger.class);
        }

        public int compare(final byte[] b1, final int s1, final int l1, final byte[] b2, final int s2, final int l2) {
            // return comparator.compare(b1, s1, l1, b2, s2, l2);
            final int off1 = decodeVIntSize(b1[s1]);
            final int off2 = decodeVIntSize(b2[s2]);
            REXP tir = null, thr = null;
            int thisValue, thatValue;
            thisValue = thatValue = 0;
            try {
                tir = REXP.newBuilder().mergeFrom(b1, s1 + off1, l1 - off1).build();
                thr = REXP.newBuilder().mergeFrom(b2, s2 + off2, l2 - off2).build();
            }
            catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new RuntimeException("RHIPE Integer Comparator:" + e);
            }
            final int til = tir.getIntValueCount();
            final int thl = thr.getIntValueCount();
            final int minl = til < thl ? til : thl;
            for (int i = 0; i < minl; i++) {
                thisValue = tir.getIntValue(i);
                thatValue = thr.getIntValue(i);
                if (thisValue < thatValue) {
                    return (-1);
                }
                if (thisValue > thatValue) {
                    return (1);
                }
            }
            return (0);
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
