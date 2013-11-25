package org.godhuli.rhipe;

import com.google.protobuf.ByteString;
import org.godhuli.rhipe.REXPProtos.REXP;

import java.util.List;

public class RObjects {
    public RObjects() {
    }

    public static REXP.Builder buildStringVector(final String[] s) {
        final REXP.Builder returnvalue = REXP.newBuilder();
        returnvalue.setRclass(REXP.RClass.STRING);
        for (int i = 0; i < s.length; i++) {
            final REXPProtos.STRING.Builder content = REXPProtos.STRING.newBuilder();
            content.setStrval(s[i]);
            returnvalue.addStringValue(content.build());
        }
        return (returnvalue);
    }

    public static REXP makeStringVector(final String[] s) {
        return (buildStringVector(s).build());
    }

    public static REXP makeStringVector(final String s) {
        final REXP.Builder returnvalue = REXP.newBuilder();
        returnvalue.setRclass(REXP.RClass.STRING);
        final REXPProtos.STRING.Builder content = REXPProtos.STRING.newBuilder();
        content.setStrval(s);
        return returnvalue.addStringValue(content.build()).build();
    }

    public static REXP.Builder buildBooleanVector(final boolean[] b) {
        final REXP.Builder cvalues = REXP.newBuilder();
        cvalues.setRclass(REXP.RClass.LOGICAL);
        for (int i = 0; i < b.length; i++) {
            if (b[i]) {
                cvalues.addBooleanValue(org.godhuli.rhipe.REXPProtos.REXP.RBOOLEAN.T);
            }
            else {
                cvalues.addBooleanValue(org.godhuli.rhipe.REXPProtos.REXP.RBOOLEAN.F);
            }
        }
        return (cvalues);
    }

    public static REXP.Builder buildDoubleVector(final double b) {
        return buildDoubleVector(new double[]{b});
    }

    public static REXP.Builder buildDoubleVector(final double[] b) {
        final REXP.Builder cvalues = REXP.newBuilder();
        cvalues.setRclass(REXP.RClass.REAL);
        for (int i = 0; i < b.length; i++) {
            cvalues.addRealValue(b[i]);
        }
        return (cvalues);
    }

    public static REXP.Builder buildRawVector(final byte[] b) {
        return buildRawVector(b, 0, b.length);
    }

    public static REXP.Builder buildRawVector(final byte[] b, final int a, final int c) {
        final REXP.Builder cvalues = REXP.newBuilder();
        cvalues.setRclass(REXP.RClass.RAW);
        cvalues.setRawValue(ByteString.copyFrom(b, a, c));
        return (cvalues);
    }

    public static REXP.Builder buildIntVector(final int[] b) {
        final REXP.Builder cvalues = REXP.newBuilder();
        cvalues.setRclass(REXP.RClass.INTEGER);
        for (int i = 0; i < b.length; i++) {
            cvalues.addIntValue(b[i]);
        }
        return (cvalues);
    }

    public static REXP.Builder addAttr(final REXP.Builder rxb, final String name, final REXP attrval) {
        rxb.addAttrName(name);
        rxb.addAttrValue(attrval);
        return (rxb);
    }

    public static REXP.Builder buildList(final String[] names, final List<REXP> rexp) {
        final REXP.Builder thevals = REXP.newBuilder();
        thevals.setRclass(REXP.RClass.LIST);
        for (int i = 0; i < rexp.size(); i++) {
            thevals.addRexpValue(rexp.get(i));
        }
        thevals.addAttrName("names");
        thevals.addAttrValue(makeStringVector(names));
        return (thevals);
    }

    public static REXP makeList(final String[] names, final List<REXP> rexp) {
        return (buildList(names, rexp).build());
    }

    public static REXP makeList(final List<String> names, final List<REXP> rexp) {
        return (makeList(names.toArray(new String[names.size()]), rexp));
    }
}
