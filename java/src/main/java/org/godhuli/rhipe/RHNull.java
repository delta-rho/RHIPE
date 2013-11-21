package org.godhuli.rhipe;

import org.godhuli.rhipe.REXPProtos.REXP;

public class RHNull extends RHBytesWritable {

    private static byte[] _nullbytes = null;
    private static REXP.Builder returnvalue;

    private static void checkBytes() {
        if (_nullbytes == null) {
            returnvalue = REXP.newBuilder();
            returnvalue.setRclass(REXP.RClass.NULLTYPE);
            _nullbytes = returnvalue.build().toByteArray();
        }
    }

    public static byte[] getRawBytes() {
        checkBytes();
        return _nullbytes;
    }

    public static REXP.Builder getBuilt() {
        checkBytes();
        return returnvalue;
    }

    public RHNull() {
        super();

        checkBytes();
        set(_nullbytes);
    }
}
