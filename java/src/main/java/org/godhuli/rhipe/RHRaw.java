package org.godhuli.rhipe;

import org.godhuli.rhipe.REXPProtos.REXP;

public class RHRaw extends RHBytesWritable {

    private static final REXP raw_template;

    static {
        final REXP.Builder templatebuild = REXP.newBuilder();
        templatebuild.setRclass(REXP.RClass.RAW);
        raw_template = templatebuild.build();
    }

    private byte[] _result = null;

    public RHRaw() {
        super();
        _result = null;
    }

    public void set(final byte[] j) {
        _result = j;
        final REXP.Builder b = REXP.newBuilder(raw_template);
        b.setRawValue(com.google.protobuf.ByteString.copyFrom(j));
        super.set(b.build().toByteArray());
    }

    public byte[] getBytes() {
        return _result;
    }
}
