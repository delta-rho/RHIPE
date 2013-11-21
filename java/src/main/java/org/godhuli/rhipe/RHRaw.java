package org.godhuli.rhipe;

import org.godhuli.rhipe.REXPProtos.REXP;

public class RHRaw extends RHBytesWritable {

    private static REXP raw_template;

    {
        REXP.Builder templatebuild = REXP.newBuilder();
        templatebuild.setRclass(REXP.RClass.RAW);
        raw_template = templatebuild.build();
    }

    private byte[] _result = null;

    public RHRaw() {
        super();
        _result = null;
    }

    public void set(byte[] j) {
        _result = j;
        REXP.Builder b = REXP.newBuilder(raw_template);
        b.setRawValue(com.google.protobuf.ByteString.copyFrom(j));
        super.set(b.build().toByteArray());
    }

    public byte[] getBytes() {
        return _result;
    }
}
