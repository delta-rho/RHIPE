package org.godhuli.rhipe;

import java.util.Set;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import org.apache.hadoop.io.WritableComparator;
import org.godhuli.rhipe.RHBytesWritable;
import org.godhuli.rhipe.RObjects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RHRaw extends RHBytesWritable{

    private static REXP raw_template;
    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.RAW);
	raw_template = templatebuild.build();
    } 
    private byte[] _result = null;
    public RHRaw(){
	super();
	_result = null;
    }

    public void set(byte[] j){
	_result  = j;
	REXP.Builder b = REXP.newBuilder(raw_template);
	b.setRawValue( com.google.protobuf.ByteString.copyFrom( j ) );
	super.set(b.build().toByteArray());
    }

    public byte[] getBytes(){
	return _result;
    }
}
