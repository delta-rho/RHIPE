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

public class RHNull extends RHBytesWritable{

    private static byte[] _nullbytes = null;
    private static REXP.Builder returnvalue;
    private static void checkBytes(){
	if(_nullbytes == null)    {
	    returnvalue   = REXP.newBuilder();
	    returnvalue.setRclass(REXP.RClass.NULLTYPE);
	    _nullbytes  = returnvalue.build().toByteArray();
	} 
    }
    public static byte[] getRawBytes(){
	checkBytes();
	return _nullbytes;
    }
    public static REXP.Builder getBuilt(){
	checkBytes();
	return returnvalue;
    }
    public RHNull(){
	super();

	checkBytes();
	set(_nullbytes);
    }
}
